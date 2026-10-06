"""Separate OS processes for publishers and each independent fanout destination."""
from __future__ import annotations
import os
import struct
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

HEADER=struct.Struct('!QQHBQQ') # campaign, stage, publisher, phase, sequence, monotonic ns

class Seen:
    def __init__(self): self.bits={}
    def add(self,publisher,seq):
        if seq<0 or seq>100_000_000: raise ValueError('sequence outside bounded bitmap')
        b=self.bits.setdefault(publisher,bytearray())
        i,bit=divmod(seq,8)
        if i>=len(b): b.extend(b'\0'*(i+1-len(b)))
        if b[i] & (1<<bit): return False
        b[i] |= 1<<bit
        return True

def counters():
    return dict(attempted=0,submitted=0,acked=0,rejected=0,nacks=0,received=0,duplicates=0,unexpected=0,
                latency_count=0,latency_sum_ns=0,latency_max_ns=0)

def worker(config, phase, until, events, publisher_stop):
    # SDK imports only in the child; progress/simulation need no broker connection.
    import psutil
    from .loadgen import _service, _materialize_system_trust_store, GeneratorConfig
    from solace.messaging.resources.topic import Topic
    from solace.messaging.resources.topic_subscription import TopicSubscription
    from solace.messaging.resources.queue import Queue
    from solace.messaging.publisher.persistent_message_publisher import MessagePublishReceiptListener
    from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
    parent=os.getppid(); proc=psutil.Process(); proc.cpu_percent(None)
    lock=threading.Lock(); counts={1:counters(),3:counters()}; seen={1:Seen(),3:Seen()}
    max_cpu=0.; service=component=None; done=set(); clean=True
    net0=psutil.net_io_counters(); last_emit=0.
    def emit(kind='sample',**extra):
        nonlocal max_cpu,last_emit
        cpu=proc.cpu_percent(None)
        if phase.value==3:max_cpu=max(max_cpu,cpu)
        net=psutil.net_io_counters()
        with lock:
            event=dict(event=kind,id=config['id'],role=config['role'],t=time.monotonic(),
                       warmup=dict(counts[1]),measure=dict(counts[3]),max_cpu=max_cpu,
                       cpu=cpu,network_tx_bytes=net.bytes_sent-net0.bytes_sent,
                       network_rx_bytes=net.bytes_recv-net0.bytes_recv,
                       measurement_done=3 in done,**extra)
        events.put(event);last_emit=time.monotonic()
    class Receipts(MessagePublishReceiptListener):
        def on_publish_receipt(self,receipt):
            p=receipt.user_context
            with lock: counts[p]['acked' if receipt.is_persisted else 'nacks']+=1
    class Failures(PublishFailureListener):
        def on_failed_publish(self,event):
            with lock: counts[3 if phase.value>=3 else 1]['nacks']+=1
    try:
        with TemporaryDirectory(prefix='bcm-grid-ca-') as ca:
            _materialize_system_trust_store(Path(ca))
            g=GeneratorConfig(config['client_name'],'grid',config['mode'],config['smf_uri'],config['vpn'],
                              config['username'],config['password'],config['topic'],config.get('queue'),
                              config['payload_bytes'],config['rate'],0,1)
            service=_service(g,ca);service.connect()
            if config['role']=='receiver':
                if config['mode']=='direct':
                    component=service.create_direct_message_receiver_builder().with_subscriptions([TopicSubscription.of(config['topic'])]).build()
                else:
                    component=service.create_persistent_message_receiver_builder().with_message_client_acknowledgement().build(Queue.durable_exclusive_queue(config['queue']))
            elif config['mode']=='direct':
                component=service.create_direct_message_publisher_builder().on_back_pressure_reject(1000).build()
                component.set_publish_failure_listener(Failures())
            else:
                component=service.create_persistent_message_publisher_builder().on_back_pressure_reject(1000).build()

            component.start()
            if config['role']=='publisher' and config['mode']=='persistent':
                component.set_message_publish_receipt_listener(Receipts())
            emit('ready')
            current=0; sequence=0; start=0.; pad=os.urandom(config['payload_bytes']-HEADER.size)
            while phase.value!=5 and os.getppid()==parent:
                p=phase.value
                if config['role']=='receiver':
                    msg=component.receive_message(100)
                    if msg is not None:
                        raw=msg.get_payload_as_bytes()
                        valid=False
                        try:
                            camp,stage,pub,mp,seq,sent=HEADER.unpack_from(raw)
                            valid=(camp==config['campaign_tag'] and
                                   (stage==config['stage_tag'] or config.get('recovery')) and mp in (1,3))
                        except (TypeError,struct.error): pass
                        if not valid:
                            with lock: counts[3]['unexpected']+=1
                            # Never ACK unidentified persistent data.
                            raise ValueError('unexpected message in owned destination')
                        with lock:
                            if config.get('recovery') or seen[mp].add(pub,seq):
                                counts[mp]['received']+=1
                                latency=max(0,time.monotonic_ns()-sent)
                                counts[mp]['latency_count']+=1
                                counts[mp]['latency_sum_ns']+=latency
                                counts[mp]['latency_max_ns']=max(counts[mp]['latency_max_ns'],latency)
                            else: counts[mp]['duplicates']+=1
                        if config['mode']=='persistent': component.ack(msg)
                elif p in (1,3) and not publisher_stop.is_set():
                    if current!=p:
                        if current in (1,3):done.add(current)
                        current=p;sequence=0;start=until.value-config['durations'][str(p)]
                    now=time.monotonic()
                    if now>=until.value:
                        done.add(p);time.sleep(.005)
                    else:
                        due=int((now-start)*config['rate'])
                        # Bounded batches: no unbounded catch-up and no publish after deadline.
                        for _ in range(min(128,max(0,due-sequence))):
                            if time.monotonic()>=until.value or publisher_stop.is_set():break
                            payload=bytearray(HEADER.pack(config['campaign_tag'],config['stage_tag'],config['publisher_index'],p,sequence,time.monotonic_ns())+pad)
                            with lock:counts[p]['attempted']+=1
                            try:
                                if config['mode']=='persistent':component.publish(payload,Topic.of(config['topic']),user_context=p)
                                else:component.publish(payload,Topic.of(config['topic']))
                                with lock:counts[p]['submitted']+=1
                            except Exception as e:
                                if 'backpressure' in type(e).__name__.lower():
                                    with lock:counts[p]['rejected']+=1
                                else: raise
                            sequence+=1
                        sleep_for=(sequence+1)/config['rate']-(time.monotonic()-start)
                        if sleep_for>0:time.sleep(min(.01,sleep_for))
                else:
                    if current in (1,3):done.add(current)
                    time.sleep(.01)
                if time.monotonic()-last_emit>=1:emit()
            if current in (1,3):done.add(current)
            component.terminate(10_000);component=None
            service.disconnect();service=None
    except BaseException as e:
        clean=False;events.put(dict(event='error',id=config['id'],error_type=type(e).__name__))
    finally:
        if component:
            try:component.terminate(2000)
            except Exception:clean=False
        if service:
            try:service.disconnect()
            except Exception:clean=False
        emit('stopped',clean_shutdown=clean)
