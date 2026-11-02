"""Deploy the single-page site to the authorized AWS SSO account.

Run: python3 site/build.py && python3 site/deploy.py
Creates only broker-capacity named resources; repeat runs update code/HTML.
"""
from __future__ import annotations

import json
import os
import secrets
import time
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent
DOMAIN = "broker-capacity.sol-se-emea.com"
ZONE = "Z019296612NXPBFLCWDSP"  # authoritative public zone, verified against dig NS
CERT = "arn:aws:acm:us-east-1:928672620016:certificate/13285189-cefc-4ff2-b11f-c104b1af9322"
ACCOUNT = "928672620016"
REGION = "us-east-1"
BUCKET = "broker-capacity-sol-se-emea-com-928672620016"
FUNCTION = "broker-capacity-readonly-connect"
ROLE = "broker-capacity-site-lambda-role"
SECRET_FILE = ROOT.parent / ".pilot" / "broker-capacity-site-origin-secret"
STATE_FILE = ROOT / "deployment.json"
CACHE_OPTIMIZED = "658327ea-f89d-4fab-a63d-7e88639e58f6"
CACHE_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
FORWARD_VIEWER_EXCEPT_HOST = "b689b0a8-53d0-40ab-baf2-68738e2966ac"


def gone(error, code):
    return isinstance(error, ClientError) and error.response["Error"]["Code"] == code


def secret():
    if not SECRET_FILE.exists():
        SECRET_FILE.parent.mkdir(exist_ok=True)
        fd = os.open(SECRET_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(secrets.token_urlsafe(48))
    if SECRET_FILE.stat().st_mode & 0o077:
        raise RuntimeError("Origin secret permissions must be owner-only")
    return SECRET_FILE.read_text().strip()


def ensure_role(iam):
    trust = {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
    try:
        arn=iam.get_role(RoleName=ROLE)["Role"]["Arn"]
    except ClientError as e:
        if not gone(e,"NoSuchEntity"): raise
        arn=iam.create_role(RoleName=ROLE,AssumeRolePolicyDocument=json.dumps(trust),Tags=[{"Key":"Project","Value":"broker-capacity-model"}])["Role"]["Arn"]
    iam.attach_role_policy(RoleName=ROLE,PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    return arn


def code_zip():
    b=BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(ROOT/"lambda_function.py","lambda_function.py")
    return b.getvalue()


def ensure_function(lam,role,origin_secret):
    try:
        lam.get_function(FunctionName=FUNCTION)
    except ClientError as e:
        if not gone(e,"ResourceNotFoundException"): raise
        # IAM role propagation may take a few seconds after first creation.
        for attempt in range(12):
            try:
                lam.create_function(FunctionName=FUNCTION,Runtime="python3.12",Role=role,Handler="lambda_function.handler",Code={"ZipFile":code_zip()},Timeout=20,MemorySize=128,Environment={"Variables":{"BCM_ORIGIN_SECRET":origin_secret}},Tags={"Project":"broker-capacity-model","Owner":"raphael-caillon"})
                break
            except ClientError as x:
                if gone(x,"InvalidParameterValueException") and attempt<11: time.sleep(2)
                else: raise
    else:
        lam.update_function_code(FunctionName=FUNCTION,ZipFile=code_zip(),Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=FUNCTION)
        lam.update_function_configuration(FunctionName=FUNCTION,Environment={"Variables":{"BCM_ORIGIN_SECRET":origin_secret}},Timeout=20,MemorySize=128)
    lam.get_waiter("function_updated").wait(FunctionName=FUNCTION)
    try:
        lam.put_function_concurrency(FunctionName=FUNCTION,ReservedConcurrentExecutions=2)
    except ClientError as e:
        if not gone(e,"InvalidParameterValueException"): raise
    try:
        url=lam.get_function_url_config(FunctionName=FUNCTION)["FunctionUrl"]
    except ClientError as e:
        if not gone(e,"ResourceNotFoundException"): raise
        url=lam.create_function_url_config(FunctionName=FUNCTION,AuthType="NONE",Cors={"AllowOrigins":[f"https://{DOMAIN}"],"AllowMethods":["POST"],"AllowHeaders":["content-type"]})["FunctionUrl"]
    for sid,action,extra in [("AllowPublicFunctionUrl","lambda:InvokeFunctionUrl",{"FunctionUrlAuthType":"NONE"}),("AllowPublicFunctionUrlInvoke","lambda:InvokeFunction",{"InvokedViaFunctionUrl":True})]:
        try: lam.add_permission(FunctionName=FUNCTION,StatementId=sid,Action=action,Principal="*",**extra)
        except ClientError as e:
            if not gone(e,"ResourceConflictException"): raise
    return url


def ensure_bucket(s3):
    try: s3.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        if not gone(e,"404") and not gone(e,"NoSuchBucket"): raise
        s3.create_bucket(Bucket=BUCKET)
    s3.put_public_access_block(Bucket=BUCKET,PublicAccessBlockConfiguration={"BlockPublicAcls":True,"IgnorePublicAcls":True,"BlockPublicPolicy":True,"RestrictPublicBuckets":True})
    s3.put_bucket_encryption(Bucket=BUCKET,ServerSideEncryptionConfiguration={"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]})
    s3.put_bucket_versioning(Bucket=BUCKET,VersioningConfiguration={"Status":"Enabled"})
    s3.put_bucket_tagging(Bucket=BUCKET,Tagging={"TagSet":[{"Key":"Project","Value":"broker-capacity-model"},{"Key":"Owner","Value":"raphael-caillon"}]})
    for name,content_type in (("index.html","text/html; charset=utf-8"),
                              ("experiments.json","application/json; charset=utf-8"),
                              ("experiments.csv","text/csv; charset=utf-8")):
        s3.upload_file(str(ROOT/"dist"/name),BUCKET,name,
                       ExtraArgs={"ContentType":content_type,"CacheControl":"public,max-age=60,must-revalidate"})


def ensure_oac(cf):
    for item in cf.list_origin_access_controls().get("OriginAccessControlList",{}).get("Items",[]):
        if item["Name"]=="broker-capacity-site-oac": return item["Id"]
    return cf.create_origin_access_control(OriginAccessControlConfig={"Name":"broker-capacity-site-oac","Description":"Private S3 access for broker capacity site","SigningProtocol":"sigv4","SigningBehavior":"always","OriginAccessControlOriginType":"s3"})["OriginAccessControl"]["Id"]


def ensure_distribution(cf,oac,function_url,origin_secret):
    found=None
    paginator=cf.get_paginator("list_distributions")
    for page in paginator.paginate():
        for item in page.get("DistributionList",{}).get("Items",[]):
            if DOMAIN in item.get("Aliases",{}).get("Items",[]): found=item
    if found: return found["Id"],found["DomainName"]
    lambda_host=urlparse(function_url).hostname
    methods={"Quantity":7,"Items":["GET","HEAD","OPTIONS","PUT","PATCH","POST","DELETE"],"CachedMethods":{"Quantity":2,"Items":["GET","HEAD"]}}
    config={
        "CallerReference":"broker-capacity-20261102-v1",
        "Aliases":{"Quantity":1,"Items":[DOMAIN]},
        "DefaultRootObject":"index.html",
        "Origins":{"Quantity":2,"Items":[
            {"Id":"private-site","DomainName":f"{BUCKET}.s3.{REGION}.amazonaws.com","OriginAccessControlId":oac,"S3OriginConfig":{"OriginAccessIdentity":""}},
            {"Id":"readonly-api","DomainName":lambda_host,"CustomHeaders":{"Quantity":1,"Items":[{"HeaderName":"x-bcm-origin","HeaderValue":origin_secret}]},"CustomOriginConfig":{"HTTPPort":80,"HTTPSPort":443,"OriginProtocolPolicy":"https-only","OriginSslProtocols":{"Quantity":1,"Items":["TLSv1.2"]},"OriginReadTimeout":25,"OriginKeepaliveTimeout":5}},
        ]},
        "DefaultCacheBehavior":{"TargetOriginId":"private-site","ViewerProtocolPolicy":"redirect-to-https","AllowedMethods":{"Quantity":2,"Items":["GET","HEAD"],"CachedMethods":{"Quantity":2,"Items":["GET","HEAD"]}},"Compress":True,"CachePolicyId":CACHE_OPTIMIZED},
        "CacheBehaviors":{"Quantity":1,"Items":[{"PathPattern":"/api/*","TargetOriginId":"readonly-api","ViewerProtocolPolicy":"https-only","AllowedMethods":methods,"Compress":True,"CachePolicyId":CACHE_DISABLED,"OriginRequestPolicyId":FORWARD_VIEWER_EXCEPT_HOST}]},
        "CustomErrorResponses":{"Quantity":0},"Comment":"Broker Capacity Studio; private static S3 and read-only Lambda lookup", "Logging":{"Enabled":False,"IncludeCookies":False,"Bucket":"","Prefix":""},"PriceClass":"PriceClass_100","Enabled":True,
        "ViewerCertificate":{"ACMCertificateArn":CERT,"SSLSupportMethod":"sni-only","MinimumProtocolVersion":"TLSv1.2_2021","CertificateSource":"acm"},
        "Restrictions":{"GeoRestriction":{"RestrictionType":"none","Quantity":0}},"HttpVersion":"http2and3","IsIPV6Enabled":True,
    }
    out=cf.create_distribution_with_tags(DistributionConfigWithTags={"DistributionConfig":config,"Tags":{"Items":[{"Key":"Project","Value":"broker-capacity-model"},{"Key":"Owner","Value":"raphael-caillon"}]}})["Distribution"]
    return out["Id"],out["DomainName"]


def policy_and_dns(s3,route53,distribution_id,distribution_domain):
    arn=f"arn:aws:cloudfront::{ACCOUNT}:distribution/{distribution_id}"
    policy={"Version":"2012-10-17","Statement":[{"Sid":"CloudFrontReadOnly","Effect":"Allow","Principal":{"Service":"cloudfront.amazonaws.com"},"Action":"s3:GetObject","Resource":f"arn:aws:s3:::{BUCKET}/*","Condition":{"StringEquals":{"AWS:SourceArn":arn}}}]}
    s3.put_bucket_policy(Bucket=BUCKET,Policy=json.dumps(policy))
    target={"HostedZoneId":"Z2FDTNDATAQYW2","DNSName":distribution_domain,"EvaluateTargetHealth":False}
    changes=[{"Action":"UPSERT","ResourceRecordSet":{"Name":DOMAIN+".","Type":t,"AliasTarget":target}} for t in ("A","AAAA")]
    route53.change_resource_record_sets(HostedZoneId=ZONE,ChangeBatch={"Comment":"Broker Capacity Studio CloudFront","Changes":changes})


def main():
    sts=boto3.client("sts")
    if sts.get_caller_identity()["Account"]!=ACCOUNT: raise RuntimeError("Wrong AWS SSO account")
    acm=boto3.client("acm",region_name=REGION)
    if acm.describe_certificate(CertificateArn=CERT)["Certificate"]["Status"]!="ISSUED": raise RuntimeError("Certificate not yet issued")
    if not (ROOT/"dist"/"index.html").exists(): raise RuntimeError("Run build.py first")
    origin_secret=secret()
    iam=boto3.client("iam"); lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION); cf=boto3.client("cloudfront"); route53=boto3.client("route53")
    print("AWS account verified; deploying named site resources")
    role=ensure_role(iam)
    function_url=ensure_function(lam,role,origin_secret)
    print("Read-only lookup Lambda ready")
    ensure_bucket(s3)
    print("Private site bucket uploaded")
    oac=ensure_oac(cf)
    distribution_id,distribution_domain=ensure_distribution(cf,oac,function_url,origin_secret)
    policy_and_dns(s3,route53,distribution_id,distribution_domain)
    paths=["/index.html","/","/experiments.json","/experiments.csv"]
    cf.create_invalidation(DistributionId=distribution_id,InvalidationBatch={"Paths":{"Quantity":len(paths),"Items":paths},"CallerReference":"bcm-site-"+str(time.time_ns())})
    state={"domain":DOMAIN,"bucket":BUCKET,"function":FUNCTION,"distributionId":distribution_id,"distributionDomain":distribution_domain,"hostedZoneId":ZONE,"certificateArn":CERT}
    STATE_FILE.write_text(json.dumps(state,indent=2)+"\n")
    print(json.dumps(state,indent=2))

if __name__=="__main__": main()
