#!/usr/bin/env python3
"""Render the one-page, versioned 1K/5K/10K experiment proposal."""
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "output/pdf/paired_multisize_experiment_proposal.pdf"
INK = colors.HexColor("#142b37")
MUTED = colors.HexColor("#4a6471")
GREEN = colors.HexColor("#008c72")
PALE = colors.HexColor("#e9f5f1")
LINE = colors.HexColor("#d7e3e6")


def font_setup():
    regular = Path("/System/Library/Fonts/Supplemental/Arial.ttf")
    bold = Path("/System/Library/Fonts/Supplemental/Arial Bold.ttf")
    if regular.exists() and bold.exists():
        pdfmetrics.registerFont(TTFont("ArialLocal", str(regular)))
        pdfmetrics.registerFont(TTFont("ArialLocal-Bold", str(bold)))
        pdfmetrics.registerFontFamily("ArialLocal", normal="ArialLocal", bold="ArialLocal-Bold")
        return "ArialLocal", "ArialLocal-Bold"
    return "Helvetica", "Helvetica-Bold"


def draw_paragraph(pdf, text, x, top, width, style):
    paragraph = Paragraph(text, style)
    _, height = paragraph.wrap(width, 1000)
    paragraph.drawOn(pdf, x, top - height)
    return top - height


def section(pdf, title, items, x, top, width, regular, bold):
    pdf.setFillColor(GREEN)
    pdf.setFont(bold, 10)
    pdf.drawString(x, top, title)
    top -= 8
    pdf.setStrokeColor(LINE)
    pdf.line(x, top, x + width, top)
    top -= 10
    style = ParagraphStyle("body", fontName=regular, fontSize=8.65, leading=11.6,
                           textColor=INK, alignment=TA_LEFT, spaceAfter=0)
    for text in items:
        top = draw_paragraph(pdf, text, x, top, width, style) - 5
    return top - 12


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    regular, bold = font_setup()
    pdf = canvas.Canvas(str(OUTPUT), pagesize=A4, pageCompression=1)
    pdf.setTitle("1K / 5K / 10K broker capacity experiment proposal")
    pdf.setAuthor("Broker Capacity Model")
    width, height = A4
    margin = 36

    pdf.setFillColor(INK)
    pdf.rect(0, height - 107, width, 107, fill=1, stroke=0)
    pdf.setFillColor(colors.white)
    pdf.setFont(bold, 20)
    pdf.drawString(margin, height - 45, "Broker capacity: paired experiment")
    pdf.setFont(regular, 10)
    pdf.drawString(margin, height - 65, "1K, 5K and 10K | proposal v1 | 3 November 2026")
    pdf.setFillColor(colors.HexColor("#a5d9cd"))
    pdf.setFont(regular, 8.5)
    pdf.drawString(margin, height - 88, "Prepared only - no broker provisioning or load run authorized by this plan")

    y = height - 122
    box_width = (width - 2 * margin - 16) / 3
    for index, (number, caption) in enumerate([
        ("12", "matched A/B pairs per class"),
        ("72", "adaptive boundary slots total"),
        ("24", "pre/post drift checks total"),
    ]):
        x = margin + index * (box_width + 8)
        pdf.setFillColor(PALE)
        pdf.roundRect(x, y - 51, box_width, 51, 5, fill=1, stroke=0)
        pdf.setFillColor(INK)
        pdf.setFont(bold, 18)
        pdf.drawString(x + 10, y - 24, number)
        pdf.setFont(regular, 8)
        pdf.drawString(x + 10, y - 39, caption)
    y -= 70

    gap = 20
    col_width = (width - 2 * margin - gap) / 2
    left_x, right_x = margin, margin + col_width + gap
    left = y
    left = section(pdf, "Question and first contrast", [
        "Estimate a <b>broker-attributed sustainable publish-rate interval</b> for one frozen broker and workload. Record a separate low-queue fast-path threshold only when native broker path evidence supports it.",
        "Reference: SMF/TLS Persistent, 1,024-byte seeded payload, two distinct queue copies, four publisher connections, one consumer per queue, exclusive queues.",
        "A/B: subscriber ACK window <b>255 vs 64</b> (<b>-asw</b>). This first test checks whether consumer flow control explains earlier queue bursts; it is not yet a general broker-feature multiplier.",
    ], left_x, left, col_width, regular, bold)
    left = section(pdf, "Randomization and run length", [
        "Per class: four sessions across at least two dates, three adjacent A/B pairs per session. The seeded schedule balances <b>6 AB and 6 BA</b> orders, with both orders in every session. A safe reference check brackets each session.",
        "Each arm searches a boundary: <b>60-second scouts</b>, then independently started <b>300-second stable and upper trials</b>. Repeat an upper candidate for another 300 seconds. Search until the decision bracket is at most <b>1%</b> wide.",
        "Each of the 72 boundary slots may need several rate trials. The minimum is 12 broker-hours of 300-second confirmations; scouts, drains and repeats add time.",
    ], left_x, left, col_width, regular, bold)
    left = section(pdf, "Frozen environment", [
        "Prefer Solace PubSub+ <b>10.26</b> for all classes. The previous dedicated 5K preflight reported 10.26; revalidate live. Freeze exact build, service ID, VPN, region/AZ, limits, queues, SDKPerf binary hash, Java version, AMI and payload seed.",
        "Candidate generator profile: separate publisher and subscriber <b>AWS c7i.xlarge</b> VMs in us-east-1 near the broker. Qualify CPU, NIC/PPS and TCP headroom. If a stronger VM is needed, start a new apparatus epoch and rerun matched arms.",
    ], left_x, left, col_width, regular, bold)

    right = y
    right = section(pdf, "Measurement and rejection gates", [
        "Accept a stable trial only if accepted ingress tracks the target by at least <b>98%</b>, variation/trend is within <b>5%</b>, each destination reconciles unique delivery and CRC, and low-water queue backlog does not trend upward.",
        "An overload candidate needs same-destination egress deficit (at least <b>2% or 100 msg/s</b>) plus rising lag and queue floor. Repeat it with independently qualified clients. Client, network, quota or telemetry limits are <b>inconclusive</b>, not broker capacity.",
        "For a displayed rate rounded to 100 msg/s, require a validated bracket no wider than <b>100 msg/s</b>; otherwise show the interval.",
    ], right_x, right, col_width, regular, bold)
    right = section(pdf, "What counts as a meaningful effect?", [
        "Analyze paired <b>log(B/A)</b> capacity ratios, using conservative boundary endpoints and the wider of pair-level and session-level t intervals. The preregistered smallest effect of interest is <b>5%</b>.",
        "<b>Equivalent:</b> full 90% interval inside -5% to +5%. <b>Material:</b> full 95% interval beyond +5% or -5%. Otherwise unresolved; add a new 12-pair batch or repair measurement. One standard deviation of raw runs is not an equivalence rule.",
        "A decision applies only to the measured class, broker instance and reference workload. Retest changed payload/fanout contexts and an independent broker before publishing a portable scaling law.",
    ], right_x, right, col_width, regular, bold)
    right = section(pdf, "Reproduce and update", [
        "Freeze the seeded plan and its SHA-256, raw SEMP/SDKPerf/host traces, exact commands, cost journal, verified boundary artifacts and analysis version. Do not overwrite a finished epoch.",
        "A reviewed result updates the versioned <b>model/calibration-v1.json</b> through a new commit and site build. New features need a name, documentation link, applicability and evidence; unverified coefficients stay illustrative.",
    ], right_x, right, col_width, regular, bold)

    bottom = min(left, right)
    if bottom < 103:
        raise RuntimeError(f"Proposal content reaches too low on page: {bottom:.1f}")
    pdf.setStrokeColor(LINE)
    pdf.line(margin, 91, width - margin, 91)
    pdf.setFillColor(MUTED)
    pdf.setFont(regular, 7.7)
    pdf.drawString(margin, 77, "Readiness: dedicated isolated 1K and 10K targets still needed; 5K requires live revalidation.")
    pdf.drawString(margin, 64, "Budget: $20/day new generator/traffic ceiling, $16 operating guard; broker subscriptions separate.")
    pdf.drawString(margin, 49, "Design basis: NIST randomized blocks and practical-equivalence testing; see methodology document for gates.")
    pdf.setFillColor(GREEN)
    pdf.setFont(bold, 7.7)
    pdf.drawRightString(width - margin, 33, "BROKER CAPACITY MODEL / 01")
    pdf.showPage()
    pdf.save()
    print(OUTPUT)


if __name__ == "__main__":
    main()
