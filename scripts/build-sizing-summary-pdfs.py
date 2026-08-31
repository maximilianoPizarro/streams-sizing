#!/usr/bin/env python3
"""Generate summary PDFs (entry + full) with subscription calc covered. No DOCX."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "salida"

RH_RED = colors.HexColor("#EE0000")
HEADER_BG = colors.HexColor("#1B1B1B")
ROW_ALT = colors.HexColor("#F5F5F5")
GRID = colors.HexColor("#CCCCCC")
MUTED = colors.HexColor("#555555")


def styles():
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "t",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=16,
            textColor=RH_RED,
            alignment=TA_LEFT,
            spaceAfter=8,
            leading=20,
        ),
        "h1": ParagraphStyle(
            "h",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=12,
            textColor=RH_RED,
            spaceBefore=0,
            spaceAfter=8,
            leading=15,
        ),
        "body": ParagraphStyle(
            "b",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=13,
            spaceAfter=6,
        ),
        "muted": ParagraphStyle(
            "m",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            textColor=MUTED,
            leading=11,
            spaceAfter=4,
        ),
        "cell": ParagraphStyle(
            "c", parent=base["Normal"], fontName="Helvetica", fontSize=8.5, leading=11
        ),
        "cell_h": ParagraphStyle(
            "ch",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=8.5,
            leading=11,
            textColor=colors.white,
        ),
        "center": ParagraphStyle(
            "ctr",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=9,
            alignment=TA_CENTER,
            leading=12,
            spaceAfter=6,
        ),
    }


def table(data, col_widths, sty):
    rows = []
    for i, row in enumerate(data):
        s = sty["cell_h"] if i == 0 else sty["cell"]
        rows.append([Paragraph(str(c), s) for c in row])
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("BOX", (0, 0), (-1, -1), 0.8, HEADER_BG),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            cmds.append(("BACKGROUND", (0, i), (-1, i), ROW_ALT))
    t.setStyle(TableStyle(cmds))
    t.hAlign = "CENTER"
    return t


def sheet(story, sty, title, note, data, widths):
    story.append(Paragraph(title, sty["h1"]))
    if note:
        story.append(Paragraph(note, sty["muted"]))
    story.append(Spacer(1, 3 * mm))
    story.append(table(data, widths, sty))
    story.append(PageBreak())


def build_pdf(path: Path, scenario_title: str, meta: dict):
    sty = styles()
    w = A4[0] - 3.2 * cm
    doc = SimpleDocTemplate(
        str(path),
        pagesize=A4,
        leftMargin=1.6 * cm,
        rightMargin=1.6 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
        title=scenario_title,
    )
    story = []

    # Cover
    story.append(Paragraph("Dimensionamiento Streams for Apache Kafka 3.2", sty["title"]))
    story.append(Paragraph(scenario_title, sty["center"]))
    story.append(
        Paragraph(
            f"Fecha: {date.today().isoformat()} · Engine 1.1.2 · Kafka 4.x (KRaft) · RHAF",
            sty["muted"],
        )
    )
    story.append(
        Paragraph(
            "Streams forma parte de RHAF. Totales por clúster sized. "
            "Dual-sitio ≈ ×2 hard Streams.",
            sty["body"],
        )
    )
    story.append(Spacer(1, 4 * mm))
    story.append(
        table(
            [
                ["Concepto", "Detalle"],
                [
                    "OpenShift — Streams (RHAF)",
                    f"{meta['nodes']} nodos ({meta['brokers']} brokers + {meta['controllers']} controllers) · {meta['kafka_vcpu']} vCPU",
                ],
                [
                    "RHAF complementarios",
                    f"{meta['rhaf_instances']} instancias · {meta['rhaf_vcpu']} vCPU",
                ],
                [
                    "Integraciones (orientativo)",
                    f"{meta['integ_instances']} instancias · {meta['integ_vcpu']} vCPU",
                ],
                ["Ingress @ proyección", f"{meta['ingress']} MB/s · binding {meta['binding']}"],
                ["Dato Kafka", meta["data_tb"]],
                ["Disco aprovisionado", meta["disk_tb"]],
                ["Suscripción Streams", f"{meta['cores']} cores"],
            ],
            [6 * cm, w - 6 * cm],
            sty,
        )
    )
    story.append(PageBreak())

    # Subscription calc page
    sheet(
        story,
        sty,
        "Cálculo de suscripción Streams",
        "Solo reporting de cores; no cambia el sizing físico de brokers/controllers.",
        [
            ["Campo", "Valor"],
            ["Política usada", "failoverExcluded"],
            [
                "Fórmula",
                f"({meta['brokers']} brokers − 1) × {meta['vcpu_per_broker']} vCPU/broker",
            ],
            [
                "Cálculo",
                f"({meta['brokers']} − 1) × {meta['vcpu_per_broker']} = {meta['cores']} cores",
            ],
            [
                "Alternativa core pairs",
                f"({meta['brokers']} × {meta['vcpu_per_broker']}) ÷ 2 = {meta['core_pairs']}",
            ],
            [
                "Controllers",
                f"{meta['controllers']} × {meta['vcpu_per_ctrl']} vCPU — no cuentan en la línea de brokers",
            ],
            [
                "Interpretación",
                "Se excluye 1 broker como holgura de failover. Validar política final con contracts.",
            ],
        ],
        [5.5 * cm, w - 5.5 * cm],
    )

    # Capacity detail
    sheet(
        story,
        sty,
        "Detalle de capacidad (clúster sized)",
        meta.get("capacity_note", ""),
        [
            ["Métrica", "Valor"],
            ["Brokers", f"{meta['brokers']} × {meta['vcpu_per_broker']} vCPU · {meta['mem_broker']} Gi · {meta['pvc']}"],
            ["Controllers KRaft", f"{meta['controllers']} × {meta['vcpu_per_ctrl']} vCPU · {meta['mem_ctrl']} Gi"],
            ["Total vCPU Streams", str(meta["kafka_vcpu"])],
            ["RHAF complementarios", f"{meta['rhaf_vcpu']} vCPU (Registry, Bridge, MM2, Console, Keycloak, Cruise Control)"],
            ["Dual-sitio (A+B) Streams", f"≈ {meta['nodes'] * 2} nodos · {meta['kafka_vcpu'] * 2} vCPU"],
        ],
        [6 * cm, w - 6 * cm],
    )

    # Services (non-blocking)
    story.append(Paragraph("Auth / permisos — canalizable por Services", sty["h1"]))
    story.append(
        Paragraph(
            "La migración LDAP/AD/Kerberos/Ranger → OAuth/OIDC y permisos por tópico "
            "<b>no bloquea</b> este dimensionamiento. Se puede canalizar con Red Hat Services "
            "(discovery + diseño + enablement; esfuerzo orientativo ~1–2 semanas según alcance).",
            sty["body"],
        )
    )

    OUT.mkdir(parents=True, exist_ok=True)
    doc.build(story)
    return path


def main():
    # Shared compute at RF+1 floor — entry and full differ on disk/ingress
    common = {
        "nodes": 7,
        "brokers": 4,
        "controllers": 3,
        "kafka_vcpu": 44,
        "vcpu_per_broker": 8,
        "vcpu_per_ctrl": 4,
        "mem_broker": 32,
        "mem_ctrl": 16,
        "rhaf_instances": 10,
        "rhaf_vcpu": 14,
        "integ_instances": 4,
        "integ_vcpu": 6,
        "cores": 24,
        "core_pairs": 16,
        "binding": "disk",
    }

    entry = {
        **common,
        "ingress": 2.4,
        "data_tb": "1.87 TB",
        "disk_tb": "2.76 TB",
        "pvc": "614 GB PVC",
        "capacity_note": "Entry 10% (50 TPS → ~300 TPS @ ~24 m).",
    }
    full = {
        **common,
        "ingress": 24.01,
        "data_tb": "18.67 TB",
        "disk_tb": "24.81 TB",
        "pvc": "6127 GB PVC",
        "capacity_note": "Full aggregate (500 TPS → ~3001 TPS @ ~24 m).",
    }

    p1 = build_pdf(
        OUT / "sizing-entry-10pct-50tps.pdf",
        "Escenario entry-level 10% (50 → ~300 TPS)",
        entry,
    )
    p2 = build_pdf(
        OUT / "sizing-peak-500-to-3k-tps.pdf",
        "Escenario agregado full (500 → ~3K TPS)",
        full,
    )
    print(f"Wrote {p1} ({p1.stat().st_size} bytes)")
    print(f"Wrote {p2} ({p2.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
