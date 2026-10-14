#!/usr/bin/env python3
"""Generate the Ask ADZA community teaching PDF (landscape 16:9 slides)."""
from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph

from ask_adza_teaching_content import DOC_VERSION, DOCS_DIR, OUTPUT_PDF, SLIDES

PAGE_W = 13.333 * inch
PAGE_H = 7.5 * inch

# Two greens + white
WHITE = colors.HexColor("#FFFFFF")
GREEN_DARK = colors.HexColor("#146C43")
GREEN_LIGHT = colors.HexColor("#E6F4EC")

BG = WHITE
BG_ALT = GREEN_LIGHT
ACCENT = GREEN_DARK
TEXT = GREEN_DARK
MUTED = GREEN_DARK
CARD = GREEN_LIGHT
CARD_LINE = GREEN_DARK
HEADER = GREEN_DARK

MARGIN = 0.55 * inch
FOOTER_H = 0.42 * inch
TITLE_TOP = PAGE_H - 0.22 * inch
BODY_TOP = PAGE_H - 1.05 * inch
FOOTNOTE_TOP = 0.72 * inch


def _escape(text: str) -> str:
    return (
        (text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


def _style(
    name: str,
    *,
    size: int,
    color=TEXT,
    leading: float | None = None,
    bold: bool = False,
    align=TA_LEFT,
) -> ParagraphStyle:
    return ParagraphStyle(
        name,
        fontName="Helvetica-Bold" if bold else "Helvetica",
        fontSize=size,
        leading=leading if leading is not None else size * 1.28,
        textColor=color,
        alignment=align,
        spaceBefore=0,
        spaceAfter=0,
    )


STY_TITLE = _style("t", size=22, bold=True, leading=26)
STY_HERO = _style("hero", size=42, bold=True, leading=48)
STY_KICKER = _style("kicker", size=12, color=ACCENT, bold=True, leading=16)
STY_SUB = _style("sub", size=16, color=MUTED, leading=22)
STY_BODY = _style("body", size=13, leading=18)
STY_BODY_SM = _style("bodysm", size=11, leading=15)
STY_BULLET = _style("bullet", size=13, leading=17)
STY_CARD_H = _style("cardh", size=13, color=ACCENT, bold=True, leading=17)
STY_FOOT = _style("foot", size=10, color=GREEN_DARK, leading=13)
STY_MUTED = _style("muted", size=10, color=GREEN_DARK, leading=13)
STY_CENTER = _style("ctr", size=11, bold=True, leading=14, align=TA_CENTER)
STY_STEP = _style("step", size=9, bold=True, leading=12, align=TA_CENTER)
STY_STEP_ON_DARK = _style("stepd", size=9, color=WHITE, bold=True, leading=12, align=TA_CENTER)
STY_CELL = _style("cell", size=9, leading=12)
STY_CELL_H = _style("cellh", size=10, color=WHITE, bold=True, leading=13)
STY_CAP = _style("cap", size=11, leading=15)
STY_MINI = _style("mini", size=7.5, leading=9.5, align=TA_CENTER)
STY_MINI_L = _style("minil", size=7.5, leading=9.5)
STY_MINI_ON_DARK = _style("minid", size=7.5, color=WHITE, leading=9.5, align=TA_CENTER)
STY_MINI_H = _style("minih", size=8, bold=True, leading=10, align=TA_CENTER)
STY_MINI_H_DARK = _style("minihd", size=8, color=WHITE, bold=True, leading=10, align=TA_CENTER)


def _draw_para(
    c: canvas.Canvas,
    text: str,
    x: float,
    y_top: float,
    width: float,
    max_height: float,
    style: ParagraphStyle,
) -> float:
    if not text or max_height < 8:
        return 0.0
    p = Paragraph(_escape(text), style)
    _w, h = p.wrap(width, max_height)
    h = min(h, max_height)
    p.drawOn(c, x, y_top - h)
    return h


def _fill_page(c: canvas.Canvas, color=BG) -> None:
    c.setFillColor(color)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    c.setFillColor(GREEN_DARK)
    c.rect(0, PAGE_H - 0.08 * inch, PAGE_W, 0.08 * inch, fill=1, stroke=0)
    c.rect(0, 0, PAGE_W, 0.06 * inch, fill=1, stroke=0)


def _round_card(c: canvas.Canvas, x: float, y: float, w: float, h: float, *, radius: float = 8) -> None:
    c.setFillColor(CARD)
    c.setStrokeColor(CARD_LINE)
    c.setLineWidth(0.8)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=1)


def _chrome(c: canvas.Canvas, spec: dict, page_i: int, page_n: int, *, alt: bool = False) -> None:
    _fill_page(c, BG_ALT if alt else BG)
    title = str(spec.get("title") or "")
    if spec.get("kind") not in ("title", "section") and title:
        _draw_para(c, title, MARGIN, TITLE_TOP - 0.12 * inch, PAGE_W - 2 * MARGIN, 0.7 * inch, STY_TITLE)
    c.setFillColor(MUTED)
    c.setFont("Helvetica", 8)
    label = f"Ask ADZA  ·  v{DOC_VERSION}  ·  {page_i} / {page_n}"
    c.drawString(MARGIN, 0.18 * inch, label)
    c.drawRightString(PAGE_W - MARGIN, 0.18 * inch, "OpenTrace community teaching")


def _footnote(c: canvas.Canvas, text: str | None) -> None:
    if not text:
        return
    _draw_para(c, text, MARGIN, FOOTNOTE_TOP, PAGE_W - 2 * MARGIN, 0.42 * inch, STY_FOOT)


def _render_title(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    _draw_para(c, str(spec.get("kicker") or ""), MARGIN, PAGE_H - 1.55 * inch, PAGE_W - 2 * MARGIN, 0.4 * inch, STY_KICKER)
    _draw_para(c, str(spec.get("title") or ""), MARGIN, PAGE_H - 2.15 * inch, PAGE_W - 2 * MARGIN, 1.1 * inch, STY_HERO)
    c.setFillColor(ACCENT)
    c.rect(MARGIN, PAGE_H - 3.45 * inch, 1.4 * inch, 0.07 * inch, fill=1, stroke=0)
    _draw_para(
        c,
        str(spec.get("subtitle") or ""),
        MARGIN,
        PAGE_H - 3.6 * inch,
        PAGE_W - 2 * MARGIN,
        0.6 * inch,
        STY_SUB,
    )
    _draw_para(
        c,
        str(spec.get("footer") or ""),
        MARGIN,
        PAGE_H - 4.35 * inch,
        PAGE_W - 2 * MARGIN,
        0.5 * inch,
        _style("tagline", size=13, color=GREEN_DARK, leading=18),
    )


def _render_section(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n, alt=True)
    c.setFillColor(ACCENT)
    c.rect(MARGIN, PAGE_H / 2 + 0.55 * inch, 1.15 * inch, 0.09 * inch, fill=1, stroke=0)
    _draw_para(
        c,
        str(spec.get("title") or ""),
        MARGIN,
        PAGE_H / 2 + 0.4 * inch,
        PAGE_W - 2 * MARGIN,
        1.1 * inch,
        _style("sec", size=30, bold=True, leading=36),
    )
    _draw_para(
        c,
        str(spec.get("subtitle") or ""),
        MARGIN,
        PAGE_H / 2 - 0.75 * inch,
        PAGE_W - 2 * MARGIN,
        0.8 * inch,
        STY_SUB,
    )


def _render_bullets(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    title = str(spec.get("title") or "")
    if title == "What is actually stored in Qdrant":
        _render_qdrant_point(c, spec, page_i, page_n)
        return
    _chrome(c, spec, page_i, page_n)
    bullets = [str(b) for b in (spec.get("bullets") or [])][:6]
    y = BODY_TOP
    max_y = FOOTNOTE_TOP + 0.12 * inch
    for bullet in bullets:
        used = _draw_para(c, f"•  {bullet}", MARGIN + 0.1 * inch, y, PAGE_W - 2 * MARGIN - 0.1 * inch, y - max_y, STY_BULLET)
        y -= used + 0.14 * inch
        if y < max_y + 0.3 * inch:
            break
    _footnote(c, spec.get("footnote"))


def _render_qdrant_point(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    layers = [
        ("Dense vector(s)", "Named cosine E5  ·  HNSW + INT8  ·  news/research: dense  ·  OTA: insight / metric / rec"),
        ("Sparse BM25", "Qdrant/bm25 with IDF  ·  same point as dense  ·  rare tokens and exact names"),
        ("Payload + indexes", "doc_kind  ·  geo_*  ·  published_at / publication_year  ·  domains  ·  ACF fields  ·  text"),
    ]
    left = MARGIN
    col_w = 6.15 * inch
    box_h = 1.35 * inch
    gap = 0.12 * inch
    top = BODY_TOP
    for i, (head, body) in enumerate(layers):
        y = top - (i + 1) * box_h - i * gap
        _round_card(c, left, y, col_w, box_h)
        c.setFillColor(ACCENT)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left + 0.18 * inch, y + box_h - 0.28 * inch, f"{i + 1:02d}")
        _draw_para(c, head, left + 0.5 * inch, y + box_h - 0.18 * inch, col_w - 0.7 * inch, 0.32 * inch, STY_CARD_H)
        _draw_para(c, body, left + 0.18 * inch, y + box_h - 0.52 * inch, col_w - 0.36 * inch, 0.7 * inch, STY_BODY_SM)

    right_x = left + col_w + 0.28 * inch
    right_w = PAGE_W - MARGIN - right_x
    bullets = [str(b) for b in (spec.get("bullets") or [])][:5]
    y = BODY_TOP
    for bullet in bullets:
        used = _draw_para(c, f"•  {bullet}", right_x, y, right_w, 1.05 * inch, STY_BODY_SM)
        y -= used + 0.12 * inch
    _footnote(c, spec.get("footnote"))


def _render_two_column(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    gap = 0.28 * inch
    col_w = (PAGE_W - 2 * MARGIN - gap) / 2
    bottom = FOOTNOTE_TOP + 0.12 * inch
    height = BODY_TOP - 0.08 * inch - bottom
    columns = (
        (MARGIN, spec.get("left_title"), spec.get("left") or []),
        (MARGIN + col_w + gap, spec.get("right_title"), spec.get("right") or []),
    )
    for x, heading, bullets in columns:
        _round_card(c, x, bottom, col_w, height)
        _draw_para(c, str(heading or ""), x + 0.22 * inch, BODY_TOP - 0.18 * inch, col_w - 0.44 * inch, 0.4 * inch, STY_CARD_H)
        y = BODY_TOP - 0.68 * inch
        for bullet in [str(b) for b in bullets][:6]:
            used = _draw_para(
                c,
                f"•  {bullet}",
                x + 0.22 * inch,
                y,
                col_w - 0.44 * inch,
                y - bottom - 0.12 * inch,
                STY_BODY_SM,
            )
            y -= used + 0.1 * inch
    _footnote(c, spec.get("footnote"))


def _render_cards(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    cards = list(spec.get("cards") or [])[:4]
    n = max(1, len(cards))
    gap = 0.2 * inch
    total_w = PAGE_W - 2 * MARGIN
    card_w = (total_w - gap * (n - 1)) / n
    bottom = FOOTNOTE_TOP + 0.12 * inch
    height = BODY_TOP - 0.08 * inch - bottom
    for i, card in enumerate(cards):
        x = MARGIN + i * (card_w + gap)
        _round_card(c, x, bottom, card_w, height)
        c.setFillColor(ACCENT)
        c.rect(x, bottom + height - 0.08 * inch, card_w, 0.08 * inch, fill=1, stroke=0)
        _draw_para(
            c,
            str(card.get("title") or ""),
            x + 0.18 * inch,
            BODY_TOP - 0.28 * inch,
            card_w - 0.36 * inch,
            0.55 * inch,
            STY_CARD_H,
        )
        _draw_para(
            c,
            str(card.get("body") or ""),
            x + 0.18 * inch,
            BODY_TOP - 0.95 * inch,
            card_w - 0.36 * inch,
            height - 1.2 * inch,
            STY_BODY_SM,
        )
    _footnote(c, spec.get("footnote"))


def _render_table(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    headers = [str(h) for h in (spec.get("headers") or [])]
    rows = [[str(cell) for cell in row] for row in (spec.get("rows") or [])]
    if not headers:
        _footnote(c, spec.get("footnote"))
        return
    n_cols = len(headers)
    n_rows = 1 + len(rows)
    x0 = MARGIN
    width = PAGE_W - 2 * MARGIN
    col_w = width / n_cols
    top = BODY_TOP - 0.05 * inch
    bottom = FOOTNOTE_TOP + 0.14 * inch
    avail = top - bottom
    header_h = 0.38 * inch
    body_rows = max(1, n_rows - 1)
    row_h = min(0.72 * inch, (avail - header_h) / body_rows)
    # Header
    y = top - header_h
    c.setFillColor(HEADER)
    c.rect(x0, y, width, header_h, fill=1, stroke=0)
    for i, header in enumerate(headers):
        _draw_para(c, header, x0 + i * col_w + 0.08 * inch, top - 0.06 * inch, col_w - 0.16 * inch, header_h - 0.08 * inch, STY_CELL_H)
    for r, row in enumerate(rows):
        y = top - header_h - (r + 1) * row_h
        c.setFillColor(GREEN_LIGHT if r % 2 == 0 else WHITE)
        c.rect(x0, y, width, row_h, fill=1, stroke=0)
        c.setStrokeColor(CARD_LINE)
        c.setLineWidth(0.4)
        c.line(x0, y, x0 + width, y)
        for i in range(n_cols):
            value = row[i] if i < len(row) else ""
            _draw_para(
                c,
                value,
                x0 + i * col_w + 0.08 * inch,
                y + row_h - 0.08 * inch,
                col_w - 0.16 * inch,
                row_h - 0.12 * inch,
                STY_CELL,
            )
    c.setStrokeColor(CARD_LINE)
    c.setLineWidth(0.7)
    c.rect(x0, top - header_h - body_rows * row_h, width, header_h + body_rows * row_h, fill=0, stroke=1)
    _footnote(c, spec.get("footnote"))


def _arrow(c: canvas.Canvas, x1: float, y1: float, x2: float, y2: float) -> None:
    c.setStrokeColor(ACCENT)
    c.setFillColor(ACCENT)
    c.setLineWidth(1.6)
    c.line(x1, y1, x2, y2)
    angle_dx = x2 - x1
    angle_dy = y2 - y1
    length = (angle_dx ** 2 + angle_dy ** 2) ** 0.5 or 1.0
    ux, uy = angle_dx / length, angle_dy / length
    size = 6
    px, py = x2, y2
    left = (px - ux * size - uy * size * 0.55, py - uy * size + ux * size * 0.55)
    right = (px - ux * size + uy * size * 0.55, py - uy * size - ux * size * 0.55)
    path = c.beginPath()
    path.moveTo(px, py)
    path.lineTo(*left)
    path.lineTo(*right)
    path.close()
    c.drawPath(path, fill=1, stroke=0)


def _step_box(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    index: int,
    label: str,
    *,
    primary: bool = False,
) -> None:
    if primary:
        c.setFillColor(GREEN_DARK)
        c.setStrokeColor(GREEN_DARK)
        c.setLineWidth(0.8)
        c.roundRect(x, y, w, h, 7, fill=1, stroke=1)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(x + w / 2, y + h - 0.2 * inch, f"{index:02d}")
        _draw_para(c, label, x + 0.06 * inch, y + h - 0.3 * inch, w - 0.12 * inch, h - 0.38 * inch, STY_STEP_ON_DARK)
    else:
        _round_card(c, x, y, w, h, radius=7)
        c.setFillColor(GREEN_DARK)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(x + w / 2, y + h - 0.2 * inch, f"{index:02d}")
        _draw_para(c, label, x + 0.06 * inch, y + h - 0.3 * inch, w - 0.12 * inch, h - 0.38 * inch, STY_STEP)


def _node(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    subtitle: str = "",
    *,
    primary: bool = False,
) -> None:
    if primary:
        c.setFillColor(GREEN_DARK)
        c.setStrokeColor(GREEN_DARK)
        c.setLineWidth(0.9)
        c.roundRect(x, y, w, h, 6, fill=1, stroke=1)
        _draw_para(c, title, x + 0.06 * inch, y + h - 0.06 * inch, w - 0.12 * inch, 0.28 * inch, STY_MINI_H_DARK)
        if subtitle:
            _draw_para(
                c,
                subtitle,
                x + 0.06 * inch,
                y + h - 0.32 * inch,
                w - 0.12 * inch,
                h - 0.38 * inch,
                STY_MINI_ON_DARK,
            )
    else:
        _round_card(c, x, y, w, h, radius=6)
        _draw_para(c, title, x + 0.06 * inch, y + h - 0.05 * inch, w - 0.12 * inch, 0.26 * inch, STY_MINI_H)
        if subtitle:
            _draw_para(
                c,
                subtitle,
                x + 0.06 * inch,
                y + h - 0.3 * inch,
                w - 0.12 * inch,
                h - 0.36 * inch,
                STY_MINI,
            )


def _lane(c: canvas.Canvas, x: float, y: float, w: float, h: float) -> None:
    c.setFillColor(WHITE)
    c.setStrokeColor(GREEN_DARK)
    c.setLineWidth(1.1)
    c.roundRect(x, y, w, h, 8, fill=1, stroke=1)


def _draw_h_pipeline(c: canvas.Canvas, steps: list[str], *, y: float, h: float, x0: float, width: float) -> None:
    n = max(1, len(steps))
    gap = 0.18 * inch if n <= 6 else 0.12 * inch
    box_w = (width - gap * (n - 1)) / n
    for i, step in enumerate(steps):
        x = x0 + i * (box_w + gap)
        _step_box(c, x, y, box_w, h, i + 1, step)
        if i < n - 1:
            _arrow(c, x + box_w + 1, y + h / 2, x + box_w + gap - 2, y + h / 2)


def _render_architecture(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    """Full query-time graph: control plane, parallel legs, fuse, generate + ACF."""
    _chrome(c, spec, page_i, page_n)
    top = BODY_TOP - 0.02 * inch
    left = MARGIN
    width = PAGE_W - 2 * MARGIN
    gap = 0.08 * inch

    row1_h = 0.52 * inch
    y1 = top - row1_h
    entries = [("CLI", "run.py"), ("FastAPI", "POST /query"), ("Streamlit", "inspector")]
    ew = 1.55 * inch
    for i, (title, sub) in enumerate(entries):
        _node(c, left + i * (ew + gap), y1, ew, row1_h, title, sub)
    _arrow(c, left + 3 * (ew + gap) - gap, y1 + row1_h / 2, left + 5.05 * inch, y1 + row1_h / 2)
    _node(c, left + 5.15 * inch, y1, 2.35 * inch, row1_h, "run_rag", "LangGraph  ·  chatbot/graph.py", primary=True)
    _node(
        c,
        left + 7.62 * inch,
        y1,
        width - 7.62 * inch,
        row1_h,
        "Short-circuit",
        "meta / product / social / clarify  →  skip retrieve",
    )

    row2_h = 0.72 * inch
    y2 = y1 - gap - 0.14 * inch - row2_h
    c.setFillColor(GREEN_DARK)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(left, y2 + row2_h + 0.03 * inch, "CONTROL PLANE  (pre-retrieval)")
    controls = [
        ("Enrich", "memory + normalize"),
        ("Decompose", "geo · time · entities"),
        ("Ontology", "measure_id"),
        ("Task mode", "fact / trend / clarify"),
        ("Contract", "retrieval_contract"),
        ("Query views", "user always first"),
    ]
    cw = (width - 5 * gap) / 6
    for i, (title, sub) in enumerate(controls):
        _node(c, left + i * (cw + gap), y2, cw, row2_h, title, sub)
        if i < 5:
            _arrow(
                c,
                left + i * (cw + gap) + cw,
                y2 + row2_h / 2,
                left + (i + 1) * (cw + gap),
                y2 + row2_h / 2,
            )

    lane_h = 2.05 * inch
    y3 = y2 - 0.18 * inch - lane_h
    c.setFillColor(GREEN_DARK)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(left, y3 + lane_h + 0.03 * inch, "RETRIEVE_LEGS  (parallel after decompose  ·  peers until merge)")
    lane_w = (width - gap) / 2
    _lane(c, left, y3, lane_w, lane_h)
    _lane(c, left + lane_w + gap, y3, lane_w, lane_h)
    _node(
        c,
        left + 0.1 * inch,
        y3 + lane_h - 0.42 * inch,
        lane_w - 0.2 * inch,
        0.34 * inch,
        "VECTOR LEG  ·  Qdrant",
        "never skipped when warehouse fails",
        primary=True,
    )
    _node(
        c,
        left + lane_w + gap + 0.1 * inch,
        y3 + lane_h - 0.42 * inch,
        lane_w - 0.2 * inch,
        0.34 * inch,
        "BQ LEG  ·  mart_dev",
        "bind-first  ·  NL2SQL fallback",
        primary=True,
    )

    v_items = [
        ("select_corpora", "gate ≤3 skips  ·  6 collections"),
        ("Thread pool", "news · papers · policies · reports · formation · OTA"),
        ("Embed", "query: prefix  ·  E5 384-d  ·  BM25 sparse"),
        ("Hybrid RRF", "filter inside Prefetch  ·  geo / time / doc_kind"),
        ("Cascade", "full → ±1y → drop time → drop geo"),
        ("3 views", "user_query always  ·  merge by point id"),
    ]
    b_items = [
        ("Class supervisor", "1–2 of 15 indicator classes"),
        ("Class engines", "bind_contracts  ·  no SELECT strings"),
        ("compile_sql_from_bind", "fact_lookup / export SQL"),
        ("NL2SQL fallback", "when bind cannot compile"),
        ("Validate + execute", "SELECT-only  ·  allowlist  ·  LIMIT"),
        ("Stamp ACF fields", "year · geo · unit on each row"),
    ]
    inner_top = y3 + lane_h - 0.5 * inch
    inner_h = 0.42 * inch
    cols = 3
    cell_w = (lane_w - 0.28 * inch - 2 * gap) / cols
    for i, (title, sub) in enumerate(v_items):
        r, col = divmod(i, cols)
        _node(
            c,
            left + 0.1 * inch + col * (cell_w + gap),
            inner_top - (r + 1) * (inner_h + 0.06 * inch),
            cell_w,
            inner_h,
            title,
            sub,
        )
    bx = left + lane_w + gap
    for i, (title, sub) in enumerate(b_items):
        r, col = divmod(i, cols)
        _node(
            c,
            bx + 0.1 * inch + col * (cell_w + gap),
            inner_top - (r + 1) * (inner_h + 0.06 * inch),
            cell_w,
            inner_h,
            title,
            sub,
        )

    row4_h = 0.58 * inch
    y4 = y3 - 0.16 * inch - row4_h
    fuse = [
        ("Merge", "first fusion  ·  OFIA source tier"),
        ("Rerank", "cross-encoder  ·  BQ source boost"),
        ("Diversify", "news cannot flood the pack"),
        ("Web fallback", "Wikipedia / Tavily if thin"),
        ("Insufficient", "say so  ·  never fluent-guess"),
    ]
    fw = (width - 4 * gap) / 5
    for i, (title, sub) in enumerate(fuse):
        _node(c, left + i * (fw + gap), y4, fw, row4_h, title, sub, primary=(i == 0))
        if i < 4:
            _arrow(c, left + i * (fw + gap) + fw, y4 + row4_h / 2, left + (i + 1) * (fw + gap), y4 + row4_h / 2)

    row5_h = 0.72 * inch
    y5 = y4 - 0.16 * inch - row5_h
    c.setFillColor(GREEN_DARK)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(left, y5 + row5_h + 0.03 * inch, "GENERATE  (post-retrieval)  ·  ACF is part of the answer, not a footnote")
    gen = [
        ("Generation plan", "shape · evidence priority · persona"),
        ("Pack context", "[Source N]  ·  BQ floor in budget"),
        ("LLM", "grounded prose  ·  no invented tonnes"),
        ("Citations", "cited packed sources only"),
        ("ACF Path B", "0–100 from CITED evidence"),
        ("Export", "DOCX / PDF / table artifacts"),
    ]
    gw = (width - 5 * gap) / 6
    for i, (title, sub) in enumerate(gen):
        _node(c, left + i * (gw + gap), y5, gw, row5_h, title, sub, primary=title.startswith("ACF"))
        if i < 5:
            _arrow(c, left + i * (gw + gap) + gw, y5 + row5_h / 2, left + (i + 1) * (gw + gap), y5 + row5_h / 2)

    _footnote(c, spec.get("footnote"))


def _render_dual_leg(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _render_architecture(c, spec, page_i, page_n)


def _render_hybrid(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    _chrome(c, spec, page_i, page_n)
    top = BODY_TOP - 0.1 * inch
    q_w = 3.2 * inch
    q_x = (PAGE_W - q_w) / 2
    q_h = 0.8 * inch
    _step_box(c, q_x, top - q_h, q_w, q_h, 1, "Query text")

    branch_w = 4.6 * inch
    branch_h = 0.95 * inch
    branch_y = top - q_h - 0.5 * inch - branch_h
    left_x = MARGIN + 0.6 * inch
    right_x = PAGE_W - MARGIN - 0.6 * inch - branch_w
    _step_box(c, left_x, branch_y, branch_w, branch_h, 2, "E5 dense  ·  prefetch ~20")
    _step_box(c, right_x, branch_y, branch_w, branch_h, 3, "BM25 sparse  ·  prefetch ~20")
    _arrow(c, q_x + q_w / 2, top - q_h, left_x + branch_w / 2, branch_y + branch_h)
    _arrow(c, q_x + q_w / 2, top - q_h, right_x + branch_w / 2, branch_y + branch_h)

    fuse_w = 3.4 * inch
    fuse_h = 0.8 * inch
    fuse_x = (PAGE_W - fuse_w) / 2
    fuse_y = branch_y - 0.45 * inch - fuse_h
    _step_box(c, fuse_x, fuse_y, fuse_w, fuse_h, 4, "RRF fuse")
    _arrow(c, left_x + branch_w / 2, branch_y, fuse_x + fuse_w / 2, fuse_y + fuse_h)
    _arrow(c, right_x + branch_w / 2, branch_y, fuse_x + fuse_w / 2, fuse_y + fuse_h)

    trim_w = 3.4 * inch
    trim_y = fuse_y - 0.38 * inch - fuse_h
    _step_box(c, fuse_x, trim_y, trim_w, fuse_h, 5, "Trim to top_k")
    _arrow(c, fuse_x + fuse_w / 2, fuse_y, fuse_x + fuse_w / 2, trim_y + fuse_h)

    caption = str(spec.get("caption") or "")
    if caption:
        _draw_para(c, caption, MARGIN, trim_y - 0.08 * inch, PAGE_W - 2 * MARGIN, 0.7 * inch, STY_CAP)
    _footnote(c, spec.get("footnote"))


def _render_pipeline(c: canvas.Canvas, spec: dict, page_i: int, page_n: int) -> None:
    title = str(spec.get("title") or "")
    if "architecture" in title.lower():
        _render_architecture(c, spec, page_i, page_n)
        return
    if "Hybrid search" in title:
        _render_hybrid(c, spec, page_i, page_n)
        return
    _chrome(c, spec, page_i, page_n)
    steps = [str(s) for s in (spec.get("steps") or [])][:8]
    box_h = 1.15 * inch
    y = BODY_TOP - box_h - 0.1 * inch
    _draw_h_pipeline(c, steps, y=y, h=box_h, x0=MARGIN, width=PAGE_W - 2 * MARGIN)
    caption = str(spec.get("caption") or "")
    if caption:
        cap_bottom = FOOTNOTE_TOP + 0.14 * inch
        cap_h = y - 0.2 * inch - cap_bottom
        _round_card(c, MARGIN, cap_bottom, PAGE_W - 2 * MARGIN, cap_h)
        _draw_para(
            c,
            caption,
            MARGIN + 0.28 * inch,
            cap_bottom + cap_h - 0.22 * inch,
            PAGE_W - 2 * MARGIN - 0.56 * inch,
            cap_h - 0.4 * inch,
            STY_CAP,
        )
    _footnote(c, spec.get("footnote"))


_RENDERERS = {
    "title": _render_title,
    "section": _render_section,
    "bullets": _render_bullets,
    "two_column": _render_two_column,
    "cards": _render_cards,
    "table": _render_table,
    "pipeline": _render_pipeline,
}


def build_pdf(output: Path = OUTPUT_PDF) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output), pagesize=(PAGE_W, PAGE_H))
    c.setTitle("Ask ADZA — OpenTrace community teaching")
    c.setAuthor("OpenTrace")
    page_n = len(SLIDES)
    for i, spec in enumerate(SLIDES, start=1):
        kind = str(spec.get("kind") or "")
        renderer = _RENDERERS.get(kind)
        if renderer is None:
            raise SystemExit(f"Unknown slide kind: {kind!r}")
        renderer(c, spec, i, page_n)
        c.showPage()
    c.save()
    return output


def main() -> None:
    path = build_pdf(OUTPUT_PDF)
    print(f"Wrote {path}  ({len(SLIDES)} pages, v{DOC_VERSION})")


if __name__ == "__main__":
    main()
