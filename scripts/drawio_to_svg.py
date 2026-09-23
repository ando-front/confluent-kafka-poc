#!/usr/bin/env python3
"""docs/diagrams/*.drawio を SVG に書き出す（draw.io アプリ不要）。

このリポジトリの図で使っている mxGraph スタイルのサブセットだけを解釈する
軽量レンダラー。.drawio が正本、.svg は生成物という関係を保つためのもの。

    python3 scripts/drawio_to_svg.py            # docs/diagrams/*.drawio を全変換
    python3 scripts/drawio_to_svg.py a.drawio   # 個別変換

対応している style:
  図形   rounded / ellipse / rhombus / swimlane / text（既定は矩形）
  装飾   fillColor strokeColor fontColor fontSize fontStyle align verticalAlign
         dashed dashPattern strokeWidth opacity arcSize startSize
  線     exitX/exitY entryX/entryY edgeStyle=orthogonalEdgeStyle
         endArrow/startArrow(block|open|none) Array as="points" edgeLabel
"""
from __future__ import annotations

import html
import math
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

FONT = "'Helvetica Neue',Arial,'Hiragino Sans','Noto Sans JP',sans-serif"
LINE_H = 1.35
PAD = 6.0


# --------------------------------------------------------------------------- 解析
def parse_style(raw: str) -> dict:
    style = {}
    for part in (raw or "").split(";"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            k, _, v = part.partition("=")
            style[k.strip()] = v.strip()
        else:
            style[part] = "1"
    return style


def num(style: dict, key: str, default: float) -> float:
    try:
        return float(style[key])
    except (KeyError, TypeError, ValueError):
        return default


def label_lines(value: str | None) -> list[str]:
    if not value:
        return []
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    text = re.sub(r"</?(div|p)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    lines = [ln.strip() for ln in text.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return lines


class Cell:
    def __init__(self, el: ET.Element):
        self.id = el.get("id")
        self.value = el.get("value")
        self.style = parse_style(el.get("style"))
        self.is_vertex = el.get("vertex") == "1"
        self.is_edge = el.get("edge") == "1"
        self.parent = el.get("parent")
        self.source = el.get("source")
        self.target = el.get("target")
        self.x = self.y = 0.0
        self.w = self.h = 0.0
        self.points: list[tuple[float, float]] = []
        self.src_point = self.tgt_point = None
        self.offset = (0.0, 0.0)
        geo = el.find("mxGeometry")
        if geo is not None:
            self.x = float(geo.get("x") or 0)
            self.y = float(geo.get("y") or 0)
            self.w = float(geo.get("width") or 0)
            self.h = float(geo.get("height") or 0)
            for arr in geo.findall("Array"):
                if arr.get("as") == "points":
                    self.points = [
                        (float(p.get("x") or 0), float(p.get("y") or 0))
                        for p in arr.findall("mxPoint")
                    ]
            for p in geo.findall("mxPoint"):
                role = p.get("as")
                pt = (float(p.get("x") or 0), float(p.get("y") or 0))
                if role == "sourcePoint":
                    self.src_point = pt
                elif role == "targetPoint":
                    self.tgt_point = pt
                elif role == "offset":
                    self.offset = pt


def load(path: pathlib.Path) -> tuple[dict[str, Cell], list[Cell], float, float]:
    root = ET.parse(path).getroot()
    model = root.find(".//mxGraphModel")
    page_w = float(model.get("pageWidth") or 1100)
    page_h = float(model.get("pageHeight") or 800)
    cells: dict[str, Cell] = {}
    order: list[Cell] = []
    for el in model.find("root"):
        if el.tag != "mxCell":
            continue
        cell = Cell(el)
        cells[cell.id] = cell
        if cell.is_vertex or cell.is_edge:
            order.append(cell)
    return cells, order, page_w, page_h


def absolute(cell: Cell, cells: dict[str, Cell]) -> tuple[float, float]:
    x, y = cell.x, cell.y
    parent = cells.get(cell.parent)
    while parent is not None and parent.is_vertex:
        x += parent.x
        y += parent.y
        parent = cells.get(parent.parent)
    return x, y


# --------------------------------------------------------------------------- 描画
def esc(text: str) -> str:
    return html.escape(text, quote=False)


def draw_text(lines, cx, cy, box_h, style, out, left=None, right=None):
    if not lines:
        return
    size = num(style, "fontSize", 12)
    color = style.get("fontColor", "#13212A")
    weight = "bold" if int(num(style, "fontStyle", 0)) & 1 else "normal"
    italic = "italic" if int(num(style, "fontStyle", 0)) & 2 else "normal"
    align = style.get("align", "center")
    valign = style.get("verticalAlign", "middle")

    anchor = {"center": "middle", "left": "start", "right": "end"}.get(align, "middle")
    if align == "left":
        tx = left + PAD
    elif align == "right":
        tx = right - PAD
    else:
        tx = cx

    step = size * LINE_H
    total = step * len(lines)
    if valign == "top":
        first = cy - box_h / 2 + PAD + size * 0.85
    elif valign == "bottom":
        first = cy + box_h / 2 - PAD - total + size * 0.85
    else:
        first = cy - total / 2 + size * 0.85
    for i, line in enumerate(lines):
        if not line:
            continue
        out.append(
            f'<text x="{tx:.1f}" y="{first + i * step:.1f}" text-anchor="{anchor}" '
            f'font-family="{FONT}" font-size="{size:g}" fill="{color}" '
            f'font-weight="{weight}" font-style="{italic}">{esc(line)}</text>'
        )


def shape_attrs(style: dict) -> str:
    fill = style.get("fillColor", "#FFFFFF")
    fill = "none" if fill == "none" else fill
    stroke = style.get("strokeColor", "#9FB3B7")
    stroke = "none" if stroke == "none" else stroke
    sw = num(style, "strokeWidth", 1)
    attrs = f'fill="{fill}" stroke="{stroke}" stroke-width="{sw:g}"'
    if "opacity" in style:
        attrs += f' fill-opacity="{num(style, "opacity", 100) / 100:.2f}"'
    if style.get("dashed") == "1":
        pattern = style.get("dashPattern", "6 4").replace(",", " ")
        attrs += f' stroke-dasharray="{pattern}"'
    return attrs


def render_vertex(cell: Cell, x: float, y: float, out: list[str]) -> None:
    s = cell.style
    w, h = cell.w, cell.h
    cx, cy = x + w / 2, y + h / 2
    lines = label_lines(cell.value)

    if "text" in s and "fillColor" not in s:
        draw_text(lines, cx, cy, h, s, out, left=x, right=x + w)
        return

    if "swimlane" in s:
        head = num(s, "startSize", 26)
        body_fill = s.get("swimlaneFillColor", "#FFFFFF")
        stroke = s.get("strokeColor", "#9FB3B7")
        sw = num(s, "strokeWidth", 1)
        rx = 4
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
            f'fill="{body_fill}" stroke="{stroke}" stroke-width="{sw:g}"/>'
        )
        out.append(
            f'<path d="M{x:.1f} {y + rx:.1f} a{rx} {rx} 0 0 1 {rx} {-rx} '
            f'h{w - 2 * rx:.1f} a{rx} {rx} 0 0 1 {rx} {rx} v{head - rx:.1f} h{-w:.1f} z" '
            f'fill="{s.get("fillColor", "#EEF3F4")}" stroke="{stroke}" stroke-width="{sw:g}"/>'
        )
        hs = dict(s)
        hs.setdefault("align", "left")
        hs["verticalAlign"] = "middle"
        draw_text(lines, cx, y + head / 2, head, hs, out, left=x, right=x + w)
        return

    if "ellipse" in s:
        out.append(
            f'<ellipse cx="{cx:.1f}" cy="{cy:.1f}" rx="{w / 2:.1f}" ry="{h / 2:.1f}" {shape_attrs(s)}/>'
        )
    elif "rhombus" in s:
        pts = f"{cx:.1f},{y:.1f} {x + w:.1f},{cy:.1f} {cx:.1f},{y + h:.1f} {x:.1f},{cy:.1f}"
        out.append(f'<polygon points="{pts}" {shape_attrs(s)}/>')
    else:
        rx = num(s, "arcSize", 8) if s.get("rounded") == "1" else 0
        rx = min(rx, w / 2, h / 2)
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
            f'rx="{rx:.1f}" {shape_attrs(s)}/>'
        )
    draw_text(lines, cx, cy, h, s, out, left=x, right=x + w)


def side_dir(s: dict, prefix: str) -> str | None:
    fx, fy = s.get(prefix + "X"), s.get(prefix + "Y")
    if fx is None or fy is None:
        return None
    fx, fy = float(fx), float(fy)
    if fy in (0.0, 1.0) and fx not in (0.0, 1.0):
        return "v"
    return "h"


def clip_to_box(cx, cy, w, h, tx, ty):
    """箱の中心 (cx,cy) から (tx,ty) 方向へ伸ばしたとき、枠と交わる点。"""
    dx, dy = tx - cx, ty - cy
    if dx == 0 and dy == 0:
        return cx, cy
    scale = float("inf")
    if dx:
        scale = min(scale, (w / 2) / abs(dx))
    if dy:
        scale = min(scale, (h / 2) / abs(dy))
    return cx + dx * scale, cy + dy * scale


def orthogonalize(points, start_dir, end_dir):
    """折れ線の各区間に、直角の曲がり角を補う。"""
    if len(points) == 2:
        (ax, ay), (bx, by) = points
        if abs(ax - bx) < 0.5 or abs(ay - by) < 0.5:
            return [(ax, ay), (bx, by)]
        if start_dir == "h" and end_dir == "h":
            mid = (ax + bx) / 2
            return [(ax, ay), (mid, ay), (mid, by), (bx, by)]
        if start_dir == "v" and end_dir == "v":
            mid = (ay + by) / 2
            return [(ax, ay), (ax, mid), (bx, mid), (bx, by)]
        if start_dir == "h":
            return [(ax, ay), (bx, ay), (bx, by)]
        return [(ax, ay), (ax, by), (bx, by)]

    out = [points[0]]
    n = len(points)
    for i in range(n - 1):
        ax, ay = out[-1]
        bx, by = points[i + 1]
        if abs(ax - bx) < 0.5 or abs(ay - by) < 0.5:
            out.append((bx, by))
            continue
        if i == 0:
            horizontal_first = start_dir == "h"
        elif i == n - 2:
            horizontal_first = end_dir == "v"
        else:
            horizontal_first = True
        out.append((bx, ay) if horizontal_first else (ax, by))
        out.append((bx, by))
    return out


def arrow_head(px, py, qx, qy, color, size=9.0, filled=True):
    ang = math.atan2(qy - py, qx - px)
    half = size * 0.42
    x1 = qx - size * math.cos(ang) + half * math.sin(ang)
    y1 = qy - size * math.sin(ang) - half * math.cos(ang)
    x2 = qx - size * math.cos(ang) - half * math.sin(ang)
    y2 = qy - size * math.sin(ang) + half * math.cos(ang)
    if filled:
        return (
            f'<polygon points="{qx:.1f},{qy:.1f} {x1:.1f},{y1:.1f} {x2:.1f},{y2:.1f}" '
            f'fill="{color}" stroke="none"/>'
        )
    return (
        f'<path d="M{x1:.1f} {y1:.1f} L{qx:.1f} {qy:.1f} L{x2:.1f} {y2:.1f}" '
        f'fill="none" stroke="{color}" stroke-width="1.4"/>'
    )


def polyline_midpoint(pts):
    seg = [math.dist(pts[i], pts[i + 1]) for i in range(len(pts) - 1)]
    total = sum(seg) or 1.0
    want, run = total / 2, 0.0
    for i, length in enumerate(seg):
        if run + length >= want:
            t = (want - run) / (length or 1.0)
            (ax, ay), (bx, by) = pts[i], pts[i + 1]
            return ax + (bx - ax) * t, ay + (by - ay) * t
        run += length
    return pts[-1]


def render_edge(cell: Cell, cells: dict, out: list[str]) -> list[tuple[float, float]]:
    s = cell.style
    color = s.get("strokeColor", "#5C7178")
    sw = num(s, "strokeWidth", 1.4)

    src = cells.get(cell.source or "")
    tgt = cells.get(cell.target or "")
    src_box = tgt_box = None
    if src is not None:
        sx, sy = absolute(src, cells)
        src_box = (sx + src.w / 2, sy + src.h / 2, src.w, src.h)
    if tgt is not None:
        tx, ty = absolute(tgt, cells)
        tgt_box = (tx + tgt.w / 2, ty + tgt.h / 2, tgt.w, tgt.h)

    start = cell.src_point
    if start is None and src_box and "exitX" in s:
        sx, sy = absolute(src, cells)
        start = (sx + float(s["exitX"]) * src.w, sy + float(s["exitY"]) * src.h)
    end = cell.tgt_point
    if end is None and tgt_box and "entryX" in s:
        tx, ty = absolute(tgt, cells)
        end = (tx + float(s["entryX"]) * tgt.w, ty + float(s["entryY"]) * tgt.h)

    if start is None:
        ref = cell.points[0] if cell.points else (end or (0, 0))
        start = clip_to_box(*src_box, *ref) if src_box else (0, 0)
    if end is None:
        ref = cell.points[-1] if cell.points else start
        end = clip_to_box(*tgt_box, *ref) if tgt_box else start

    pts = [start, *cell.points, end]
    if s.get("edgeStyle") == "orthogonalEdgeStyle":
        pts = orthogonalize(pts, side_dir(s, "exit") or "h", side_dir(s, "entry") or "h")

    dash = ""
    if s.get("dashed") == "1":
        dash = f' stroke-dasharray="{s.get("dashPattern", "6 4").replace(",", " ")}"'
    d = "M" + " L".join(f"{x:.1f} {y:.1f}" for x, y in pts)
    join = 'stroke-linejoin="round" stroke-linecap="round"'
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw:g}" {join}{dash}/>')

    end_arrow = s.get("endArrow", "block")
    if end_arrow != "none":
        out.append(arrow_head(*pts[-2], *pts[-1], color, filled=end_arrow != "open"))
    start_arrow = s.get("startArrow", "none")
    if start_arrow != "none":
        out.append(arrow_head(*pts[1], *pts[0], color, filled=start_arrow != "open"))
    return pts


def render(path: pathlib.Path) -> str:
    cells, order, page_w, page_h = load(path)
    body: list[str] = []
    edge_paths: dict[str, list] = {}

    for cell in order:
        if cell.is_edge:
            edge_paths[cell.id] = render_edge(cell, cells, body)
        elif cells.get(cell.parent) is not None and cells[cell.parent].is_edge:
            continue  # edgeLabel は後段でまとめて描く
        else:
            x, y = absolute(cell, cells)
            render_vertex(cell, x, y, body)

    for cell in order:
        parent = cells.get(cell.parent or "")
        if not cell.is_vertex or parent is None or not parent.is_edge:
            continue
        pts = edge_paths.get(parent.id)
        if not pts:
            continue
        mx, my = polyline_midpoint(pts)
        mx += cell.offset[0]
        my += cell.offset[1]
        lines = label_lines(cell.value)
        size = num(cell.style, "fontSize", 11)
        bg = cell.style.get("labelBackgroundColor", "#FFFFFF")
        if bg != "none" and lines:
            width = max(len(ln) for ln in lines) * size * 0.62 + 8
            height = size * LINE_H * len(lines) + 4
            body.append(
                f'<rect x="{mx - width / 2:.1f}" y="{my - height / 2:.1f}" width="{width:.1f}" '
                f'height="{height:.1f}" rx="2" fill="{bg}" stroke="none"/>'
            )
        draw_text(lines, mx, my, 0, cell.style, body, left=mx, right=mx)

    inner = "\n".join("  " + line for line in body)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{page_w:g}" height="{page_h:g}" '
        f'viewBox="0 0 {page_w:g} {page_h:g}" role="img">\n'
        f'  <rect width="{page_w:g}" height="{page_h:g}" fill="#FFFFFF"/>\n'
        f"{inner}\n</svg>\n"
    )


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    targets = [pathlib.Path(a) for a in argv[1:]] or sorted((root / "docs/diagrams").glob("*.drawio"))
    if not targets:
        print("変換対象の .drawio が見つかりません", file=sys.stderr)
        return 1
    for src in targets:
        dst = src.with_suffix(".svg")
        dst.write_text(render(src), encoding="utf-8")
        print(f"{src.name} -> {dst.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
