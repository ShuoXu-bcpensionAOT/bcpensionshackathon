"""Shared BCPC brand style for the TAG deck diagrams (official MS icon library)."""
import sys, os
sys.path.insert(0, r"C:\Users\Shuo\.claude\assets\diagram-icons")
from svg_icons import find_icon, inline  # noqa: F401

# BCPC brand palette (sampled from the TAG template deck)
TEAL      = "#00778A"   # primary
TEAL_MID  = "#1B8196"
TEAL_DK   = "#28525B"
GREY      = "#4D4D4F"   # body text
GREY_MID  = "#7A7C80"
LINE      = "#9AA3AC"
WHITE     = "#FFFFFF"
BG        = "#FFFFFF"
TEAL_TINT = "#E8F2F4"   # fabric container fill
TINT2     = "#F2F5F7"   # neutral container fill
AMBER     = "#B26B00"
GREEN     = "#0E7C5F"

FONT = ('font-family="Arial, Helvetica, sans-serif"')


def T(x, y, s, size=13, w="400", fill=GREY, anchor="middle", ls="0"):
    """Text. `s` may contain \n for multi-line (rendered as tspans)."""
    lines = str(s).split("\n")
    if len(lines) == 1:
        return (f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{w}" fill="{fill}" '
                f'text-anchor="{anchor}" letter-spacing="{ls}">{esc(lines[0])}</text>')
    out = [f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{w}" fill="{fill}" '
           f'text-anchor="{anchor}" letter-spacing="{ls}">']
    for i, ln in enumerate(lines):
        dy = 0 if i == 0 else size + 3
        out.append(f'<tspan x="{x}" dy="{dy}">{esc(ln)}</tspan>')
    out.append('</text>')
    return "".join(out)


def esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def box(x, y, w, h, fill=WHITE, stroke=LINE, rx=10, sw=1.4, dash=""):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
            f'stroke="{stroke}" stroke-width="{sw}"{d}/>')


def band(x, y, w, h, label, fill=TINT2, stroke=LINE, rx=10, lab_fill=TEAL_DK, dash="", ls="1.2"):
    """Container box with a small caps label in its top-left."""
    return (box(x, y, w, h, fill, stroke, rx, dash=dash) +
            T(x + 16, y + 22, label.upper(), 11, "700", lab_fill, "start", ls))


def arrow(x1, y1, x2, y2, color=TEAL, label="", lx=None, ly=None, dash="", sw=2.0, fs=11,
          marker="ah", lfill=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    out = (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}" '
           f'marker-end="url(#{marker})"{d}/>')
    if label:
        mx = lx if lx is not None else (x1 + x2) / 2
        my = ly if ly is not None else (y1 + y2) / 2 - 8
        out += T(mx, my, label, fs, "600", lfill or color)
    return out


def tile(icon, cx, top, label, sub="", isize=40, lw="700", tsize=12, ssize=10.5, fill=GREY):
    """Centred icon with a label (and optional sub-label) underneath."""
    out = inline(icon, cx - isize / 2, top, isize)
    out += T(cx, top + isize + 16, label, tsize, lw, fill)
    if sub:
        nlab = len(str(label).split("\n"))
        out += T(cx, top + isize + 16 + nlab * (tsize + 3) + 1, sub, ssize, "400", GREY_MID)
    return out


DEFS = ('<defs>'
        '<marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker>'
        '<marker id="ahg" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{LINE}"/></marker>'
        '</defs>')


def save(path, W, H, parts, bg=BG):
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
           f'viewBox="0 0 {W} {H}" {FONT}>'
           f'<rect width="{W}" height="{H}" fill="{bg}"/>' + DEFS + "".join(parts) + '</svg>')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w", encoding="utf-8").write(svg)
    return path


# icon shorthands
def fab(q):
    return find_icon("fabric", q)


def az(q):
    return find_icon("azure", q)
