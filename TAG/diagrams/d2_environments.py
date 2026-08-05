"""Diagram 2 — Workspace, environment & deployment model (BCPC TAG deck)."""
from style import *

W, H = 1340, 620
p = []

# ------------------------------------------------------------------ git / CI
GX, GY, GW, GH = 24, 150, 158, 250
p.append(band(GX, GY, GW, GH, "Source of truth", "#EEF6F1", "#B6D6C4", lab_fill=GREEN))
gcx = GX + GW / 2
p.append(tile(fab("branch fork link"), gcx, GY + 52, "Git repo", "config-as-code", isize=32,
              tsize=12, ssize=10))
p.append(tile(az("app registrations"), gcx, GY + 150, "Deployment\nSPN", "the only identity\nthat changes PROD",
              isize=30, tsize=11.5, ssize=9.5))

# ------------------------------------------------------------- environments
envs = [
    ("DEV",  "build freely",           "masked / synthetic data",  210, "#F2F7F8", "#9FC4CC"),
    ("UAT",  "validate & sign-off",    "masked / representative",  590, "#EEF4F6", "#8FBAC4"),
    ("PROD", "promotion only",         "real data · PII governed",  970, "#E4EFF2", TEAL),
]
EY, EW, EH = 62, 340, 384
for name, mode, data, ex, fill, stroke in envs:
    isprod = name == "PROD"
    p.append(box(ex, EY, EW, EH, fill, stroke, rx=12, sw=2.2 if isprod else 1.6))
    p.append(T(ex + 18, EY + 28, name, 15, "700", TEAL if isprod else TEAL_DK, "start", "1.2"))
    p.append(T(ex + EW - 18, EY + 28, mode, 11, "600", GREY_MID, "end"))

    # workspaces
    p.append(band(ex + 16, EY + 44, EW - 32, 132, "Workspaces", WHITE, "#C8D5D9"))
    for i, (wlab, wsub) in enumerate([("Data platform", "lakehouses · pipelines"),
                                      ("Reporting", "semantic models · reports")]):
        wx = ex + 16 + (EW - 32) * (0.27 + 0.46 * i)
        p.append(inline(fab("group workspace"), wx - 16, EY + 80, 32))
        p.append(T(wx, EY + 128, wlab, 11, "700", GREY))
        p.append(T(wx, EY + 143, wsub, 9.5, "400", GREY_MID))

    # per-environment isolation
    p.append(band(ex + 16, EY + 188, EW - 32, 108, "Isolated per environment", WHITE, "#C8D5D9"))
    iso = [(az("groups"), "Entra\ngroups"), (az("app registrations"), "Service\nprincipals"),
           (az("key vault"), "Key Vault"), (az("virtual networks"), "Source\nconnection")]
    for i, (ic, lab) in enumerate(iso):
        ix = ex + 16 + (EW - 32) * (0.13 + 0.245 * i)
        p.append(inline(ic, ix - 12, EY + 220, 24))
        p.append(T(ix, EY + 262, lab, 9.5, "600", GREY))

    # data posture
    dfill = "#FDECEC" if isprod else "#F4F7F4"
    dstroke = "#E3B4B4" if isprod else "#CBD8CB"
    p.append(box(ex + 16, EY + 306, EW - 32, 56, dfill, dstroke, rx=8, sw=1.2))
    p.append(T(ex + EW / 2, EY + 328, "DATA", 9.5, "700", GREY_MID, "middle", "1.2"))
    p.append(T(ex + EW / 2, EY + 348, data, 11, "600", "#A03A3A" if isprod else GREY))

# git -> DEV
p.append(arrow(GX + GW + 4, EY + 110, 204, EY + 110, GREEN, sw=2.0))

# promotion arrows — label sits on a white pill so it never collides with a box border
for x1, x2 in [(210 + EW, 590), (590 + EW, 970)]:
    p.append(arrow(x1 + 6, EY + 110, x2 - 6, EY + 110, TEAL, sw=2.4))
    mid = (x1 + x2) / 2
    p.append(f'<rect x="{mid-34}" y="{EY+76}" width="68" height="20" rx="10" fill="{WHITE}" '
             f'stroke="{TEAL}" stroke-width="1"/>')
    p.append(T(mid, EY + 90, "promote", 10.5, "700", TEAL))

# ------------------------------------------------------- promotion statement
p.append(box(210, 470, 1100, 46, "#F2F7F8", "#9FC4CC", rx=9, sw=1.4))
p.append(T(760, 499, "Only code + configuration promote  —  never data, never credentials.  "
                     "Executed by the Deployment SPN through the pipeline.",
           12.5, "600", TEAL_DK))

# no-downward-flow marker
p.append(f'<line x1="210" y1="540" x2="1310" y2="540" stroke="#C0504D" stroke-width="1.6" '
         f'stroke-dasharray="7 5"/>')
p.append(f'<rect x="545" y="527" width="430" height="26" rx="6" fill="{WHITE}" '
         f'stroke="#C0504D" stroke-width="1.2"/>')
p.append(T(760, 545, "Production data never flows back to DEV / UAT", 11.5, "700", "#A03A3A",
           extra=' data-layout-ok="LINE_TEXT"'))

# ------------------------------------------------------------------ capacity
p.append(box(24, 566, 1292, 40, "#F7F4FA", "#CBBEDA", rx=8, sw=1.4))
p.append(T(670, 591, "Fabric capacity  ·  Canadian region  ·  PROD on dedicated "
                     "capacity; DEV / UAT may share  ·  capacity assignment restricted to platform admins",
           11.5, "600", "#5B4B7A"))

save(r"C:\Users\Shuo\AppData\Local\Temp\claude\C--Users-Shuo-OneDrive----bcpensionshackathon\16a4ed22-96b3-43f5-9649-e3ce5286ee1e\scratchpad\deck\out\d2_environments.svg", W, H, p)
print("ok")
