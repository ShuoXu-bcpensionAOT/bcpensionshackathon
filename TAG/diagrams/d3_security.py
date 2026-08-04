"""Diagram 3 — Security, access & governance: defence in depth (BCPC TAG deck)."""
from style import *

W, H = 1340, 620
p = []

# ------------------------------------------------------- 1. IDENTITY GATEWAY
IX, IY, IW, IH = 24, 62, 250, 400
p.append(band(IX, IY, IW, IH, "Identity  ·  the front door", "#F7F4FA", "#CBBEDA",
              lab_fill="#5B4B7A"))
icx = IX + IW / 2
p.append(tile(az("groups"), icx, IY + 52, "Entra ID security groups",
              "access is never granted to individuals", isize=32, tsize=11.5, ssize=9.5))
p.append(tile(az("conditional access"), icx, IY + 168, "Conditional Access",
              "MFA · compliant device · location", isize=32, tsize=11.5, ssize=9.5))
p.append(tile(az("entra privleged identity management"), icx, IY + 284,
              "PIM — just-in-time admin", "no standing privileged access",
              isize=32, tsize=11.5, ssize=9.5))

# ------------------------------------------------- 2. FIVE NARROWING LAYERS
LX, LY, LW = 310, 62, 690
p.append(T(LX, LY + 14, "DEFENCE IN DEPTH  —  EACH LAYER NARROWER THAN THE LAST", 11.5, "700",
           TEAL, "start", "1.2"))
layers = [
    ("Tenant guardrails", "publish-to-web off · external sharing off · SPN scope · mandatory labels", az("policy")),
    ("Capacity",          "PROD on dedicated capacity · assignment restricted", az("virtual machine")),
    ("Workspace roles",   "Admin / Member / Contributor / Viewer — assigned to groups", fab("group workspace")),
    ("Item permissions",  "per lakehouse · semantic model · report", fab("lakehouse")),
    ("Row & column security", "OneLake RLS / CLS + dynamic masking, applied from config", az("information protection")),
]
ly = LY + 32
LH, LSTEP = 64, 74
last_mid = last_right = 0
for i, (lab, sub, ic) in enumerate(layers):
    w = LW - i * 68
    x = LX + i * 34
    shade = ["#E4EFF2", "#D4E6EA", "#C2DBE1", "#AFD0D8", "#9AC4CE"][i]
    p.append(box(x, ly, w, LH, shade, TEAL_MID, rx=9, sw=1.3))
    p.append(inline(ic, x + 16, ly + 17, 30))
    p.append(T(x + 58, ly + 29, lab, 12.5, "700", TEAL_DK, "start"))
    p.append(T(x + 58, ly + 48, sub, 10, "400", GREY, "start"))
    last_mid, last_right = ly + LH / 2, x + w
    ly += LSTEP

# identity feeds the stack
p.append(arrow(IX + IW + 4, IY + 200, LX - 6, IY + 200, "#5B4B7A", sw=2.2))

# the funnel's narrow end feeds the net effect
p.append(arrow(last_right + 8, last_mid, 1034, last_mid, GREEN, sw=2.0, dash="6 4"))

# ------------------------------------------------------------- 3. NET RESULT
RX, RY, RW, RH = 1040, 62, 276, 400
p.append(band(RX, RY, RW, RH, "Net effect", "#EEF6F1", "#B6D6C4", lab_fill=GREEN))
rcx = RX + RW / 2
p.append(tile(az("users"), rcx, RY + 48, "A business consumer", "", isize=30, tsize=12))
res = ["passes tenant policy + MFA",
       "sits on the assigned capacity",
       "gets Viewer on one workspace",
       "sees only the published report",
       "and only the rows & columns",
       "their policy permits"]
ry = RY + 132
for r in res:
    p.append(f'<circle cx="{RX+26}" cy="{ry-4}" r="3.5" fill="{GREEN}"/>')
    p.append(T(RX + 40, ry, r, 11, "400", GREY, "start"))
    ry += 26
p.append(box(RX + 16, RY + 300, RW - 32, 78, WHITE, "#B6D6C4", rx=8, sw=1.2))
p.append(T(rcx, RY + 324, "EVERY ACTION AUDITED", 10, "700", GREEN, "middle", "1.1"))
p.append(T(rcx, RY + 346, "access grants · data reads", 10.5, "400", GREY))
p.append(T(rcx, RY + 364, "pipeline runs · admin changes", 10.5, "400", GREY))

# ------------------------------------------------------------ 4. GOVERNANCE
p.append(band(24, 496, 1292, 100, "Governance  ·  who owns the data", "#F2F5F7", LINE))
gov = [
    (fab("group workspace"),  "Domains", "Pensions · Employer · Member"),
    (az("users"),             "Domain owners", "accountable for their data"),
    (fab("database checkmark"), "Data stewards", "meaning · quality · access"),
    (fab("database ribbon"),  "Certified products", "endorsed Gold datasets"),
    (fab("database pulse"),   "Data quality", "declarative rules + quarantine"),
    (fab("branch fork signal"), "Lineage", "source → bronze → silver → gold"),
]
step = 1292 / len(gov)
for i, (ic, lab, sub) in enumerate(gov):
    gx = 24 + step * (i + 0.5)
    p.append(inline(ic, gx - 13, 528, 26))
    p.append(T(gx, 572, lab, 11, "700", GREY))
    p.append(T(gx, 587, sub, 9.5, "400", GREY_MID))

save(r"C:\Users\Shuo\AppData\Local\Temp\claude\C--Users-Shuo-OneDrive----bcpensionshackathon\16a4ed22-96b3-43f5-9649-e3ce5286ee1e\scratchpad\deck\out\d3_security.svg", W, H, p)
print("ok")
