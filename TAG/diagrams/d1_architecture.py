"""Diagram 1 — High-level Fabric logical architecture (BCPC TAG deck)."""
from style import *

W, H = 1340, 620
p = []

# ---------------------------------------------------------------- 1. SOURCES
SX, SY, SW, SH = 24, 62, 196, 400
p.append(band(SX, SY, SW, SH, "Data sources"))
cx = SX + SW / 2
srcs = [
    (az("managed database"), "Oracle (OCI)",     "member & pension"),
    (az("sql database"),    "SQL Server",        "line-of-business"),
    (fab("cloud endpoint"), "APIs / SaaS",       "HTTP · Entra"),
    (fab("document one page multiple text"),  "Files",             "csv · xlsx drop"),
]
ty = SY + 56
for ic, lab, sub in srcs:
    p.append(tile(ic, cx, ty, lab, sub, isize=34, tsize=11.5, ssize=9.5))
    ty += 92

# ------------------------------------------------------------ 2. CONNECTIVITY
GX, GY, GW, GH = 240, 62, 176, 400
p.append(band(GX, GY, GW, GH, "Private connectivity", fill="#F6F1E9", stroke="#D9C7A8",
              lab_fill=AMBER))
gcx = GX + GW / 2
p.append(tile(az("virtual networks"), gcx, GY + 62, "VNet data\ngateway", "Azure / PaaS sources",
              isize=34, tsize=11.5, ssize=9.5))
p.append(tile(az("on-premises data gateway"), gcx, GY + 190, "On-premises\ndata gateway",
              "Oracle mirroring · HA", isize=34, tsize=11.5, ssize=9.5))
p.append(tile(az("express route"), gcx, GY + 318, "ExpressRoute /\nPrivate Link", "no public path",
              isize=30, tsize=11, ssize=9.5))

# arrows sources -> gateways
for i in range(4):
    y = SY + 48 + i * 92 + 17
    p.append(arrow(SX + SW + 4, y, GX - 6, y, LINE, sw=1.6, marker="ahg"))

# ----------------------------------------------------------------- 3. FABRIC
FX, FY, FW, FH = 436, 40, 720, 444
p.append(box(FX, FY, FW, FH, TEAL_TINT, TEAL, rx=14, sw=2.0))
p.append(T(FX + 20, FY + 26, "MICROSOFT FABRIC", 12.5, "700", TEAL, "start", "1.4"))
p.append(T(FX + FW - 20, FY + 26, "Fabric capacity · Canadian region", 11, "600", TEAL_DK, "end"))

# 3a. ingest & orchestrate
p.append(band(FX + 20, FY + 44, FW - 40, 104, "Ingest & orchestrate", WHITE, "#BBD5DB"))
oy = FY + 80
for i, (ic, lab) in enumerate([
        (fab("pipeline"), "Data pipelines"),
        (fab("notebook"), "Spark notebooks"),
        (fab("dataflow gen2"), "Mirroring"),
        (az("sql database"), "config_db")]):
    ox = FX + 118 + i * 170
    p.append(inline(ic, ox - 15, oy, 30))
    p.append(T(ox, oy + 46, lab, 11, "600", GREY))

# 3b. OneLake medallion
p.append(band(FX + 20, FY + 162, FW - 40, 150, "OneLake  ·  medallion", WHITE, "#BBD5DB"))
my = FY + 208
med = [("Bronze", "raw · append-only", FX + 130),
       ("Silver", "cleansed · DQ", FX + 360),
       ("Gold",   "star schema", FX + 590)]
for lab, sub, mx in med:
    p.append(inline(fab("lakehouse"), mx - 19, my, 38))
    p.append(T(mx, my + 54, lab, 12.5, "700", TEAL_DK))
    p.append(T(mx, my + 70, sub, 10, "400", GREY_MID))
p.append(arrow(FX + 165, my + 20, FX + 322, my + 20, TEAL, sw=1.8))
p.append(arrow(FX + 395, my + 20, FX + 552, my + 20, TEAL, sw=1.8))

# 3c. serve
p.append(band(FX + 20, FY + 326, FW - 40, 100, "Serve", WHITE, "#BBD5DB"))
sy2 = FY + 362
p.append(inline(fab("semantic model"), FX + 150 - 15, sy2, 30))
p.append(T(FX + 150, sy2 + 46, "Semantic model", 11, "600", GREY))
p.append(inline(fab("report"), FX + 390 - 15, sy2, 30))
p.append(T(FX + 390, sy2 + 46, "Power BI reports", 11, "600", GREY))
p.append(inline(fab("database sql"), FX + 620 - 15, sy2, 30))
p.append(T(FX + 620, sy2 + 46, "SQL endpoint", 11, "600", GREY))
# Gold feeds the serve layer: straight down to the SQL endpoint, and across to the semantic model
p.append(arrow(FX + 590, my + 88, FX + 590, FY + 354, TEAL, sw=1.8))
p.append(f'<polyline points="{FX+590},{my+88} {FX+590},{FY+336} {FX+150},{FY+336} '
         f'{FX+150},{FY+354}" fill="none" stroke="{TEAL}" stroke-width="1.8" '
         f'marker-end="url(#ah)"/>')
# semantic model -> reports
p.append(arrow(FX + 205, sy2 + 15, FX + 335, sy2 + 15, TEAL, sw=1.8))

# gateway -> fabric
p.append(arrow(GX + GW + 4, FY + 96, FX - 6, FY + 96, TEAL, "ingest", sw=2.2))

# --------------------------------------------------------------- 4. CONSUMERS
CX, CY, CW, CH = 1176, 62, 140, 400
p.append(band(CX, CY, CW, CH, "Consumers"))
ccx = CX + CW / 2
p.append(tile(az("users"), ccx, CY + 62, "Business\nusers", "published app", isize=32,
              tsize=11.5, ssize=9.5))
p.append(tile(az("groups"), ccx, CY + 196, "Analysts", "report authoring", isize=32,
              tsize=11.5, ssize=9.5))
p.append(tile(fab("briefcase report"), ccx, CY + 318, "Stewards", "governed data", isize=30,
              tsize=11.5, ssize=9.5))
# consumption flows out of the Serve band, not the ingest band
p.append(arrow(FX + FW + 4, FY + 376, CX - 6, FY + 376, TEAL, "consume", sw=2.2))

# ------------------------------------------------- 5. CROSS-CUTTING PLATFORM
PX, PY, PW, PH = 24, 500, 1292, 96
p.append(band(PX, PY, PW, PH, "Platform services  ·  applied to every environment",
              "#F7F4FA", "#CBBEDA", lab_fill="#5B4B7A"))
plat = [
    (az("groups"),                              "Entra ID groups"),
    (az("conditional access"),                  "Conditional Access"),
    (az("entra privleged identity management"), "PIM (JIT admin)"),
    (az("key vault"),                           "Key Vault secrets"),
    (az("information protection"),              "Sensitivity labels"),
    (az("monitor"),                             "Audit & monitoring"),
    (az("app registrations"),                   "Service principals"),
]
step = PW / len(plat)
for i, (ic, lab) in enumerate(plat):
    px = PX + step * (i + 0.5)
    p.append(inline(ic, px - 13, PY + 40, 26))
    p.append(T(px, PY + 84, lab, 10.5, "600", GREY))

save(r"C:\Users\Shuo\AppData\Local\Temp\claude\C--Users-Shuo-OneDrive----bcpensionshackathon\16a4ed22-96b3-43f5-9649-e3ce5286ee1e\scratchpad\deck\out\d1_architecture.svg", W, H, p)
print("ok")
