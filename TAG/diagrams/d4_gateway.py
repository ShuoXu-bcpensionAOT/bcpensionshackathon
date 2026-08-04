"""Diagram 4 — Gateway & connectivity architecture (BCPC TAG deck)."""
from style import *

W, H = 1340, 620
p = []

p.append(T(24, 34, "EVERY SOURCE PATH IS PRIVATE  —  NO PUBLIC INTERNET ROUTE TO BCPC DATA",
           12.5, "700", TEAL, "start", "1.2"))

# ================================================== PATH A — managed / Azure
AY = 58
p.append(box(24, AY, 896, 222, "#F2F7F8", "#9FC4CC", rx=12, sw=1.6))
p.append(T(44, AY + 26, "PATH A  ·  AZURE & CLOUD SOURCES", 11.5, "700", TEAL_DK, "start", "1.2"))
p.append(T(900, AY + 26, "Microsoft-managed  ·  preferred default", 10.5, "600", GREY_MID, "end"))

p.append(box(48, AY + 42, 250, 158, WHITE, "#C8D5D9", rx=9))
p.append(T(173, AY + 66, "SOURCES", 10, "700", GREY_MID, "middle", "1.2"))
for i, (ic, lab) in enumerate([(az("sql database"), "Azure SQL DB / MI"),
                               (az("azure database postgresql server"), "PostgreSQL"),
                               (fab("cloud endpoint"), "SaaS & HTTP APIs")]):
    p.append(inline(ic, 74, AY + 80 + i * 40, 24))
    p.append(T(108, AY + 97 + i * 40, lab, 11, "600", GREY, "start"))

p.append(box(360, AY + 42, 210, 158, WHITE, "#C8D5D9", rx=9))
p.append(T(465, AY + 66, "PRIVATE LINK", 10, "700", GREY_MID, "middle", "1.2"))
p.append(inline(az("private link"), 449, AY + 84, 32))
p.append(T(465, AY + 140, "Private endpoints", 11.5, "700", GREY))
p.append(T(465, AY + 158, "traffic never leaves", 10, "400", GREY_MID))
p.append(T(465, AY + 173, "the Microsoft backbone", 10, "400", GREY_MID))

p.append(box(632, AY + 42, 250, 158, "#E4EFF2", TEAL_MID, rx=9, sw=1.6))
p.append(T(757, AY + 66, "VNET DATA GATEWAY", 10, "700", TEAL_DK, "middle", "1.2"))
p.append(inline(az("virtual networks"), 741, AY + 82, 32))
p.append(T(757, AY + 138, "Microsoft-managed", 11.5, "700", TEAL_DK))
p.append(T(757, AY + 156, "injected into a delegated subnet", 9.5, "400", GREY))
p.append(T(757, AY + 171, "nothing to install · nothing to patch", 9.5, "400", GREY))
p.append(T(757, AY + 188, "requires F8+  ·  BCPC runs F64", 9.5, "600", GREEN))

p.append(arrow(302, AY + 120, 354, AY + 120, TEAL, sw=2.0))
p.append(arrow(574, AY + 120, 626, AY + 120, TEAL, sw=2.0))

# ============================================ PATH B — self-hosted / on-prem
BY = 300
p.append(box(24, BY, 896, 222, "#F6F1E9", "#D9C7A8", rx=12, sw=1.6))
p.append(T(44, BY + 26, "PATH B  ·  ON-PREMISES & OCI SOURCES", 11.5, "700", AMBER, "start", "1.2"))
p.append(T(900, BY + 26, "Self-hosted  ·  minimum necessary footprint", 10.5, "600", GREY_MID, "end"))

p.append(box(48, BY + 42, 250, 158, WHITE, "#E0D2BC", rx=9))
p.append(T(173, BY + 66, "SOURCES", 10, "700", GREY_MID, "middle", "1.2"))
for i, (ic, lab) in enumerate([(az("managed database"), "Oracle (OCI / on-prem)"),
                               (az("sql server"), "On-prem SQL Server"),
                               (az("azure fileshares"), "File shares")]):
    p.append(inline(ic, 74, BY + 80 + i * 40, 24))
    p.append(T(108, BY + 97 + i * 40, lab, 11, "600", GREY, "start"))

p.append(box(360, BY + 42, 210, 158, WHITE, "#E0D2BC", rx=9))
p.append(T(465, BY + 66, "EXPRESSROUTE", 10, "700", GREY_MID, "middle", "1.2"))
p.append(inline(az("express route"), 449, BY + 84, 32))
p.append(T(465, BY + 140, "Private circuit", 11.5, "700", GREY))
p.append(T(465, BY + 158, "into the BCPC VNet", 10, "400", GREY_MID))
p.append(T(465, BY + 173, "no inbound ports opened", 10, "400", GREY_MID))

p.append(box(632, BY + 42, 250, 158, "#F6EDE0", "#C79A4B", rx=9, sw=1.6))
p.append(T(757, BY + 66, "ON-PREM DATA GATEWAY", 10, "700", AMBER, "middle", "1.2"))
p.append(inline(az("on-premises data gateway"), 741, BY + 82, 32))
p.append(T(757, BY + 138, "HA cluster · hardened VMs", 11.5, "700", GREY))
p.append(T(757, BY + 156, "outbound-only · patched monthly", 9.5, "400", GREY))
p.append(T(757, BY + 173, "dedicated to the gateway role", 9.5, "400", GREY))
p.append(T(757, BY + 190, "REQUIRED for Oracle mirroring", 9.5, "700", "#A03A3A"))

p.append(arrow(302, BY + 120, 354, BY + 120, AMBER, sw=2.0))
p.append(arrow(574, BY + 120, 626, BY + 120, AMBER, sw=2.0))

# ==================================================== FABRIC (shared target)
p.append(box(944, AY + 42, 372, 480, "#E4EFF2", TEAL, rx=12, sw=2.0))
p.append(T(1130, AY + 76, "MICROSOFT FABRIC", 13, "700", TEAL, "middle", "1.4"))
p.append(inline(fab("lakehouse"), 1110, AY + 96, 40))
p.append(T(1130, AY + 158, "F64 capacity · Canada Central", 11, "600", TEAL_DK))
p.append(box(968, AY + 180, 324, 106, WHITE, "#BBD5DB", rx=8))
p.append(T(1130, AY + 204, "WORKLOADS SERVED", 9.5, "700", GREY_MID, "middle", "1.2"))
for i, t in enumerate(["Pipelines · Copy Job · Dataflow Gen2",
                       "Mirroring (near-real-time replicas)",
                       "Power BI semantic models & reports"]):
    p.append(T(1130, AY + 228 + i * 20, t, 10.5, "400", GREY))

p.append(box(968, AY + 302, 324, 96, WHITE, "#BBD5DB", rx=8))
p.append(T(1130, AY + 326, "CREDENTIALS", 9.5, "700", GREY_MID, "middle", "1.2"))
p.append(inline(az("key vault"), 1117, AY + 336, 26))
p.append(T(1130, AY + 384, "Read-only source accounts in Key Vault", 10, "400", GREY))

p.append(box(968, AY + 412, 324, 88, WHITE, "#BBD5DB", rx=8))
p.append(T(1130, AY + 436, "DATA RESIDENCY", 9.5, "700", GREY_MID, "middle", "1.2"))
p.append(T(1130, AY + 458, "Capacity and gateway VMs both", 10, "400", GREY))
p.append(T(1130, AY + 474, "hosted in a Canadian region", 10, "400", GREY))

p.append(arrow(886, AY + 120, 938, AY + 120, TEAL, sw=2.4))
p.append(arrow(886, BY + 120, 938, BY + 120, AMBER, sw=2.4))

save(r"C:\Users\Shuo\AppData\Local\Temp\claude\C--Users-Shuo-OneDrive----bcpensionshackathon\16a4ed22-96b3-43f5-9649-e3ce5286ee1e\scratchpad\deck\out\d4_gateway.svg", W, H, p)
print("ok")
