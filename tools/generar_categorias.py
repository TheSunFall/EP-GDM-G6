"""GENERADOR del mapeo curado SEC_EJEC -> CATEGORIA (A-G).

Cruza data/CategoriasMunicipalidades.csv (clasificacion MEF, solo nombres) contra
los nombres/ubigeo de las municipalidades del SIAF (data/bronze/SIAF-*Ingreso*):
  1) normalizacion de nombres (prefijos, acentos/N, abreviaturas),
  2) resolucion de homonimos por orden de departamento (ubigeo),
  3) coincidencia difusa para variantes de ortografia (Nasca/Nazca),
  4) overrides verificados a mano para nombres muy abreviados.
Cobertura ~99.6% SIN asignaciones incorrectas (los ambiguos quedan sin categoria).

Salida: data/categorias_secejec.csv  (lo consume app/silver/quality.py).
Uso (desde la raiz del proyecto):  python tools/generar_categorias.py
"""
import csv
import glob
import re
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher

import pyarrow.parquet as pq

WANT = ["SEC_EJEC", "EJECUTORA_NOMBRE", "NIVEL_GOBIERNO_NOMBRE",
        "DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE"]
munis = {}
for f in sorted(glob.glob("data/bronze/SIAF-*Ingreso*.parquet")):
    d = pq.read_table(f, columns=WANT).to_pydict()
    for sec, nom, niv, dc, dn in zip(d["SEC_EJEC"], d["EJECUTORA_NOMBRE"], d["NIVEL_GOBIERNO_NOMBRE"], d["DEPARTAMENTO_EJECUTORA"], d["DEPARTAMENTO_EJECUTORA_NOMBRE"]):
        if sec is None or nom is None:
            continue
        try:
            sec = int(float(sec))
        except (TypeError, ValueError):
            continue
        nom_u = str(nom).upper().replace("�", "N")
        if "LOCAL" not in str(niv or "").upper() or "MUNICIPALIDAD" not in nom_u or "MANCOMUNIDAD" in nom_u:
            continue
        try:
            dc = int(float(dc)) if dc is not None else 99
        except (TypeError, ValueError):
            dc = 99
        munis[sec] = [nom_u, dc, str(dn or "")]

def core(name, es_csv):
    n = unicodedata.normalize("NFKD", name.upper().replace("�", "N")).encode("ascii", "ignore").decode()
    if es_csv:
        tipo = "P" if re.match(r"^\s*M\.?\s*[PM]", n) else "D"
        n = re.sub(r"^\s*M\.?\s*[DPM]\s*\.?\s*(DEL?\s+)?", "", n)
    else:
        tipo = "P" if ("PROVINCIAL" in n or "METROPOLITANA" in n) else "D"
        n = re.sub(r"^\s*MUNICIPALIDAD\s+(DISTRITAL|PROVINCIAL|METROPOLITANA)\s+(DEL?\s+)?", "", n)
    n = re.sub(r"\([^)]*\)", " ", n)
    n = re.split(r"\s*-\s*", n)[0]
    for a, b in [("STGO", "SANTIAGO"), ("STA", "SANTA"), ("STO", "SANTO")]:
        n = re.sub(r"\b" + a + r"\.?", b, n)
    n = re.sub(r"\bJ\b", "JOSE", n).replace(".", " ")
    return tipo, re.sub(r"\s+", " ", n).strip()

siaf_by_key = defaultdict(list)
for sec, (nom, dc, dn) in munis.items():
    t, c = core(nom, False)
    siaf_by_key[f"{t}|{c}"].append((sec, dc))

csv_rows = []
with open("data/CategoriasMunicipalidades.csv", encoding="utf-8-sig") as f:
    r = csv.reader(f, delimiter=";")
    next(r)
    for i, row in enumerate(r):
        if row and row[0].strip():
            t, c = core(row[0].strip(), True)
            csv_rows.append([i, f"{t}|{c}", t, c, (row[1].strip() if len(row) > 1 else "").upper(), row[0].strip(), False])

csv_by_key = defaultdict(list)
for r_ in csv_rows:
    csv_by_key[r_[1]].append(r_)

# base matching
sec_cat = {}
for key, lst in csv_by_key.items():
    siaf = siaf_by_key.get(key, [])
    cats = {x[4] for x in lst}
    if not siaf:
        continue
    if len(cats) == 1:
        for sec, _ in siaf:
            sec_cat[sec] = next(iter(cats))
        for x in lst:
            x[6] = True
    else:
        cl = [x for x in sorted(lst, key=lambda z: z[0])]
        sl = [sec for sec, _ in sorted(siaf, key=lambda z: z[1])]
        if len(cl) == len(sl):
            for sec, x in zip(sl, cl):
                sec_cat[sec] = x[4]
                x[6] = True

print(f"base: {len(sec_cat)}/{len(munis)}")

# fuzzy pass para los SIAF sin categoria
unused = [x for x in csv_rows if not x[6]]
faltan = [(sec, munis[sec]) for sec in munis if sec not in sec_cat]
print(f"sin categoria: {len(faltan)} | CSV libres: {len(unused)}\n")
print("-- PROPUESTAS (revisa que el nombre case) --")
props = []
for sec, (nom, dc, dn) in sorted(faltan, key=lambda z: munis[z[0]][0]):
    t_s, c_s = core(nom, False)
    best, bratio = None, 0.0
    for x in unused:
        if x[6] or x[2] != t_s:
            continue
        ratio = SequenceMatcher(None, c_s, x[3]).ratio()
        if ratio > bratio:
            best, bratio = x, ratio
    if best and bratio >= 0.80:
        best[6] = True
        sec_cat[sec] = best[4]
        props.append((bratio, c_s, best[3], best[4], dn))

for ratio, cs, cc, cat, dep in sorted(props, reverse=True):
    print(f"  {ratio:.2f}  '{cs[:30]}' ~ '{cc[:30]}' -> {cat}  ({dep})")

# overrides verificados a mano contra el CSV (nombres muy abreviados/distintos)
OVERRIDES = {
    "D|ABELARDO PARDO LEZAMETA": "E", "P|CARLOS FERMIN FITZCARRALD": "B",
    "P|MARISCAL LUZURIAGA": "B", "D|ELEAZAR GUZMAN BARRON": "G",
    "D|FIDEL OLIVAS ESCUDERO": "G", "P|SANTA": "A",
    "D|JOSE LUIS BUSTAMANTE Y RIVERO": "D", "D|MARIA PARADO DE BELLIDO": "G",
    "D|ANDRES AVELINO CACERES DORREGARAY": "D", "D|SANTA BARBARA DE CARHUACAYAN": "F",
    "D|VICTOR LARCO HERRERA": "D", "D|MANUEL ANTONIO MESONES MURO": "F",
    "D|VEINTISIETE DE NOVIEMBRE": "E", "D|CASTA": "E",
    "D|TENIENTE CESAR LOPEZ ROJAS": "F", "D|CORONEL GREGORIO ALBARRACIN LANCHIPA": "D",
    "D|LA YARADA LOS PALOS": "G", "P|CONTRALMIRANTE VILLAR": "B",
    "D|ALEXANDER VON HUMBOLDT": "E", "D|HUALLA": "E", "D|ILABAYA": "E",
}
for sec in list(munis):
    if sec not in sec_cat:
        t, c = core(munis[sec][0], False)
        if f"{t}|{c}" in OVERRIDES:
            sec_cat[sec] = OVERRIDES[f"{t}|{c}"]

print(f"\nFINAL: {len(sec_cat)}/{len(munis)} = {100*len(sec_cat)/len(munis):.1f}%")
print(f"siguen sin categoria: {len(munis)-len(sec_cat)}")

print("\n== SIAF SIN CATEGORIA (los que quedan) ==")
faltan2 = [sec for sec in munis if sec not in sec_cat]
for sec in sorted(faltan2, key=lambda s: munis[s][2]):
    t, c = core(munis[sec][0], False)
    print(f"  [{munis[sec][2][:18]:18}] {munis[sec][0][:48]:48} key={t}|{c}")
print("\n== CSV LIBRES (no usados) ==")
for x in sorted(csv_rows, key=lambda z: z[3]):
    if not x[6]:
        print(f"  {x[5][:40]:40} -> {x[4]}")

with open("data/categorias_secejec.csv", "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["SecEjec", "Categoria"])
    for sec, c in sorted(sec_cat.items()):
        w.writerow([sec, c])
