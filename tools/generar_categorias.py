"""GENERADOR del mapeo SEC_EJEC -> CATEGORIA (A-G) por UBIGEO (cruce exacto).

Cruza el Anexo IV del DS 003-2026-EF (clasificacion oficial de municipalidades del
Programa de Incentivos 2026, que SI trae ubigeo) contra el ubigeo de cada ejecutora
municipal del SIAF. Al cruzar por ubigeo el match es EXACTO: cada municipalidad
recibe su categoria oficial, sin ambiguedad de homonimos (cada "Santa Rosa" tiene
su ubigeo unico) y sin perder ningun municipio.

Entradas (el PDF es OPCIONAL):
  - *anexo-iv*.pdf  (Anexo IV del DS 003-2026-EF). Si esta, se parsea y refresca
    data/categorias_ubigeo.csv. Si NO esta, se usa el data/categorias_ubigeo.csv ya
    guardado (que es el PDF parseado) -> sigue siendo reproducible sin el PDF.
  - data/bronze/SIAF-*Ingreso*.parquet
Salidas:
  - data/categorias_ubigeo.csv   (catalogo oficial: Ubigeo, Categoria, ubicacion)
  - data/categorias_secejec.csv  (lo consume app/silver/quality.py)
Uso (desde la raiz del proyecto):  python tools/generar_categorias.py
"""
import csv
import glob
import os
import re

import pyarrow.parquet as pq
import pypdf

# Ubigeos que NO figuran en el Anexo IV (distritos creados despues), verificados a mano:
OVERRIDES_UBIGEO = {
    "160405": "G",  # Santa Rosa de Loreto (prov. Mariscal Ramon Castilla, Loreto)
}

_REPL = chr(65533)  # caracter de reemplazo (la N con tilde sale asi al extraer el PDF)


def parse_anexo_iv(pdf_path: str) -> tuple[dict[str, str], dict[str, str]]:
    """Devuelve (ubigeo->categoria, ubigeo->'DEPTO PROVINCIA DISTRITO') del Anexo IV."""
    txt = " ".join(p.extract_text() for p in pypdf.PdfReader(pdf_path).pages)
    txt = re.sub(r"\s+", " ", txt)
    # Cada registro arranca con  "N Ubigeo(6) ..."; partimos por ese patron.
    partes = re.split(r"(?:^| )(\d{1,4}) (\d{6}) ", txt)
    it = iter(partes[1:])
    cat: dict[str, str] = {}
    loc: dict[str, str] = {}
    for _n, ubigeo, body in zip(it, it, it):
        # La categoria es el unico token A-G suelto del registro (los nombres no
        # tienen letras sueltas); el ultimo cubre tambien el caso con encabezado.
        letras = re.findall(r"(?<![A-Z])([A-G])(?![A-Z])", body)
        if not letras:
            continue
        cat[ubigeo] = letras[-1]
        nombre = re.split(r" Ubigeo Departamento", body)[0]
        nombre = re.sub(r"\s*" + letras[-1] + r"\s*$", "", nombre).strip()
        loc[ubigeo] = re.sub(_REPL, "N", nombre)
    return cat, loc


def cargar_catalogo_ubigeo() -> dict[str, str]:
    """Devuelve el catalogo ubigeo->categoria.

    Si esta el PDF del Anexo IV en la raiz, lo parsea y (re)escribe
    ``data/categorias_ubigeo.csv``. Si NO esta el PDF, cae al
    ``data/categorias_ubigeo.csv`` ya guardado (que es el PDF parseado), de modo
    que el cruce siga siendo reproducible sin depender del PDF.
    """
    pdf_path = next(iter(sorted(glob.glob("*anexo-iv*.pdf") + glob.glob("*Anexo*IV*.pdf"))), None)
    if pdf_path:
        ubigeo_cat, ubigeo_loc = parse_anexo_iv(pdf_path)
        ubigeo_cat.update(OVERRIDES_UBIGEO)
        with open("data/categorias_ubigeo.csv", "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Ubigeo", "Categoria", "Departamento_Provincia_Distrito"])
            for u in sorted(ubigeo_cat):
                w.writerow([u, ubigeo_cat[u], ubigeo_loc.get(u, "")])
        print(f"Anexo IV ({pdf_path}): {len(ubigeo_cat)} ubigeos -> data/categorias_ubigeo.csv")
        return ubigeo_cat

    catalogo = "data/categorias_ubigeo.csv"
    if not os.path.exists(catalogo):
        raise FileNotFoundError(
            "No se encontro el PDF del Anexo IV ni data/categorias_ubigeo.csv. "
            "Coloca el PDF (*anexo-iv*.pdf) en la raiz para regenerar el catalogo."
        )
    ubigeo_cat = {}
    with open(catalogo, encoding="utf-8-sig") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            if row and row[0].strip():
                ubigeo_cat[row[0].strip()] = row[1].strip()
    ubigeo_cat.update(OVERRIDES_UBIGEO)
    print(f"Sin PDF: usando {catalogo} ({len(ubigeo_cat)} ubigeos)")
    return ubigeo_cat


def main() -> None:
    ubigeo_cat = cargar_catalogo_ubigeo()

    # Cruce SIAF por ubigeo (solo ejecutoras municipales).
    want = ["SEC_EJEC", "EJECUTORA_NOMBRE", "NIVEL_GOBIERNO_NOMBRE",
            "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA"]
    sec_cat: dict[int, str] = {}
    sin_cat: list[tuple[int, str, str]] = []
    vistos: set[int] = set()
    for fp in sorted(glob.glob("data/bronze/SIAF-*Ingreso*.parquet")):
        d = pq.read_table(fp, columns=want).to_pydict()
        for sec, nom, niv, dd, pp, di in zip(*[d[c] for c in want]):
            if sec is None or not nom:
                continue
            try:
                sec = int(float(sec))
            except (TypeError, ValueError):
                continue
            if sec in vistos:
                continue
            nu = str(nom).upper().replace(_REPL, "N")
            if ("LOCAL" not in str(niv or "").upper()
                    or "MUNICIPALIDAD" not in nu or "MANCOMUNIDAD" in nu):
                continue
            vistos.add(sec)
            try:
                ubigeo = f"{int(float(dd)):02d}{int(float(pp)):02d}{int(float(di)):02d}"
            except (TypeError, ValueError):
                continue
            if ubigeo in ubigeo_cat:
                sec_cat[sec] = ubigeo_cat[ubigeo]
            else:
                sin_cat.append((sec, ubigeo, nu))

    print(f"SIAF municipios: {len(vistos)} | con categoria: {len(sec_cat)} "
          f"({100 * len(sec_cat) / max(len(vistos), 1):.1f}%) | sin categoria: {len(sin_cat)}")
    for sec, ubigeo, nu in sin_cat:
        print(f"  SIN CATEGORIA  SEC={sec} ubigeo={ubigeo} {nu}")

    with open("data/categorias_secejec.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["SecEjec", "Categoria"])
        for sec in sorted(sec_cat):
            w.writerow([sec, sec_cat[sec]])
    print("Escrito data/categorias_secejec.csv")


if __name__ == "__main__":
    main()
