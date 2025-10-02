#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, unicodedata, glob
import pandas as pd

# ---- feste Pfade ----
IMAGE_DIR = r"Images/Objektbilder/Objektbilder"
EXCEL_DIR = r"Excel-Files/Objektdaten"
OUT_CSV   = r"linked.csv"

# ---- Basics ----
IMG_EXTS = {".jpg", ".jpeg"}
ID_REGEX = re.compile(r"(?P<id>[^/\\]+?)(?:\.[A-Za-z0-9]+)?$")
EXCEL_ID_COL = "t1"  # ID

# Mapping laut deiner Übersicht; Klammerteile werden später automatisch entfernt
T_COLUMN_MAP = {
    "t1":  "Inventar-Nr",
    "t2":  "Beteiligte",
    "t3":  "Materialien",
    "t5":  "Maßangaben",
    "t8":  "Standort",
    "t9":  "Standort-Beschreibung (eigener Name; in Schreibmaschinen nicht vorhanden)",
    "t10": "t10",  # kein Klarname gegeben -> neutral übernehmen
    "t12": "Ausführlicher Objektname mit allen Informationen (eigener Name; in Schreibmaschinen nur teilweise vorhanden)",
    "t13": "Verlinkte Bilder",
    "t14": "Herst.-Jahr",
}

# Anzeige-Reihenfolge (falls vorhanden)
OUTPUT_ORDER = [
    "Inventar-Nr", "Ausführlicher Objektname", "Beteiligte", "Materialien",
    "Maßangaben", "Herst.-Jahr", "Standort", "Standort-Beschreibung",
    "Verlinkte Bilder", "t10"
]

def _norm(s):
    if s is None: return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return "".join(ch for ch in s if ch.isalnum() or ch in "-_").lower()

def _extract_id(fname: str):
    m = ID_REGEX.search(os.path.basename(fname))
    return m.group("id") if m else None

def scan_images(images_dir: str) -> pd.DataFrame:
    rows = []
    for root, _, files in os.walk(images_dir):
        for f in files:
            if os.path.splitext(f)[1].lower() in IMG_EXTS:
                oid = _extract_id(f)
                rows.append({
                    "file_path": os.path.join(root, f),
                    "file_name": f,
                    "obj_id_raw": oid,
                    "obj_id_norm": _norm(oid) if oid else ""
                })
    return pd.DataFrame(rows)

def _strip_parens(colname: str) -> str:
    # entfernt alles in Klammern + trimmt
    return re.sub(r"\s*\([^)]*\)", "", str(colname)).strip()

def read_excel_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".xls":
            # benötigt xlrd==1.2.0
            return pd.read_excel(path, engine="xlrd")
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"WARN: Konnte {os.path.basename(path)} nicht lesen ({e}). Datei wird übersprungen.")
        return pd.DataFrame()

def load_all_excels(excel_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(excel_dir, "*.xls")) +
                   glob.glob(os.path.join(excel_dir, "*.xlsx")))
    parts = []
    for f in files:
        df = read_excel_any(f)
        if df.empty:
            continue
        # t*-Spalten in Klartext umbenennen (nur vorhandene)
        ren = {t: nice for t, nice in T_COLUMN_MAP.items() if t in df.columns}
        df = df.rename(columns=ren)
        # Klammern aus allen Spaltennamen entfernen
        df.columns = [_strip_parens(c) for c in df.columns]
        # ID-Spalte (t1 oder schon "Inventar-Nr")
        id_col = "t1" if "t1" in df.columns else ("Inventar-Nr" if "Inventar-Nr" in df.columns else None)
        if not id_col:
            print(f"WARN: In {os.path.basename(f)} keine ID-Spalte (t1/Inventar-Nr). Übersprungen.")
            continue
        df["_obj_id_norm"] = df[id_col].map(_norm)
        df["_source_file"] = os.path.basename(f)
        parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def main():
    imgs = scan_images(IMAGE_DIR)
    if imgs.empty:
        print("WARN: Keine JPGs gefunden.")
    meta = load_all_excels(EXCEL_DIR)
    if meta.empty:
        print("WARN: Keine Excel-Daten geladen.")

    linked = imgs.merge(meta, left_on="obj_id_norm", right_on="_obj_id_norm", how="left", suffixes=("", "_tbl"))

    # Report
    missing_rows = linked[linked["_obj_id_norm"].isna()]["obj_id_raw"].dropna().astype(str).unique().tolist()
    img_ids = set(imgs["obj_id_norm"].dropna())
    id_col_used = "t1" if "t1" in meta.columns else ("Inventar-Nr" if "Inventar-Nr" in meta.columns else None)
    missing_imgs = meta.loc[~meta["_obj_id_norm"].isin(img_ids), id_col_used].dropna().astype(str).unique().tolist() if id_col_used else []
    dup_imgs = imgs["obj_id_norm"][imgs["obj_id_norm"].duplicated()].dropna().unique().tolist()
    dup_meta = meta["_obj_id_norm"][meta["_obj_id_norm"].duplicated()].dropna().unique().tolist()

    print("images_total:", len(imgs))
    print("excel_rows:", len(meta))
    print("linked_rows:", len(linked) - len(missing_rows))
    print("unmatched_images (Bilder ohne Excel):", len(missing_rows))
    print("unmatched_excel_rows (Excel ohne Bild):", len(missing_imgs))
    print("duplicate_ids_images:", len(dup_imgs))
    print("duplicate_ids_excel:", len(dup_meta))
    print("excel_id_col_used:", id_col_used)

    # Ausgabe-Spalten sortieren: Bildbasis + gewünschte Metadaten in definierter Reihenfolge + Rest
    base_cols = ["file_path", "file_name", "obj_id_raw", "obj_id_norm", "_source_file"]
    nice_present = [c for c in OUTPUT_ORDER if c in linked.columns]
    other_meta = [c for c in linked.columns if c not in base_cols + nice_present + ["_obj_id_norm"]]
    out_cols = base_cols + nice_present + other_meta

    linked[out_cols].to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("CSV:", os.path.abspath(OUT_CSV))

if __name__ == "__main__":
    main()
