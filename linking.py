#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from typing import List, Optional
import glob
import pandas as pd

# ================== Pfade (relativ zur Datei) ==================
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
IMAGE_DIR = os.path.join(BASE_DIR, "Images", "Objektbilder", "Objektbilder")  # <== dein Bilder-Root mit Jahresordnern
EXCEL_DIR = os.path.join(BASE_DIR, "Excel-Files", "Objektdaten")
OUT_CSV   = os.path.join(BASE_DIR, "linked.csv")
OUT_MISSES_CSV = os.path.join(BASE_DIR, "linked_misses.csv")  # optional: Übersicht der verfehlten IDs

# ================== Einstellungen ==================
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}
# Regex wie bei der anderen Gruppe:
# "1/1997/1063 0" -> g1=1, year=1997, g3=1063, g4=0 (g4=Serie ignorieren!)
OBJ_RE = re.compile(r"(\d+)\s*/\s*(\d{4})\s*/\s*(\d{3,4})\s+(\d+)", re.IGNORECASE)

def read_excel_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".xlsx":
            return pd.read_excel(path, engine="openpyxl")
        elif ext == ".xls":
            try:
                return pd.read_excel(path)            # Auto
            except Exception:
                return pd.read_excel(path, engine="xlrd")  # Fallback
        else:
            raise RuntimeError(f"Unsupported extension: {ext}")
    except Exception as e:
        print(f"WARN: {os.path.basename(path)} nicht lesbar ({e}).")
        return pd.DataFrame()

def load_selected_excels_only_t1(excel_dir: str) -> pd.DataFrame:
    """
    Lädt NUR die geforderten Teilmengen & NUR Spalte T1/t1 (wie bei der anderen Gruppe).
    - Liste_AK Kommunikation.xls → Zeilen 301–600
    - Liste_AEG Produktsammlung.xls → Zeilen 2501–3000
    """
    specs = [
        ("Liste_AK Kommunikation.xls", (300, 600)),        # 301–600
        ("Liste_AEG Produktsammlung.xls", (2500, 3000)),   # 2501–3000
    ]
    parts = []
    for fname, (start, stop) in specs:
        path = os.path.join(excel_dir, fname)
        if not os.path.exists(path):
            alt = os.path.splitext(path)[0] + ".xlsx"
            if os.path.exists(alt):
                path = alt
            else:
                print(f"WARN: Datei fehlt: {path}")
                avail = [os.path.basename(p) for p in glob.glob(os.path.join(excel_dir, "*.xls*"))]
                print("      Gefunden im Ordner:", avail[:10])
                continue

        print(f"Lesen: {os.path.relpath(path, BASE_DIR)}  Zeilen {start+1}–{stop}")
        df = read_excel_any(path)
        if df.empty:
            continue

        # T1/t1 robust finden
        lower = {c.lower(): c for c in df.columns}
        t1 = lower.get("t1")
        if not t1:
            print(f"WARN: {os.path.basename(path)} ohne Spalte T1/t1 -> übersprungen.")
            continue

        # zuschneiden + nur T1
        df = df.iloc[start:stop]
        slim = df[[t1]].copy()
        slim = slim.rename(columns={t1: "obj_id_raw"})
        slim["_source_file"] = os.path.basename(path)
        parts.append(slim)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

def parse_object_number(raw) -> Optional[dict]:
    """
    Zieht Objektnummer wie bei der anderen Gruppe per Regex aus T1:
    Liefert year (4-stellig) und base_prefix = 'g1-year-g3-'.
    Serie (g4) wird NICHT benutzt -> alle Serien werden gesammelt.
    """
    if not isinstance(raw, str):
        return None
    m = OBJ_RE.search(raw)
    if not m:
        return None
    g1, year, g3, _g4 = m.groups()
    base_prefix = f"{g1}-{year}-{g3}-"
    return {"year": year, "base_prefix": base_prefix}

def find_matching_files_in_year(images_root: str, year: str, base_prefix: str) -> List[str]:
    """
    Sucht NICHT rekursiv in <images_root>/<year>/ nach Dateien,
    deren Name mit base_prefix beginnt (case-insensitive) und erlaubter Endung.
    -> entspricht exakt dem Vorgehen der anderen Gruppe.
    """
    year_dir = os.path.join(images_root, year)
    if not os.path.isdir(year_dir):
        return []

    base_lower = base_prefix.lower()
    out = []
    for name in os.listdir(year_dir):
        p = os.path.join(year_dir, name)
        if not os.path.isfile(p):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext not in ALLOWED_EXTS:
            continue
        if name.lower().startswith(base_lower):
            out.append(p)
    return sorted(out)

def build_links_like_group(meta: pd.DataFrame, images_root: str) -> pd.DataFrame:
    """
    Wie die andere Gruppe:
    - nimm nur T1
    - parse (g1/year/g3/g4) -> base_prefix 'g1-year-g3-'
    - suche Dateien NUR in <images_root>/<year>/, die mit base_prefix starten
    - g4 (Serie) wird ignoriert -> alle 000/001/002 werden eingesammelt
    - dedupe je (year, base_prefix)
    Output: CSV mit obj_id_raw (Repräsentant), year, base_prefix, images (Basenamen), image_count, _source_files
    """
    if meta.empty:
        return pd.DataFrame(columns=["obj_id_raw", "year", "base_prefix", "images", "image_count", "_source_files"])

    # Vorbereiten: Serie bereinigen, Duplikate T1 entfernen (wie andere Gruppe es indirekt tut)
    series = (
        meta["obj_id_raw"]
        .dropna()
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .drop_duplicates()
    )

    rows = []
    misses = []
    seen_prefixes = set()  # (year, base_prefix)

    # Für Quellinfo
    src_map = {}  # (year, base_prefix) -> set(source_files)
    for _, r in meta.iterrows():
        parsed = parse_object_number(r["obj_id_raw"]) if isinstance(r["obj_id_raw"], str) else None
        if not parsed:
            continue
        key = (parsed["year"], parsed["base_prefix"].lower())
        src_map.setdefault(key, set()).add(r["_source_file"])

    for raw in series.tolist():
        parsed = parse_object_number(raw)
        if not parsed:
            misses.append({"obj_id_raw": raw, "reason": "Keine Objektnummer im Format 'g1/yyyy/g3 g4' gefunden"})
            continue

        year = parsed["year"]
        base_prefix = parsed["base_prefix"]
        key = (year, base_prefix.lower())
        if key in seen_prefixes:
            continue
        seen_prefixes.add(key)

        matches = find_matching_files_in_year(images_root, year, base_prefix)
        if not matches:
            misses.append({"obj_id_raw": raw, "year": year, "base_prefix": base_prefix, "reason": f"Keine Dateien in {year}/ mit Präfix '{base_prefix}'"})
            continue

        # Quellen zusammenfassen (alle Excels, die diese Basis hatten)
        source_files = sorted(src_map.get(key, set()))

        rows.append({
            "obj_id_raw": raw,                          # repräsentative T1 (erste/irgendeine)
            "year": year,
            "base_prefix": base_prefix,                 # z.B. '1-1997-1063-'
            "images": [os.path.basename(p) for p in matches],  # nur Dateinamen für Übersicht
            "image_count": len(matches),
            "_source_files": ", ".join(source_files),
        })

    # Misses optional mitschreiben
    if misses:
        pd.DataFrame(misses).to_csv(OUT_MISSES_CSV, index=False, encoding="utf-8")
        print(f"Misses-Log: {OUT_MISSES_CSV}")

    return pd.DataFrame(rows)

def main():
    # 1) Excels laden (nur T1, gewünschte Zeilenbereiche)
    meta = load_selected_excels_only_t1(EXCEL_DIR)
    if meta.empty:
        print("WARN: Keine Excel-Daten geladen.")
        pd.DataFrame(columns=["obj_id_raw","year","base_prefix","images","image_count","_source_files"]).to_csv(OUT_CSV, index=False, encoding="utf-8")
        print("CSV:", OUT_CSV)
        return

    # 2) Verknüpfen wie die andere Gruppe (Basispräfix im Jahresordner)
    linked = build_links_like_group(meta, IMAGE_DIR)

    # 3) Report
    unique_prefixes = len(linked)
    with_images = int((linked["image_count"] > 0).sum()) if not linked.empty else 0
    without_images = 0  # nach obigem Flow tauchen "ohne Bilder" nicht in linked auf, die stehen in linked_misses.csv
    print("excel_rows:", len(meta))
    print("unique_object_groups:", unique_prefixes)
    print("object_groups_with_images:", with_images)
    print("object_groups_without_images:", without_images)

    # 4) CSV schreiben
    linked.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("CSV:", OUT_CSV)

    # 5) kleine Stichprobe
    if not linked.empty:
        print("\nBeispiele MIT Bildern:")
        print(linked[["obj_id_raw","year","base_prefix","image_count"]].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
