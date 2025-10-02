#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import ast
import time
import random
import base64
import glob
from typing import List, Dict, Any

import pandas as pd
import requests
from dotenv import load_dotenv

# ================== Pfade/Dateien ==================
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LINKED_CSV = os.path.join(BASE_DIR, "linked.csv")
IMAGE_DIR  = os.path.join(BASE_DIR, "Images", "Objektbilder", "Objektbilder")
EXCEL_DIR  = os.path.join(BASE_DIR, "Excel-Files", "Objektdaten")
OUT_XLSX   = os.path.join(BASE_DIR, "catalog_results.xlsx")

# Welche Excel-Dateien + Zeilenbereiche du nutzt (wie beim Linking)
EXCEL_SPECS = [
    ("Liste_AK Kommunikation.xls", (300, 600)),      # 301–600
    ("Liste_AEG Produktsammlung.xls", (2500, 3000)), # 2501–3000
]

# ===== Mapping alter Excel-Header -> "korrekter Spaltenname" (für PROMPT-Anzeige) =====
# >>> Passe die rechten Werte exakt an deine Übersicht an. <<<
LABELS: Dict[str, str] = {
    "t1":  "Inventar-Nr",
    "t2":  "Hersteller",
    "t3":  "Materialien",
    "t4":  "Maße",
    "t5":  "Beschreibung (Excel)",
    "t6":  "Kategorie",
    "t7":  "Bezeichnung",
    "t8":  "Standort / Depot",
    "t9":  "Provenienz",
    "t10": "Zustand",
    "t11": "Signatur",
    "t12": "Inventarnr. (alt)",
    "t13": "Bildangabe (Excel)",
    "t14": "Datierung / Jahr",
}

# ===== Rate Limit / Retries =====
MAX_RETRIES = 6
BASE_BACKOFF = 2.0           # Sekunden
PAUSE_BETWEEN_OBJECTS = 5.0  # Sekunden zwischen Objekten
MAX_IMAGES_PER_CALL = 3      # klein halten gegen 429

# ================== OpenAI Setup ==================
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("❌ Kein OPENAI_API_KEY gefunden! Bitte .env prüfen.")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"

# ================== Utils ==================
PARENS_RE = re.compile(r"\s*\([^)]*\)")

def strip_parens(val):
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    return PARENS_RE.sub("", s).strip()

def read_excel_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".xlsx":
            return pd.read_excel(path, engine="openpyxl")
        elif ext == ".xls":
            try:
                return pd.read_excel(path)           # Auto
            except Exception:
                return pd.read_excel(path, engine="xlrd")  # Fallback
        else:
            raise RuntimeError(f"Unsupported extension: {ext}")
    except Exception as e:
        print(f"WARN: {os.path.basename(path)} nicht lesbar ({e}).")
        return pd.DataFrame()

def _norm_year(y: Any) -> str:
    s = "" if y is None else str(y)
    m = re.search(r"\b(\d{4})\b", s)
    return m.group(1) if m else ""

def find_file_anywhere(root: str, year: str, fname: str) -> str | None:
    year = _norm_year(year)
    if year:
        p = os.path.join(root, year, fname)
        if os.path.exists(p):
            return p
    p2 = os.path.join(root, fname)
    if os.path.exists(p2):
        return p2
    hits = glob.glob(os.path.join(root, "**", fname), recursive=True)
    return hits[0] if hits else None

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def _parse_images_field(field: Any) -> List[str]:
    if field is None or (isinstance(field, float) and pd.isna(field)):
        return []
    if isinstance(field, list):
        return [str(x).strip() for x in field if str(x).strip()]
    s = str(field).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            val = ast.literal_eval(s)
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
        except Exception:
            pass
    parts = [t.strip().strip("'\"[]") for t in s.split(",")]
    return [p for p in parts if p]

# ================== Prompt ==================
def build_prompt_from_tcols(meta_row: Dict[str, Any]) -> str:
    """
    Prompt nutzt ausschließlich Excel-Spalten:
    Für jede vorhandene T-Spalte (t1..t14):  'KORREKTER NAME [Tn]: Wert' (Klammern-Inhalte entfernt).
    """
    lines = []
    for tcol in [f"t{i}" for i in range(1, 15)]:
        if tcol in meta_row:
            label = LABELS.get(tcol, tcol.upper())
            val = strip_parens(meta_row.get(tcol, ""))
            if val:
                lines.append(f"- {label} [{tcol.upper()}]: {val}")

    meta_block = "\n".join(lines) if lines else "- (keine Metadaten gefunden)"

    return f"""Du bist Museums-Kurator:in.
Erstelle eine präzise, wissenschaftlich klingende **Objektbeschreibung auf Deutsch**.
Vermeide Phrasen wie „Dieses Bild zeigt…“. Beziehe dich auf sichtbare Merkmale:
Materialien, Konstruktion, Maße (falls erkennbar), Funktion/Nutzung, ggf. historischer Kontext.

Nutze ausschließlich die folgenden Excel-Metadaten (korrekter Name + alter Spaltenname) als Kontext, ohne sie wörtlich zu wiederholen:

{meta_block}
"""

# ================== API mit Retries ==================
def _post_with_retries(payload: dict) -> dict:
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(OPENAI_URL, headers=headers, json=payload, timeout=120)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"429 Too Many Requests – warte {wait:.1f}s (Versuch {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            wait = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.5)
            print(f"WARN: API-Fehler ({e}) – backoff {wait:.1f}s (Versuch {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)
    raise RuntimeError("Abbruch nach MAX_RETRIES wegen wiederholter 429/Netzwerkfehler.")

def query_chatgpt(images: List[str], prompt_text: str) -> str:
    images = images[:MAX_IMAGES_PER_CALL]
    image_contents = []
    for p in images:
        try:
            b64 = encode_image(p)
            image_contents.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"}
            })
        except Exception as e:
            print(f"WARN: Konnte Bild nicht lesen ({p}): {e}")

    payload = {
        "model": OPENAI_MODEL,
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt_text}, *image_contents]
        }],
        "max_tokens": 700
    }
    data = _post_with_retries(payload)
    return data["choices"][0]["message"]["content"]

# ================== Excel-Metadaten laden (nur echte Excel-Spalten) ==================
def load_meta_rows_from_excels(excel_dir: str) -> pd.DataFrame:
    """
    Lädt ausschließlich die definierten Zeilenbereiche.
    Behält Original-Header als t1..t14 (lowercase) und säubert nur Werte (Klammern raus).
    """
    parts = []
    want_lower = {f"t{i}" for i in range(1, 15)}  # t1..t14

    for fname, (start, stop) in EXCEL_SPECS:
        path = os.path.join(excel_dir, fname)
        if not os.path.exists(path):
            alt = os.path.splitext(path)[0] + ".xlsx"
            path = alt if os.path.exists(alt) else path

        df = read_excel_any(path)
        if df.empty:
            continue

        lower_map = {c.lower(): c for c in df.columns}
        have_keys = [k for k in want_lower if k in lower_map]
        if not have_keys:
            continue

        cols_real = [lower_map[k] for k in have_keys]
        sub = df.iloc[start:stop][cols_real].copy()

        # Spaltennamen auf t*-lower normalisieren
        rename_dict = {lower_map[k]: k for k in have_keys}
        sub = sub.rename(columns=rename_dict)

        # Werte säubern (Klammern raus)
        for col in sub.columns:
            sub[col] = sub[col].map(strip_parens)

        parts.append(sub)

    if not parts:
        return pd.DataFrame(columns=["t1"])

    full_cols = sorted(set().union(*[set(p.columns) for p in parts]))
    parts = [p.reindex(columns=full_cols) for p in parts]
    meta = pd.concat(parts, ignore_index=True)
    return meta

# ================== Hauptklasse ==================
class CatalogGenerator:
    def __init__(self, linked_csv: str, excel_dir: str, image_dir: str, output_excel: str):
        self.linked = pd.read_csv(linked_csv)
        self.meta_df = load_meta_rows_from_excels(excel_dir)  # enthält t*-Spalten
        self.image_dir = image_dir
        self.output_excel = output_excel

    def _meta_for_obj(self, obj_id_raw: str) -> Dict[str, Any]:
        """
        Sucht Metadaten über T1 (t1). Nutzt nur Excel-Spalten, nichts Neues.
        """
        if self.meta_df.empty or "t1" not in self.meta_df.columns:
            return {"t1": obj_id_raw}
        left = str(obj_id_raw).strip()
        hit = self.meta_df[self.meta_df["t1"].astype(str).str.strip() == left]
        if hit.empty:
            return {"t1": obj_id_raw}
        return hit.iloc[0].to_dict()  # nur t*-Keys

    def _safe_write_excel(self, df: pd.DataFrame, path: str) -> str:
        # robustes Speichern (falls OneDrive/Excel sperrt)
        try:
            df.to_excel(path, index=False, engine="openpyxl")
            return path
        except PermissionError:
            import datetime
            base, ext = os.path.splitext(path)
            alt = f"{base}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
            df.to_excel(alt, index=False, engine="openpyxl")
            print(f"WARN: Datei war gesperrt – gespeichert als {os.path.basename(alt)}")
            return alt

    def run(self, limit: int = 1):
        if self.linked.empty:
            print("WARN: linked.csv ist leer — nichts zu tun.")
            pd.DataFrame(columns=["objekt_id","description"]).to_excel(
                self.output_excel, index=False, engine="openpyxl"
            )
            print("📘 Ergebnisse gespeichert in:", self.output_excel)
            return

        results = []

        for i, (_, row) in enumerate(self.linked.iterrows()):
            if i >= limit:
                break

            obj_id = str(row.get("obj_id_raw", ""))
            year   = str(row.get("year", ""))

            # Bilder aus linked.csv
            image_names = _parse_images_field(row.get("images", ""))
            image_paths = []
            for img in image_names:
                if os.path.isabs(img) or os.path.sep in img:
                    p = img
                    if os.path.exists(p):
                        image_paths.append(p)
                        continue
                found = find_file_anywhere(self.image_dir, year, img)
                if found:
                    image_paths.append(found)

            # Metadaten (nur t*-Spalten)
            meta_row = self._meta_for_obj(obj_id)

            # Prompt ausschließlich aus Excel-Spalten: "korrekter Name [Tn]: Wert"
            prompt = build_prompt_from_tcols(meta_row)
            print(f"\n--- PROMPT für {obj_id} ---\n{prompt}\n--- ENDE PROMPT ---\n")


            print(f"🔍 {obj_id}: {len(image_paths)} Bilder → Anfrage an ChatGPT …")
            try:
                description = query_chatgpt(image_paths, prompt)
            except Exception as e:
                print(f"❌ Fehler bei {obj_id}: {e}")
                description = f"ERROR: {e}"

            # === OUTPUT: ausschließlich objekt_id + description ===
            results.append({
                "objekt_id": obj_id,
                "description": description
            })

            time.sleep(PAUSE_BETWEEN_OBJECTS)

        df_out = pd.DataFrame(results, columns=["objekt_id","description"])
        saved = self._safe_write_excel(df_out, self.output_excel)
        print(f"\n📘 Ergebnisse gespeichert in: {saved}")

# ================== Start ==================
if __name__ == "__main__":
    cg = CatalogGenerator(
        linked_csv=LINKED_CSV,
        excel_dir=EXCEL_DIR,
        image_dir=IMAGE_DIR,
        output_excel=OUT_XLSX
    )
    cg.run(limit=3)  # fürs Testen klein halten; später erhöhen
