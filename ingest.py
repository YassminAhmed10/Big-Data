"""
ingest.py — Stage 1: Data Ingestion & Merging
CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026

Dataset: Child Labour & Poverty (ILO + World Bank)
  - CLD_XHAS_SEX_AGE_GEO_NB_A  → children attending school
  - CLD_XHAN_SEX_AGE_GEO_NB_A  → children NOT attending school
  - 4909ea2f...Data.csv          → World Bank poverty indicators

Usage:
    python ingest.py <any_csv_path>
    python ingest.py combined_child_labor.csv   # if pre-merged
    python ingest.py CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv  # triggers auto-merge
"""

import sys
import os
import subprocess
import pandas as pd

# ── File names (expected in same directory as this script) ─────────────────
ATT_FILE     = "CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv"
NOT_ATT_FILE = "CLD_XHAN_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv"
POVERTY_FILE = "4909ea2f-5255-49a2-811e-5583974af6ab_Data.csv"
COMBINED     = "combined_child_labor.csv"
RAW_OUT      = "data_raw.csv"


def merge_sources() -> pd.DataFrame:
    """Merge child labour (ILO) + poverty (World Bank) into one raw dataset."""

    print("[ingest] Merging child labour sources ...")

    # ── Load ILO child labour data ────────────────────────────────────────
    df_att = pd.read_csv(ATT_FILE)
    df_not = pd.read_csv(NOT_ATT_FILE)

    df_att["school_status"] = "Attending"
    df_not["school_status"] = "Not Attending"

    df_labour = pd.concat([df_att, df_not], ignore_index=True)
    print(f"  • ILO child labour rows: {len(df_labour)}")
    print(f"  • ILO columns: {list(df_labour.columns)}")

    # ── Load World Bank poverty data ──────────────────────────────────────
    df_poverty = pd.read_csv(POVERTY_FILE)
    print(f"  • World Bank poverty rows: {len(df_poverty)}")
    print(f"  • World Bank columns: {list(df_poverty.columns)}")

    # Extract 4-digit year from strings like "2020 [YR2020]"
    df_poverty["Time"] = pd.to_numeric(
        df_poverty["Time"].astype(str).str.extract(r"(\d{4})")[0],
        errors="coerce"
    )

    # Ensure year column is int in labour dataset
    year_col = None
    for candidate in ["time", "TIME", "Year", "year", "REF_DATE", "ref_date"]:
        if candidate in df_labour.columns:
            year_col = candidate
            break

    if year_col is None:
        print("  [WARN] No year column found in ILO data — skipping merge with poverty data")
        df_combined = df_labour
    else:
        df_labour[year_col] = pd.to_numeric(df_labour[year_col], errors="coerce")

        # Find country/area column in labour dataset
        area_col = None
        for candidate in ["ref_area.label", "ref_area", "Country", "country", "COUNTRY"]:
            if candidate in df_labour.columns:
                area_col = candidate
                break

        if area_col is None:
            print("  [WARN] No country column found in ILO data — skipping poverty merge")
            df_combined = df_labour
        else:
            df_combined = pd.merge(
                df_labour,
                df_poverty,
                left_on=[area_col, year_col],
                right_on=["Country Name", "Time"],
                how="inner"
            )
            print(f"  • After merge: {df_combined.shape[0]} rows × {df_combined.shape[1]} columns")

    df_combined.to_csv(COMBINED, index=False)
    print(f"  ✓ Saved combined dataset → {COMBINED}")
    return df_combined


def load_dataset(path: str) -> pd.DataFrame:
    """Load any CSV / Excel / JSON file."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path)
    elif ext in (".xls", ".xlsx"):
        return pd.read_excel(path)
    elif ext == ".json":
        return pd.read_json(path)
    else:
        print(f"[ERROR] Unsupported extension: {ext}")
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python ingest.py <dataset_path>")
        print("        Example: python ingest.py combined_child_labor.csv")
        sys.exit(1)

    input_path = sys.argv[1]

    # ── Decide whether to merge or load directly ──────────────────────────
    if input_path == COMBINED and os.path.exists(COMBINED):
        # Pre-merged file passed directly
        print(f"[ingest] Loading pre-merged dataset: {COMBINED}")
        df = pd.read_csv(COMBINED)

    elif all(os.path.exists(f) for f in [ATT_FILE, NOT_ATT_FILE, POVERTY_FILE]):
        # Raw source files present — run the merge pipeline
        print("[ingest] Source CSVs detected — running merge ...")
        df = merge_sources()

    elif os.path.exists(input_path):
        # Fall back: load whatever file was given
        print(f"[ingest] Loading dataset: {input_path}")
        df = load_dataset(input_path)

    else:
        print(f"[ERROR] File not found: {input_path}")
        sys.exit(1)

    # ── Save raw copy ─────────────────────────────────────────────────────
    df.to_csv(RAW_OUT, index=False)

    print(f"\n[ingest] Raw dataset shape : {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"[ingest] Columns           : {list(df.columns)}")
    print(f"[ingest] Saved raw copy    → {RAW_OUT}")

    # ── Hand off to preprocessing ─────────────────────────────────────────
    print("\n[ingest] Calling preprocess.py ...")
    result = subprocess.run(
        [sys.executable, "preprocess.py", RAW_OUT],
        check=True
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
