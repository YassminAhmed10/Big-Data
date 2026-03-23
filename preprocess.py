"""
preprocess.py — Stage 2: Data Preprocessing
CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026

Dataset: ILO Child Labour + World Bank Poverty
Pipeline stages (min 3 tasks each):
  1. Data Cleaning      — dedup, drop high-null cols, fill NaN, fix dtypes
  2. Feature Transform  — encode categoricals, scale numerics
  3. Dimensionality Red — column selection by variance / PCA
  4. Discretization     — bin labour rate & poverty index columns

Saves result as data_preprocessed.csv, then calls analytics.py.
"""

import sys
import os
import subprocess
import warnings
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.decomposition import PCA

warnings.filterwarnings("ignore")

OUTPUT_PATH = "data_preprocessed.csv"

# Columns that are IDs / labels — skip scaling but may encode
ID_COLS = {"ref_area", "ref_area.label", "sex.label", "classif1.label",
           "classif2.label", "source.label", "obs_status.label",
           "school_status", "Country Name", "Country Code",
           "Indicator Name", "Indicator Code"}


# ─────────────────────────────────────────────────────────────
# STAGE 1 — Data Cleaning
# ─────────────────────────────────────────────────────────────
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[preprocess | STAGE 1] Data Cleaning")
    original = df.shape

    # Task 1 — Remove duplicate rows
    df = df.drop_duplicates()
    print(f"  • Removed {original[0] - df.shape[0]} duplicate rows")

    # Task 2 — Drop columns where >60% values are missing
    thresh = 0.60
    high_null = [c for c in df.columns if df[c].isnull().mean() > thresh]
    if high_null:
        df = df.drop(columns=high_null)
        print(f"  • Dropped {len(high_null)} high-null columns (>{int(thresh*100)}%): {high_null[:5]}{'...' if len(high_null)>5 else ''}")
    else:
        print(f"  • No columns exceeded {int(thresh*100)}% missing threshold")

    # Task 3 — Fill missing values (numeric → median, categorical → mode)
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    filled_n = 0
    for col in num_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())
            filled_n += 1
    if filled_n:
        print(f"  • Filled NaN in {filled_n} numeric column(s) with median")

    filled_c = 0
    for col in cat_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown")
            filled_c += 1
    if filled_c:
        print(f"  • Filled NaN in {filled_c} categorical column(s) with mode")

    # Task 4 — Strip whitespace & standardise string columns
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    print(f"  • Stripped whitespace from {len(cat_cols)} string column(s)")

    # Task 5 — Drop constant columns (zero variance)
    const_cols = [c for c in df.select_dtypes(include=[np.number]).columns
                  if df[c].nunique() <= 1]
    if const_cols:
        df = df.drop(columns=const_cols)
        print(f"  • Dropped {len(const_cols)} constant/near-constant column(s): {const_cols}")

    print(f"  → Shape after cleaning: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# STAGE 2 — Feature Transformation
# ─────────────────────────────────────────────────────────────
def transform_features(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[preprocess | STAGE 2] Feature Transformation")

    cat_cols = [c for c in df.select_dtypes(include=["object", "category"]).columns
                if c not in {"Country Name", "Country Code", "Indicator Name", "Indicator Code"}]

    # Task 1 — Label-encode low-cardinality categoricals
    le = LabelEncoder()
    encoded, dropped = [], []
    for col in cat_cols:
        n = df[col].nunique()
        if n <= 25:
            df[col] = le.fit_transform(df[col].astype(str))
            encoded.append(col)
        else:
            df = df.drop(columns=[col])
            dropped.append(col)

    if encoded:
        print(f"  • Label-encoded {len(encoded)} column(s): {encoded}")
    if dropped:
        print(f"  • Dropped {len(dropped)} high-cardinality column(s): {dropped[:4]}{'...' if len(dropped)>4 else ''}")

    # Task 2 — Drop any remaining non-numeric columns
    remaining_obj = df.select_dtypes(include=["object"]).columns.tolist()
    if remaining_obj:
        df = df.drop(columns=remaining_obj)
        print(f"  • Dropped remaining text columns: {remaining_obj[:4]}")

    # Task 3 — StandardScaler on all numeric columns
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        scaler = StandardScaler()
        df[num_cols] = scaler.fit_transform(df[num_cols])
        print(f"  • StandardScaler applied to {len(num_cols)} column(s)")

    # Task 4 — Fill any NaN introduced by scaling (shouldn't happen, safety net)
    df = df.fillna(0)
    print(f"  → Shape after transformation: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# STAGE 3 — Dimensionality Reduction
# ─────────────────────────────────────────────────────────────
def reduce_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[preprocess | STAGE 3] Dimensionality Reduction")
    n_cols = df.shape[1]

    if n_cols > 15:
        # PCA when many features present
        n_components = min(12, df.shape[0] - 1, n_cols)
        pca = PCA(n_components=n_components, random_state=42)
        transformed = pca.fit_transform(df)
        explained = pca.explained_variance_ratio_.cumsum()[-1]
        col_names = [f"PC{i+1}" for i in range(n_components)]
        df = pd.DataFrame(transformed, columns=col_names)
        print(f"  • PCA: {n_cols} → {n_components} principal components")
        print(f"  • Cumulative explained variance: {explained:.2%}")
    else:
        # Column selection by variance when already compact
        variances = df.var().sort_values(ascending=False)
        keep_n = max(3, n_cols // 2)
        top_cols = variances.head(keep_n).index.tolist()
        dropped = [c for c in df.columns if c not in top_cols]
        df = df[top_cols]
        if dropped:
            print(f"  • Column selection: kept top {keep_n} features by variance")
            print(f"  • Dropped low-variance features: {dropped}")
        else:
            print(f"  • Kept all {n_cols} columns (already compact)")

    print(f"  → Shape after reduction: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# STAGE 4 — Discretization
# ─────────────────────────────────────────────────────────────
def discretize(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[preprocess | STAGE 4] Discretization")
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not num_cols:
        print("  • No numeric columns available for binning")
        return df

    # Bin 1 — Quartile bins on first feature (e.g. PC1 or top-variance col)
    col1 = num_cols[0]
    label1 = f"{col1}_quartile"
    try:
        df[label1] = pd.qcut(
            df[col1], q=4,
            labels=["Low", "Med-Low", "Med-High", "High"],
            duplicates="drop"
        )
        print(f"  • Quartile-binned '{col1}' → '{label1}'")
        print(f"    {df[label1].value_counts().to_dict()}")
    except Exception as e:
        print(f"  [WARN] qcut failed on '{col1}': {e}")

    # Bin 2 — Equal-width bins on second feature
    if len(num_cols) >= 2:
        col2 = num_cols[1]
        label2 = f"{col2}_level"
        try:
            df[label2] = pd.cut(
                df[col2], bins=3,
                labels=["Low", "Medium", "High"],
                duplicates="drop"
            )
            print(f"  • Equal-width-binned '{col2}' → '{label2}'")
        except Exception as e:
            print(f"  [WARN] cut failed on '{col2}': {e}")

    print(f"  → Shape after discretization: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python preprocess.py <data_raw.csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"[ERROR] File not found: {input_path}")
        sys.exit(1)

    print(f"[preprocess] Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"[preprocess] Input shape: {df.shape}")

    df = clean_data(df)
    df = transform_features(df)
    df = reduce_dimensions(df)
    df = discretize(df)

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\n[preprocess] ✓ Saved → {OUTPUT_PATH}  (shape: {df.shape})")

    print("\n[preprocess] Calling analytics.py ...")
    result = subprocess.run(
        [sys.executable, "analytics.py", OUTPUT_PATH],
        check=True
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
