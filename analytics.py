"""
analytics.py — Stage 3: Statistical Analytics & Insights
CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026

Dataset: ILO Child Labour + World Bank Poverty
Generates three textual insights saved to insight1.txt, insight2.txt, insight3.txt.
"""

import sys
import os
import subprocess
import pandas as pd
import numpy as np
from scipy import stats


def insight_1_descriptive(df: pd.DataFrame) -> str:
    numeric_df = df.select_dtypes(include=[np.number])
    desc = numeric_df.describe().T

    lines = [
        "=" * 65,
        "INSIGHT 1 — Descriptive Statistics of Preprocessed Features",
        "=" * 65,
        "",
        f"Dataset shape  : {df.shape[0]} rows × {df.shape[1]} columns",
        f"Numeric cols   : {len(numeric_df.columns)}",
        "",
        f"{'Feature':<22} {'Mean':>9} {'Std':>9} {'Min':>9} {'Max':>9}",
        "-" * 62,
    ]
    for col in desc.index:
        lines.append(
            f"{col:<22} {desc.loc[col,'mean']:>9.4f} {desc.loc[col,'std']:>9.4f}"
            f" {desc.loc[col,'min']:>9.4f} {desc.loc[col,'max']:>9.4f}"
        )

    lines += [
        "",
        "Key Observations:",
        f"  • Highest mean   : {desc['mean'].idxmax()} ({desc['mean'].max():.4f})",
        f"  • Lowest mean    : {desc['mean'].idxmin()} ({desc['mean'].min():.4f})",
        f"  • Highest spread : {desc['std'].idxmax()} (std={desc['std'].max():.4f})",
        "",
        "Context — Child Labour & Poverty:",
        "  After PCA/column-selection and StandardScaler preprocessing,",
        "  means near zero and unit standard deviations are expected.",
        "  Features with higher residual spread indicate structural",
        "  inequality patterns across countries and years — these are",
        "  the most informative dimensions for clustering child-labour",
        "  risk into geographic and economic tiers.",
    ]
    return "\n".join(lines)


def insight_2_correlation(df: pd.DataFrame) -> str:
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.shape[1] < 2:
        return "INSIGHT 2: Insufficient numeric features for correlation analysis."

    corr = numeric_df.corr()
    cols = corr.columns.tolist()
    pairs = [(cols[i], cols[j], corr.iloc[i, j])
             for i in range(len(cols)) for j in range(i+1, len(cols))]
    pairs.sort(key=lambda x: abs(x[2]), reverse=True)

    strong_pos = [(a, b, r) for a, b, r in pairs if r >= 0.7]
    strong_neg = [(a, b, r) for a, b, r in pairs if r <= -0.7]

    lines = [
        "=" * 65,
        "INSIGHT 2 — Pairwise Feature Correlation Analysis",
        "=" * 65,
        "",
        f"Features analysed : {len(cols)}",
        f"Pairs evaluated   : {len(pairs)}",
        f"Strong positive (r ≥ 0.70) : {len(strong_pos)}",
        f"Strong negative (r ≤ -0.70): {len(strong_neg)}",
        "",
        f"Top 10 Correlated Pairs:",
        f"  {'Feature A':<18} {'Feature B':<18} {'r':>8}",
        "  " + "-" * 48,
    ]
    for a, b, r in pairs[:10]:
        tag = " ← STRONG" if abs(r) >= 0.7 else ""
        lines.append(f"  {a:<18} {b:<18} {r:>8.4f}{tag}")

    lines += [
        "",
        "Context — Child Labour & Poverty:",
        "  In child-labour datasets, strong positive correlations often appear",
        "  between school non-attendance rates and poverty headcount indices,",
        "  reflecting the economic drivers of child labour.",
        "  Strong negative correlations between gender-disaggregated features",
        "  (male vs female) highlight gender gaps in access to education.",
        "  PCA in preprocessing already reduced multicollinearity by projecting",
        "  correlated raw features onto orthogonal principal components.",
    ]
    if strong_pos:
        a, b, r = strong_pos[0]
        lines.append(f"\n  Strongest positive: '{a}' & '{b}' (r = {r:.4f})")
    if strong_neg:
        a, b, r = strong_neg[0]
        lines.append(f"  Strongest negative: '{a}' & '{b}' (r = {r:.4f})")

    return "\n".join(lines)


def insight_3_distribution(df: pd.DataFrame) -> str:
    numeric_df = df.select_dtypes(include=[np.number])
    cols = numeric_df.columns.tolist()

    lines = [
        "=" * 65,
        "INSIGHT 3 — Distribution Shape & Normality Analysis",
        "=" * 65,
        "",
        "Skewness  > +1.0 → right-skewed (long right tail)",
        "Skewness  < -1.0 → left-skewed  (long left tail)",
        "Shapiro-Wilk p < 0.05 → evidence against normality",
        "",
        f"{'Feature':<20} {'Skew':>8} {'Kurt':>8} {'SW p':>10} {'Normal?':>8}",
        "-" * 58,
    ]

    right_skewed, left_skewed, approx_normal = [], [], []
    non_normal_count = 0

    for col in cols:
        s = numeric_df[col].dropna()
        skew = float(s.skew())
        kurt = float(s.kurtosis())

        if len(s) >= 3:
            _, pval = stats.shapiro(s.sample(min(len(s), 5000), random_state=42))
            normal = "Yes" if pval >= 0.05 else "No"
            if pval < 0.05:
                non_normal_count += 1
        else:
            pval, normal = float("nan"), "N/A"

        lines.append(f"{col:<20} {skew:>8.3f} {kurt:>8.3f} {pval:>10.4f} {normal:>8}")

        if skew > 1.0:
            right_skewed.append(col)
        elif skew < -1.0:
            left_skewed.append(col)
        else:
            approx_normal.append(col)

    lines += [
        "",
        "Summary:",
        f"  Right-skewed features : {len(right_skewed)} → {right_skewed}",
        f"  Left-skewed features  : {len(left_skewed)} → {left_skewed}",
        f"  Approx. symmetric     : {len(approx_normal)}",
        f"  Non-normal (SW test)  : {non_normal_count} / {len(cols)}",
        "",
        "Context — Child Labour & Poverty:",
        "  Child-labour rates are typically right-skewed: most countries",
        "  have low-to-moderate rates while a small subset of high-burden",
        "  nations pull the distribution right. This skew is well-known",
        "  in ILO reporting and motivates log-transformation before",
        "  regression or distance-based clustering. Non-normality in the",
        "  poverty features similarly reflects global inequality — a",
        "  handful of countries account for disproportionate poverty.",
    ]
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python analytics.py <data_preprocessed.csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"[ERROR] File not found: {input_path}")
        sys.exit(1)

    print(f"[analytics] Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"[analytics] Shape: {df.shape}")

    for fname, fn in [
        ("insight1.txt", insight_1_descriptive),
        ("insight2.txt", insight_2_correlation),
        ("insight3.txt", insight_3_distribution),
    ]:
        content = fn(df)
        with open(fname, "w") as f:
            f.write(content + "\n")
        print(f"[analytics] ✓ Saved → {fname}")

    print("\n[analytics] Calling visualize.py ...")
    result = subprocess.run(
        [sys.executable, "visualize.py", input_path],
        check=True
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
