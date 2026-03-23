"""
cluster.py — Stage 5: K-Means Clustering
CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026

Dataset: ILO Child Labour + World Bank Poverty
Applies K-Means clustering, finds optimal k via silhouette score.
Outputs full report to clusters.txt.
"""

import sys
import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score

OUTPUT_TXT = "clusters.txt"
MAX_FEATURES = 10


def find_optimal_k(X, k_range):
    best_k, best_score, scores = 2, -1, {}
    for k in k_range:
        if k >= X.shape[0]:
            break
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        score = silhouette_score(X, labels)
        scores[k] = score
        if score > best_score:
            best_score = score
            best_k = k
    return best_k, scores


def main():
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python cluster.py <data_preprocessed.csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"[ERROR] File not found: {input_path}")
        sys.exit(1)

    print(f"[cluster] Loading: {input_path}")
    df = pd.read_csv(input_path)

    # Select numeric, non-null columns
    numeric_df = df.select_dtypes(include=[np.number]).dropna(axis=1)
    variances = numeric_df.var().sort_values(ascending=False)
    selected = variances.head(MAX_FEATURES).index.tolist()
    X = numeric_df[selected].values

    print(f"[cluster] Features used: {selected}")
    print(f"[cluster] Matrix shape : {X.shape}")

    # Find optimal k
    k_max = min(9, X.shape[0])
    print(f"[cluster] Evaluating k = 2 .. {k_max-1} via silhouette score ...")
    optimal_k, sil_scores = find_optimal_k(X, range(2, k_max))
    print(f"[cluster] Optimal k = {optimal_k}")

    # Final fit
    kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)

    sil     = silhouette_score(X, labels)
    db      = davies_bouldin_score(X, labels)
    inertia = kmeans.inertia_

    unique, counts = np.unique(labels, return_counts=True)
    cluster_dist   = dict(zip(unique.tolist(), counts.tolist()))
    centroids      = kmeans.cluster_centers_

    # ── Build report ──────────────────────────────────────────────────────
    lines = [
        "=" * 65,
        "K-MEANS CLUSTERING REPORT",
        "Child Labour & Poverty Analytics — CSCI461 Assignment #1",
        "=" * 65,
        "",
        f"Input file     : {input_path}",
        f"Total samples  : {X.shape[0]}",
        f"Features used  : {len(selected)}",
        f"  {selected}",
        "",
        "─" * 65,
        "SILHOUETTE SCORE PER k",
        "─" * 65,
    ]
    for k, s in sorted(sil_scores.items()):
        bar    = "█" * max(1, int(round(s * 35)))
        marker = " ← SELECTED" if k == optimal_k else ""
        lines.append(f"  k={k}  {s:+.4f}  {bar}{marker}")

    lines += [
        "",
        "─" * 65,
        "FINAL MODEL METRICS",
        "─" * 65,
        f"  Optimal clusters (k)   : {optimal_k}",
        f"  Silhouette Score       : {sil:.4f}   (range -1..1, higher=better)",
        f"  Davies-Bouldin Index   : {db:.4f}   (lower=better)",
        f"  Inertia (WCSS)         : {inertia:.4f}",
        "",
        "─" * 65,
        "SAMPLES PER CLUSTER",
        "─" * 65,
    ]
    for cid, cnt in cluster_dist.items():
        pct = 100.0 * cnt / X.shape[0]
        bar = "▌" * max(1, int(round(pct / 2)))
        lines.append(f"  Cluster {cid:>2}  │  {cnt:>6} samples  ({pct:5.1f}%)  {bar}")

    # Centroid table
    disp = selected[:6]
    lines += [
        "",
        "─" * 65,
        "CLUSTER CENTROIDS (first 6 features)",
        "─" * 65,
        "  " + f"{'Cluster':>8}  " + "  ".join(f"{c[:10]:>10}" for c in disp),
        "  " + "-" * 56,
    ]
    for cid, c in enumerate(centroids):
        vals = "  ".join(f"{v:>10.4f}" for v in c[:len(disp)])
        lines.append(f"  {cid:>8}  {vals}")

    lines += [
        "",
        "─" * 65,
        "INTERPRETATION — CHILD LABOUR CONTEXT",
        "─" * 65,
        f"  The dataset was partitioned into {optimal_k} country-year cluster(s).",
        f"  Silhouette = {sil:.4f}: {'well-separated groups.' if sil >= 0.5 else 'moderate separation — may reflect data sparsity after merge.'}",
        f"  Davies-Bouldin = {db:.4f}: {'compact, well-defined clusters.' if db <= 1.0 else 'some cluster overlap; overlapping economic conditions across countries.'}",
        "",
        "  Cluster interpretation (typical for this domain):",
        "  • Low cluster(s)  → countries with low child labour and low poverty",
        "  • High cluster(s) → high child labour burden + high poverty rates",
        "  • Mid cluster(s)  → transitional economies with mixed indicators",
        "  These labels can be used as risk-tier features in further analysis.",
        "",
        "=" * 65,
    ]

    report = "\n".join(lines)
    with open(OUTPUT_TXT, "w") as f:
        f.write(report + "\n")

    print(report)
    print(f"\n[cluster] ✓ Saved → {OUTPUT_TXT}")
    print("[cluster] Pipeline complete ✓")


if __name__ == "__main__":
    main()
