"""
visualize.py — Stage 4: Data Visualization
CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026

Dataset: ILO Child Labour + World Bank Poverty
Creates four plots in a 2×2 grid and saves as summary_plot.png.
Then calls cluster.py.
"""

import sys
import os
import subprocess
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

OUTPUT_PNG = "summary_plot.png"

BG      = "#0d1117"
PANEL   = "#161b22"
BORDER  = "#30363d"
TEXT    = "#e6edf3"
ACCENT  = "#58a6ff"
WARM    = "#f78166"
PALETTE = "mako"


def setup_ax(ax, title):
    ax.set_facecolor(PANEL)
    ax.tick_params(colors=TEXT, labelsize=8)
    ax.xaxis.label.set_color(TEXT)
    ax.yaxis.label.set_color(TEXT)
    ax.set_title(title, color=TEXT, fontsize=11, fontweight="bold", pad=8)
    ax.spines[["top","right"]].set_visible(False)
    for sp in ["left","bottom"]:
        ax.spines[sp].set_color(BORDER)
    ax.grid(True, color=BORDER, linewidth=0.5, alpha=0.7)


def build_plots(df: pd.DataFrame):
    numeric_df = df.select_dtypes(include=[np.number])
    num_cols = numeric_df.columns.tolist()

    plt.rcParams.update({
        "figure.facecolor": BG, "axes.facecolor": PANEL,
        "text.color": TEXT, "font.family": "DejaVu Sans",
    })

    fig = plt.figure(figsize=(18, 14), facecolor=BG)
    fig.suptitle(
        "Child Labour & Poverty Analytics — Preprocessed Feature Overview",
        fontsize=16, fontweight="bold", color=TEXT, y=0.98
    )
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.42, wspace=0.38,
                           left=0.07, right=0.97, top=0.93, bottom=0.06)

    # ── Plot 1: Overlaid histograms ───────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    colors = sns.color_palette(PALETTE, min(5, len(num_cols)))
    for i, col in enumerate(num_cols[:5]):
        ax1.hist(numeric_df[col].dropna(), bins=35, alpha=0.55,
                 color=colors[i], label=col, edgecolor="none")
    setup_ax(ax1, "Feature Distributions (Scaled)")
    ax1.set_xlabel("Scaled Value")
    ax1.set_ylabel("Count")
    ax1.legend(fontsize=7, framealpha=0.25, facecolor=PANEL, labelcolor=TEXT)

    # ── Plot 2: Correlation heatmap ───────────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    corr_cols = num_cols[:min(10, len(num_cols))]
    corr_m = numeric_df[corr_cols].corr()
    mask = np.triu(np.ones_like(corr_m, dtype=bool))
    im = sns.heatmap(
        corr_m, mask=mask, ax=ax2, cmap="RdBu_r",
        center=0, vmin=-1, vmax=1,
        annot=(len(corr_cols) <= 8), fmt=".2f",
        annot_kws={"size": 7, "color": TEXT},
        linewidths=0.4, linecolor=BG,
        cbar_kws={"shrink": 0.8}
    )
    ax2.set_title("Feature Correlation Heatmap", color=TEXT, fontsize=11,
                  fontweight="bold", pad=8)
    ax2.tick_params(axis="x", rotation=45, labelsize=7, colors=TEXT)
    ax2.tick_params(axis="y", rotation=0,  labelsize=7, colors=TEXT)
    cbar = ax2.collections[0].colorbar
    cbar.ax.yaxis.set_tick_params(color=TEXT, labelcolor=TEXT)

    # ── Plot 3: Box-plots ─────────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[1, 0])
    box_cols = num_cols[:min(8, len(num_cols))]
    data_bp = [numeric_df[c].dropna().values for c in box_cols]
    pal = sns.color_palette(PALETTE, len(box_cols))
    bp = ax3.boxplot(
        data_bp, patch_artist=True, widths=0.5,
        medianprops=dict(color="#ffffff", linewidth=2),
        whiskerprops=dict(color=TEXT, linewidth=1),
        capprops=dict(color=TEXT, linewidth=1),
        flierprops=dict(marker="o", markersize=2, alpha=0.3, markeredgewidth=0)
    )
    for patch, c in zip(bp["boxes"], pal):
        patch.set_facecolor(c); patch.set_alpha(0.7)
    ax3.set_xticks(range(1, len(box_cols)+1))
    ax3.set_xticklabels(box_cols, rotation=35, ha="right", fontsize=8, color=TEXT)
    setup_ax(ax3, "Feature Box-Plots (Scaled)")
    ax3.set_ylabel("Scaled Value")

    # ── Plot 4: Scatter top-2 features coloured by 3rd ───────────────────
    ax4 = fig.add_subplot(gs[1, 1])
    if len(num_cols) >= 2:
        xc, yc = num_cols[0], num_cols[1]
        if len(num_cols) >= 3:
            cv = numeric_df[num_cols[2]]
            sc = ax4.scatter(numeric_df[xc], numeric_df[yc],
                             c=cv, cmap=PALETTE, alpha=0.5, s=10, linewidths=0)
            cb = plt.colorbar(sc, ax=ax4, shrink=0.8)
            cb.set_label(num_cols[2], color=TEXT, fontsize=8)
            cb.ax.yaxis.set_tick_params(color=TEXT, labelcolor=TEXT)
        else:
            ax4.scatter(numeric_df[xc], numeric_df[yc],
                        color=ACCENT, alpha=0.5, s=10, linewidths=0)
        ax4.set_xlabel(xc); ax4.set_ylabel(yc)
        setup_ax(ax4, f"Scatter: {xc} vs {yc}")
    else:
        ax4.text(0.5, 0.5, "Insufficient features", ha="center", va="center",
                 fontsize=12, color=TEXT)
        ax4.axis("off")

    fig.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight",
                facecolor=BG, edgecolor="none")
    plt.close(fig)
    print(f"[visualize] ✓ Saved → {OUTPUT_PNG}")


def main():
    if len(sys.argv) < 2:
        print("[ERROR] Usage: python visualize.py <data_preprocessed.csv>")
        sys.exit(1)

    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"[ERROR] File not found: {input_path}")
        sys.exit(1)

    print(f"[visualize] Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"[visualize] Shape: {df.shape}")
    build_plots(df)

    print("\n[visualize] Calling cluster.py ...")
    result = subprocess.run(
        [sys.executable, "cluster.py", input_path], check=True
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
