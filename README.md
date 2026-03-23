# Child Labour & Poverty Analytics Pipeline
### CSCI461: Introduction to Big Data — Assignment #1 — Spring 2026
#### Nile University — Faculty of Information Technology

---

## Team Members

| Name | ID | Section |
|------|----|---------|
| Member 1 | ID1 | Section X |
| Member 2 | ID2 | Section X |
| Member 3 | ID3 | Section X |
| Member 4 | ID4 | Section X |

> **Replace the placeholders above with your actual names, IDs, and section.**

---

## Dataset

We use **three raw, uncleaned datasets** from ILO and World Bank:

| File | Source | Description |
|------|--------|-------------|
| `CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv` | ILO ILOSTAT | Children in employment **attending** school |
| `CLD_XHAN_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv` | ILO ILOSTAT | Children in employment **not attending** school |
| `4909ea2f-5255-49a2-811e-5583974af6ab_Data.csv` | World Bank | Poverty & inequality indicators by country/year |

> ⚠️ These datasets are **raw** (not cleaned/preprocessed).  
> ⚠️ None of these were used in any lab, ML, NLP, or Data Analysis course.

---

## Project Structure

```
Child-Labour-Poverty-Analytics-Pipeline/
├── Dockerfile
├── ingest.py
├── preprocess.py
├── analytics.py
├── visualize.py
├── cluster.py
├── summary.sh
├── README.md
├── CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv
├── CLD_XHAN_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv
├── 4909ea2f-5255-49a2-811e-5583974af6ab_Data.csv
└── results/
    ├── data_raw.csv
    ├── combined_child_labor.csv
    ├── data_preprocessed.csv
    ├── insight1.txt
    ├── insight2.txt
    ├── insight3.txt
    ├── summary_plot.png
    └── clusters.txt
```

---

## Pipeline Execution Flow

```
  ┌──────────────────────────────────────────────────────────────────┐
  │                     Docker Container                              │
  │                    /app/pipeline/                                 │
  │                                                                   │
  │  ingest.py                                                        │
  │    • Detects source CSVs → merges ILO + World Bank data           │
  │    • Saves combined_child_labor.csv + data_raw.csv                │
  │         │                                                         │
  │         ▼                                                         │
  │  preprocess.py                                                    │
  │    • STAGE 1 Cleaning    : dedup, drop high-null, fill NaN,       │
  │                            strip whitespace, drop constants        │
  │    • STAGE 2 Transform   : label-encode cats, StandardScaler,     │
  │                            drop remaining text cols                │
  │    • STAGE 3 Reduction   : PCA (if >15 cols) or variance select   │
  │    • STAGE 4 Discretize  : quartile bins + equal-width bins       │
  │    • Saves data_preprocessed.csv                                  │
  │         │                                                         │
  │         ▼                                                         │
  │  analytics.py                                                     │
  │    • Insight 1: Descriptive statistics per feature                │
  │    • Insight 2: Pairwise correlation analysis                     │
  │    • Insight 3: Skewness, kurtosis, Shapiro-Wilk normality test   │
  │    • Saves insight1.txt, insight2.txt, insight3.txt               │
  │         │                                                         │
  │         ▼                                                         │
  │  visualize.py                                                     │
  │    • Plot 1: Overlaid feature histograms                          │
  │    • Plot 2: Correlation heatmap (lower triangle)                 │
  │    • Plot 3: Box-plots per feature                                │
  │    • Plot 4: Scatter (top-2 features, colour = 3rd)              │
  │    • Saves summary_plot.png                                       │
  │         │                                                         │
  │         ▼                                                         │
  │  cluster.py                                                       │
  │    • K-Means with optimal k via silhouette score (k=2..8)         │
  │    • Reports: samples/cluster, centroids, Silhouette, DB index    │
  │    • Saves clusters.txt                                           │
  └──────────────────────────────────────────────────────────────────┘
                          │
                    summary.sh (HOST)
                          │
              results/ ← all .csv .txt .png files
```

---

## Docker Commands

### Step 1 — Build the image

```bash
docker build -t child-labour-pipeline .
```

### Step 2 — Run the container

```bash
docker run -it --name cl-run child-labour-pipeline
```

### Step 3 — Run the full pipeline (inside the container)

```bash
python ingest.py CLD_XHAS_SEX_AGE_GEO_NB_A-filtered-2026-03-19.csv
```

This single command runs the **entire pipeline** end-to-end:
`ingest.py → preprocess.py → analytics.py → visualize.py → cluster.py`

### Step 4 — Export results to host & remove container

Run this **on your host machine** (not inside the container):

```bash
bash summary.sh cl-run
```

---

## Script Descriptions

### `ingest.py`
- Accepts a dataset path via `sys.argv[1]`
- Auto-detects the three source CSVs and merges them:
  - Concatenates attending/not-attending ILO records
  - Adds a `school_status` column
  - Inner-joins with World Bank poverty data on `(country, year)`
- Saves `combined_child_labor.csv` and `data_raw.csv`

### `preprocess.py`
| Stage | Task | Detail |
|-------|------|--------|
| Cleaning | Remove duplicates | `drop_duplicates()` |
| Cleaning | Drop high-null columns | Drop cols with >60% missing |
| Cleaning | Fill NaN | numeric → median, categorical → mode |
| Cleaning | Strip whitespace | All string columns |
| Cleaning | Drop constant columns | Zero-variance features removed |
| Transform | Label-encode | Low-cardinality (≤25 unique) categoricals |
| Transform | StandardScaler | All numeric columns |
| Transform | Drop text columns | Any remaining non-numeric columns |
| Reduction | PCA or column select | PCA if >15 cols; variance selection otherwise |
| Discretize | Quartile binning | `pd.qcut` → 4 bins (Low/Med-Low/Med-High/High) |
| Discretize | Equal-width binning | `pd.cut` → 3 bins (Low/Medium/High) |

### `analytics.py`
- **insight1.txt** — Descriptive stats table (mean, std, min, max per feature)
- **insight2.txt** — Pairwise Pearson correlation; flags |r| ≥ 0.7 pairs
- **insight3.txt** — Skewness, kurtosis, Shapiro-Wilk normality test

### `visualize.py`
Dark-themed 2×2 figure (18×14 inches, 150 DPI):
- Feature distribution histograms
- Lower-triangle correlation heatmap
- Box-plots per feature
- Scatter plot (top-2 principal components, coloured by 3rd)

### `cluster.py`
- Tests k = 2..8 via silhouette score → picks optimal k
- Fits final K-Means model
- Outputs cluster sizes, centroids, silhouette score, Davies-Bouldin index
- Interprets clusters in child-labour domain context

### `summary.sh`
```bash
bash summary.sh [container_name]   # default: cl-run
```
- Copies all `.csv`, `.txt`, `.png` files to `results/`
- Stops and removes the container

---

## Bonus

### Docker Hub
```bash
docker tag child-labour-pipeline <dockerhub-username>/child-labour-pipeline:latest
docker push <dockerhub-username>/child-labour-pipeline:latest
```

### GitHub
```bash
git init
git add .
git commit -m "CSCI461 Big Data Assignment 1 — Child Labour Pipeline"
git remote add origin https://github.com/<username>/Child-Labour-Poverty-Analytics-Pipeline.git
git push -u origin main
```

---

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| pandas | latest | Data loading, merging, CSV I/O |
| numpy | latest | Numerical operations |
| matplotlib | latest | Plot rendering |
| seaborn | latest | Statistical visualisations |
| scikit-learn | latest | Preprocessing, PCA, K-Means |
| scipy | latest | Shapiro-Wilk test |
| requests | latest | (Available for API data fetching) |

---

*Nile University — CSCI461: Introduction to Big Data — Spring 2026*
