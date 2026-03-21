# ⏳ SkillDecay — Skills Shelf Life in the AI Era

> How long before your tech stack becomes obsolete? SkillDecay scores every tech skill by its remaining shelf life using real job market data, and visualises the result as a Career Doomsday Clock.

**Live dashboard →** https://skill-decay.streamlit.app*  
**Dataset →** [asaniczka/1-3m-linkedin-jobs-and-skills-2024](https://www.kaggle.com/datasets/asaniczka/1-3m-linkedin-jobs-and-skills-2024)

---

## What it does

SkillDecay ingests 10,000 LinkedIn job postings, extracts tech skill demand signals, and scores each skill with:

- **Shelf life in months** — how long the skill is likely to remain in demand
- **Doomsday clock %** — how close the skill is to obsolescence (0% = safe, 100% = extinct)
- **Risk tier** — low / medium / high / critical
- **Confidence flag** — based on job posting volume (high ≥ 30 jobs, medium ≥ 10, low < 10)

Results are served through an interactive Streamlit dashboard with four views: portfolio doomsday clock, skill search, leaderboard, and data quality panel.

---

## Pipeline architecture

```
Raw CSVs (Kaggle)
      │
      ▼
┌─────────────┐     DAG 1 — ingest_bronze
│   BRONZE    │     Reads raw CSV → adds audit columns
│  Parquet    │     (_ingested_at, _source, _dag_run_id)
└─────────────┘
      │
      ▼
┌─────────────┐     DAG 2 — transform_silver
│   SILVER    │     Tech filter (title keywords + soft skill exclusion)
│  Parquet    │     INNER JOIN with skills → explode to one row per skill
└─────────────┘     Config-driven via include/config/tech_keywords.json
      │
      ▼
┌─────────────┐     DAG 3 — score_gold
│    GOLD     │     Skill demand count → log+MinMax scaling
│  Parquet    │     → shelf_life_months → doomsday_clock_pct
└─────────────┘     → risk_tier → confidence flag
      │
      ▼
┌─────────────┐
│  Streamlit  │     4-tab interactive dashboard
│  Dashboard  │
└─────────────┘
```

**Orchestration:** Apache Airflow via Astro CLI 1.40.1  
**Dev environment:** GitHub Codespaces

---

## Scoring formula

```python
# Step 1: Count skill demand from job postings
job_count = number of tech job postings mentioning this skill

# Step 2: Log-compress the skew, then MinMax scale to [0, 1]
demand_score = MinMax(log1p(job_count))

# Step 3: Derive shelf life and doomsday clock
shelf_life_months = demand_score * 120          # 0–10 year range
doomsday_clock_pct = 100 - (demand_score * 100) # inverse: higher demand = safer

# Step 4: Assign risk tier
risk_tier = "low"      if doomsday_clock_pct < 25
          = "medium"   if doomsday_clock_pct < 50
          = "high"     if doomsday_clock_pct < 75
          = "critical" if doomsday_clock_pct >= 75

# Step 5: Confidence flag
confidence = "high"   if job_count >= 30
           = "medium" if job_count >= 10
           = "low"    if job_count < 10
```

**Why log+MinMax?** Raw job counts are heavily skewed — Python appears 10× more than niche languages. Log compression prevents the top 5 skills from dominating the scale. MinMax then forces the result into [0, 1] for consistent scoring across all skills.

---

## Top skills from Gold layer

| Skill | Jobs | Shelf Life | Doomsday % | Risk |
|-------|------|-----------|------------|------|
| python | 153 | 120 months | 0% | 🟢 low |
| aws | 110 | 111 months | 7.5% | 🟢 low |
| kubernetes | 110 | 111 months | 7.5% | 🟢 low |
| java | 86 | 104 months | 13.2% | 🟢 low |
| linux | 84 | 103 months | 13.7% | 🟢 low |
| sql | 84 | 103 months | 13.7% | 🟢 low |
| docker | 72 | 99 months | 17.2% | 🟢 low |
| devops | 63 | 95 months | 20.2% | 🟢 low |

*AI/ML skills (pytorch, deep learning) present but low-confidence — fewer than 10 postings in the 10K sample.*

---

## Stack and design decisions

### Why Airflow (Astro CLI), not a script?
A single Python script would work for 10K rows. Airflow demonstrates production-grade orchestration — retries, dependency management, XCom for task handoff, and a scheduler. The same DAGs scale to the full 1.3M row dataset by changing only the source path. Astro CLI wraps Docker Compose automatically, avoiding hours of provider debugging.

### Why pandas, not PySpark?
10K rows fits comfortably in memory. PySpark local mode adds JVM startup overhead with no benefit at this scale. The architecture is designed to swap pandas for PySpark at the Silver and Gold layers for the full 1.3M dataset — only the `pd.read_parquet` / `pd.DataFrame` calls change.

### Why Parquet, not Delta Lake?
Parquet is sufficient for a static, batch-processed dataset. Delta Lake adds ACID transactions and time-travel, which are valuable when multiple DAGs write concurrently or when you need to roll back a bad pipeline run. Noted as a v2 upgrade path alongside PySpark.

### Why INNER JOIN in Silver layer?
Job postings without matched skills can't contribute to skill scoring — they'd only inflate the denominator. INNER JOIN keeps the pipeline honest: every Silver row represents a confirmed (job, skill) pair.

### Why log+MinMax, not StandardScaler?
StandardScaler produces negative values and assumes a normal distribution — neither is appropriate for job counts. Log+MinMax gives a strictly positive [0, 1] score with compressed skew. Interpretable: 1.0 = most in-demand skill in the dataset, 0.0 = least.

### Why a config file for tech keywords?
`include/config/tech_keywords.json` is loaded by DAG 2, not hardcoded. This means the tech filter can be updated without touching DAG code — a data engineering best practice that makes the pipeline maintainable and testable independently of orchestration logic.

---

## Project structure

```
skill-decay/
├── dags/
│   ├── dag_01_ingest_bronze.py       # Raw CSV → Bronze Parquet
│   ├── dag_02_transform_silver.py    # Bronze → Silver (tech filter + join)
│   └── dag_03_score_gold.py          # Silver → Gold (scoring + confidence)
├── include/
│   ├── data/
│   │   ├── raw/                      # Source CSVs
│   │   ├── bronze/                   # job_postings parquet (10,000 rows)
│   │   ├── silver/                   # job_skills_exploded parquet (~30K rows)
│   │   └── gold/                     # skill_scores parquet (10,571 skills scored)
│   └── config/
│       └── tech_keywords.json        # Tech title filter + soft skill exclusion list
├── streamlit_app/
│   └── app.py                        # 4-tab Streamlit dashboard
├── scripts/
│   ├── create_sample.py              # Samples 10K rows from full dataset
│   └── explore_so_survey.py          # SO survey skill explorer (future enrichment)
├── requirements.txt
└── README.md
```

**Note on `include/` vs `data/`:** Astro CLI mounts only the `include/` directory into the Airflow Docker container. All data files live under `include/data/` so DAGs can read and write them. A top-level `data/` folder would be invisible to DAG tasks.

---

## How to run

### Prerequisites
- GitHub Codespaces (or Docker locally)
- Astro CLI 1.40.1+
- Kaggle account + API token (`~/.kaggle/kaggle.json`)

### Setup

```bash
# Clone the repo
git clone https://github.com/nbkju9b/skill-decay
cd skill-decay

# Start Airflow
astro dev start

# Access Airflow UI
# https://localhost:8080  (local) or forwarded Codespace port
# Login: admin / admin
```

### Run the pipeline

```bash
# In Airflow UI, trigger DAGs in order:
# 1. dag_01_ingest_bronze
# 2. dag_02_transform_silver
# 3. dag_03_score_gold

# Or via CLI:
astro dev run dags trigger dag_01_ingest_bronze
```

### Run the dashboard

```bash
pip install streamlit plotly --break-system-packages
streamlit run streamlit_app/app.py
```

---

## Gold layer stats

| Metric | Value |
|--------|-------|
| Total skills scored | 10,571 |
| Unique skills | 10,601 |
| Risk: critical | 1,484 |
| Risk: high | 588 |
| Risk: medium | 190 |
| Risk: low | 50 |
| Gold parquet size | 198.5 KB |

---

## Future work (v2)

- [ ] **Scale to full 1.3M dataset** — swap pandas → PySpark, Parquet → Delta Lake
- [ ] **DAG 4 — Great Expectations** data quality checks as a separate DAG
- [ ] **MLflow experiment tracking** — log scoring weights and metrics per pipeline run
- [ ] **SO Developer Survey enrichment** — fuzzy-join developer adoption signal (supply side) against job posting demand (demand side) to identify leading-indicator skills
- [ ] **Streamlit Community Cloud deployment** — shareable public URL
- [ ] **Time-series scoring** — re-run pipeline monthly to track shelf life movement over time

---

## Author

**Arti Awasthi** — Senior Engineering Leader, 21 years experience  
Ex-Arcesium (D.E. Shaw) · Wells Fargo · Visa · JPMorgan · Credit Suisse · Bank of America  
GitHub: [github.com/nbkju9b](https://github.com/nbkju9b) · LinkedIn: [linkedin.com/in/arti-awasthi](https://linkedin.com/in/arti-awasthi)  
Founder · [Fast Tech Academy](https://fasttechacademy.com)

---

*Built as part of an AI/ML engineering portfolio targeting VP Engineering and Data Engineering roles at quantitative finance and top-tier tech firms.*