"""
DAG 3: score_gold
Reads Silver Parquet → counts skill demand → scores shelf life → writes Gold.
Three tasks: validate_silver → compute_scores → write_gold
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

SILVER_DIR = Path("/usr/local/airflow/include/data/silver")
GOLD_DIR = Path("/usr/local/airflow/include/data/gold")


@dag(
    dag_id="dag_03_score_gold",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["skilldecay", "gold"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def score_gold():

    @task()
    def validate_silver():
        """
        Task 1: Confirm Silver Parquet exists and has data.
        Fail fast — same pattern as DAG 1 and DAG 2.
        """
        import pandas as pd

        path = SILVER_DIR / "job_skills_exploded" / "part-00001.parquet"

        if not path.exists():
            raise FileNotFoundError(f"Silver file missing: {path}")

        df = pd.read_parquet(path)

        if len(df) == 0:
            raise ValueError("Silver file is empty")

        print(f"[validate_silver] ✓ {len(df):,} rows")
        print(f"[validate_silver] Unique skills: {df['skill'].nunique():,}")
        return len(df)

    @task()
    def compute_scores(row_count: int):
        """
        Task 2: Count skill demand, apply log+MinMax scaling, compute shelf-life.
        """
        import pandas as pd
        import numpy as np

        print(f"[compute_scores] Processing {row_count:,} silver rows")

        path = SILVER_DIR / "job_skills_exploded" / "part-00001.parquet"
        df = pd.read_parquet(path)

        # Step 1: Count occurrences per skill
        skill_counts = (
            df.groupby("skill")
            .agg(
                job_count=("job_link", "count"),
                top_job_title=("job_title", lambda x: x.value_counts().index[0])
            )
            .reset_index()
        )
        print(f"[compute_scores] Unique skills to score: {len(skill_counts):,}")

        # Step 2: Log scaling
        skill_counts["log_count"] = np.log1p(skill_counts["job_count"])

        # Step 3: Min-Max scaling → demand_score 0 to 1
        min_log = skill_counts["log_count"].min()
        max_log = skill_counts["log_count"].max()
        skill_counts["demand_score"] = (
            (skill_counts["log_count"] - min_log) /
            (max_log - min_log)
        )

        # Step 4: Shelf-life and Doomsday Clock
        skill_counts["shelf_life_months"] = (
            skill_counts["demand_score"] * 120
        ).round(1)

        skill_counts["doomsday_clock_pct"] = (
            100 - (skill_counts["shelf_life_months"] / 120 * 100)
        ).round(1)

        # Step 5: Risk tier
        skill_counts["risk_tier"] = pd.cut(
            skill_counts["shelf_life_months"],
            bins=[0, 24, 48, 72, 120],
            labels=["critical", "high", "medium", "low"]
        )

        # Step 6: Confidence flag — based on job count
        # MIN_COUNT thresholds derived from data analysis:
        # < 10 jobs = unreliable signal (single team/company skew)
        # 10-29 jobs = medium confidence
        # 30+ jobs = high confidence (statistically meaningful)
        skill_counts["confidence"] = skill_counts["job_count"].apply(
            lambda x: "high" if x >= 30 else "medium" if x >= 10 else "low"
        )

        # Log confidence distribution
        print(f"[compute_scores] Confidence distribution:")
        print(skill_counts["confidence"].value_counts())

        print(f"[compute_scores] Sample scores:")
        print(
            skill_counts
            .sort_values("demand_score", ascending=False)
            .head(10)
            [["skill", "job_count", "demand_score", "shelf_life_months", "doomsday_clock_pct", "risk_tier", "confidence"]]
            .to_string(index=False)
        )

        staging_path = GOLD_DIR.parent / "gold_staged.csv"
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        skill_counts.to_csv(staging_path, index=False)
        return str(staging_path)

    @task()
    def write_gold(staged_path: str):
        """
        Task 3: Write scored skills to Gold Parquet.
        """
        import pandas as pd

        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        gold_path = GOLD_DIR / "skill_scores"
        gold_path.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(staged_path)

        output_file = gold_path / "part-00001.parquet"
        df.to_parquet(output_file, index=False)

        size_kb = output_file.stat().st_size / 1024
        print(f"[write_gold] ✓ {len(df):,} skills scored → {output_file} ({size_kb:.1f} KB)")
        print(f"[write_gold] Risk distribution:")
        print(df["risk_tier"].value_counts())
        return str(output_file)
    
    

    # Wire all 3 tasks — inside the function
    row_count = validate_silver()
    staged = compute_scores(row_count)
    write_gold(staged)


score_gold()