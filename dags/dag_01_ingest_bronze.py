"""
DAG 1: ingest_bronze
Reads raw CSV samples → adds audit columns → writes to Bronze as Parquet.
Three tasks in sequence: validate → audit → write.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

RAW_DIR = Path("/usr/local/airflow/include/data/raw")
BRONZE_DIR = Path("/usr/local/airflow/include/data/bronze")


@dag(
    dag_id="dag_01_ingest_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["skilldecay", "bronze"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def ingest_bronze():

    @task()
    def validate_source():
        """
        Task 1: Validate raw files exist and have data.
        Fail fast here rather than discovering missing files
        halfway through the pipeline.
        Returns row count for downstream logging.
        """
        import pandas as pd

        path = RAW_DIR / "linkedin_job_postings_sample.csv"

        if not path.exists():
            raise FileNotFoundError(f"Source file missing: {path}")

        df = pd.read_csv(path)

        if len(df) == 0:
            raise ValueError(f"Source file is empty: {path}")

        print(f"[validate_source] ✓ {len(df):,} rows found at {path}")
        print(f"[validate_source] Columns: {df.columns.tolist()}")
        return len(df)

    @task()
    def add_audit_columns(row_count: int):
        """
        Task 2: Add audit columns, save to staging path.
        Receives row_count from Task 1 — confirms validation passed.
        Returns staging file path (not DataFrame) — correct XCom pattern.
        """
        import pandas as pd
        from datetime import timezone

        print(f"[add_audit_cols] Processing {row_count:,} validated rows")

        path = RAW_DIR / "linkedin_job_postings_sample.csv"
        df = pd.read_csv(path)

        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source"] = "kaggle/asaniczka/1-3m-linkedin-jobs-and-skills-2024"
        df["_dag_run_id"] = "manual"

        staging_path = RAW_DIR / "linkedin_postings_staged.csv"
        df.to_csv(staging_path, index=False)
        print(f"[add_audit_cols] Staged {len(df):,} rows → {staging_path}")
        return str(staging_path)

    @task()
    def write_bronze(staged_path: str):
        """
        Task 3: Read staged CSV, write to Bronze as Parquet.
        Parquet for dev speed — swap to Delta Lake for production.
        """
        import pandas as pd

        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        bronze_path = BRONZE_DIR / "job_postings"
        bronze_path.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(staged_path)

        output_file = bronze_path / "part-00001.parquet"
        df.to_parquet(output_file, index=False)

        size_kb = output_file.stat().st_size / 1024
        print(f"[write_bronze] ✓ {len(df):,} rows → {output_file} ({size_kb:.1f} KB)")
        return str(output_file)

    # Wire tasks — output of each flows into the next
    row_count = validate_source()
    staged = add_audit_columns(row_count)
    write_bronze(staged)


ingest_bronze()