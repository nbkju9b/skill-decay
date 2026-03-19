"""
DAG 2: transform_silver
Reads Bronze Parquet → cleans → joins skills → explodes → writes Silver.
Four tasks: deduplicate → standardise → join_skills → explode_and_write
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

BRONZE_DIR = Path("/usr/local/airflow/include/data/bronze")
SILVER_DIR = Path("/usr/local/airflow/include/data/silver")
RAW_DIR = Path("/usr/local/airflow/include/data/raw")

@dag(
    dag_id="dag_02_transform_silver",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["skilldecay", "silver"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)

def transform_silver():
    @task
    def validate_bronze():
        """
        Task 1: Confirm Bronze Parquet exists and has data.
        Same fail-fast pattern as DAG 1.
        """
        import pandas as pd
        path = BRONZE_DIR
        if not path.exists():
            raise FileNotFoundError(f"Bronze file missing: {path}")
        df = pd.read_parquet(path)

        if(len(df)==0):
            raise ValueError("Bronze file is empty")
        print(f"[validate_bronze] ✓ {len(df):,} rows found")
        print(f"[validate_bronze] Columns: {df.columns.tolist()}")
        return len(df)
    validate_bronze()

    @task()
    def join_skills(row_count: int):
        """
        Task 2: INNER JOIN Bronze postings with job_skills on job_link.
        
        Why INNER JOIN not LEFT JOIN?
        Rows without skills can't contribute to shelf-life scoring.
        Keeping them as NULLs would corrupt downstream counts.
        We drop them here intentionally — Silver is clean by design.
        """
        import pandas as pd
        print(f"[join_skills] Processing {row_count:,} bronze rows")

        # Load bronze postings
        bronze_path = BRONZE_DIR / "job_postings" / "part-00001.parquet"
        posts = pd.read_parquet(bronze_path)

        # Load skills
        skills_path = RAW_DIR / "job_skills_sample.csv"
        skills = pd.read_csv(skills_path)

        print(f"[join_skills] Postings: {len(posts):,} | Skills: {len(skills):,}")

        # INNER JOIN — only keep jobs that have skills data

        joined = posts.merge(skills, on="job_link", how ="inner")

        print(f"[join_skills] After INNER JOIN: {len(joined):,} rows")
        print(f"[join_skills] Columns: {joined.columns.tolist()}")

        # Save to staging
        staging_path = RAW_DIR/"silver_staged_csv"
        joined.to_csv(staging_path, index=False)
        return str(staging_path)
    # Wire tasks
    row_count = validate_bronze()
    join_skills(row_count)

    @task()
    def explode_skills(staged_path: str):
        """
        Task 3: Explode comma-separated skills into one row per skill.
        
        Before: 1 row | job_link=X | job_skills="Python, SQL, Spark"
        After:  3 rows | job_link=X | skill="Python"
                        | job_link=X | skill="SQL"  
                        | job_link=X | skill="Spark"
        Why this matters: To count "how many jobs required Python this week"
        you need Python on its own row. You can't COUNT() inside a string.                
        """ 

        import pandas as pd

        df = pd.read_csv(staged_path)
        print(f"[explode_skills] Input: {len(df):,} rows")

        # Step 1: Split comma-separated string into a Python list
        df["skill"] = df["job_skills"].str.split(",")

        # Step 2: Explode list into separate rows
        df = df.explode("skill")

        # Step 3: Clean up — strip whitespace, lowercase, drop nulls
        df["skill"] = df["skill"].str.strip().str.lower()
        df = df.dropna(subset=["skill"])
        df = df[df["skill"] != ""]

        print(f"[explode_skills] Output: {len(df):,} rows")
        print(f"[explode_skills] Unique skills: {df['skill'].nunique():,}")
        print(f"[explode_skills] Top 10 skills:")
        print(df["skill"].value_counts().head(10))

        # Keep only columns we need for Gold scoring
        silver_df = df[[
            "job_link", "job_title", "company",
            "job_location", "first_seen", "job_level",
            "skill", "_ingested_at", "_source"
        ]]

        # Save staging
        silver_staging = RAW_DIR / "silver_exploded.csv"
        silver_df.to_csv(silver_staging, index=False)
        print(f"[explode_skills] Saved to {silver_staging}")
        return str(silver_staging)

    @task()
    def write_silver(staged_path: str):
        """
        Task 4: Write exploded data to Silver Parquet.
        Silver is the clean, analytics-ready layer.
        """
        import pandas as pd

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        silver_path = SILVER_DIR / "job_skills_exploded"
        silver_path.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(staged_path) 
        output_file = silver_path / "part-00001.parquet"
        df.to_parquet(output_file, index=False)
        size_kb = output_file.stat().st_size / 1024
        print(f"[write_silver] ✓ {len(df):,} rows → {output_file} ({size_kb:.1f} KB)")
        return str(output_file)

     # Wire all 4 tasks in sequence
    row_count = validate_bronze()
    staged = join_skills(row_count)
    exploded = explode_skills(staged)
    write_silver(exploded)              

transform_silver() 
   


        
 