"""
DAG 2: transform_silver
Reads Bronze Parquet → tech filter → join skills → explode → writes Silver.

Four tasks:
    validate_bronze → join_skills → explode_skills → write_silver

Key design decisions:
    - Two-level tech filter: title keywords + soft skill exclusion
    - Both filter lists loaded from include/config/tech_keywords.json
    - Config file approach: keywords managed outside DAG code
    - INNER JOIN: drops jobs without skills (can't contribute to scoring)
    - Explode: one row per skill enables COUNT() per skill per week
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

BRONZE_DIR = Path("/usr/local/airflow/include/data/bronze")
SILVER_DIR = Path("/usr/local/airflow/include/data/silver")
RAW_DIR = Path("/usr/local/airflow/include/data/raw")
CONFIG_PATH = Path("/usr/local/airflow/include/config/tech_keywords.json")


@dag(
    dag_id="dag_02_transform_silver",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["skilldecay", "silver"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def transform_silver():

    @task()
    def validate_bronze():
        """
        Task 1: Confirm Bronze Parquet exists and has data.
        Fail fast — catch missing files before doing any work.
        Returns row count so Task 2 can log it.
        """
        import pandas as pd

        path = BRONZE_DIR / "job_postings" / "part-00001.parquet"

        if not path.exists():
            raise FileNotFoundError(f"Bronze file missing: {path}")

        df = pd.read_parquet(path)

        if len(df) == 0:
            raise ValueError("Bronze file is empty")

        print(f"[validate_bronze] ✓ {len(df):,} rows found")
        print(f"[validate_bronze] Columns: {df.columns.tolist()}")
        return len(df)

    @task()
    def join_skills(row_count: int):
        """
        Task 2: Apply tech filter then INNER JOIN with job_skills.

        Two-level tech filter:
            Level 1 — filter job_title to tech roles using keyword list
            Level 2 — soft skill exclusion happens in explode_skills (Task 3)

        Why load filter from config file?
            Keywords live outside DAG code. When new roles emerge
            (e.g. 'llm engineer'), update the JSON file — no DAG changes needed.
            Config file is version-controlled so changes are tracked in git.

        Why INNER JOIN not LEFT JOIN?
            Rows without skills can't contribute to shelf-life scoring.
            Keeping NULLs would corrupt downstream counts.
            Drop them here — Silver is clean by design.
        """
        import json
        import pandas as pd

        print(f"[join_skills] Processing {row_count:,} bronze rows")

        # Load filter config from JSON file
        with open(CONFIG_PATH) as f:
            config = json.load(f)

        TECH_TITLE_KEYWORDS = config["tech_title_keywords"]
        print(f"[join_skills] Loaded {len(TECH_TITLE_KEYWORDS)} tech title keywords")

        # Load bronze postings
        bronze_path = BRONZE_DIR / "job_postings" / "part-00001.parquet"
        posts = pd.read_parquet(bronze_path)
        print(f"[join_skills] Bronze rows: {len(posts):,}")

        # ── Level 1: Tech title filter ─────────────────────────────────
        # str.contains with na=False: null job_titles → False (not matched)
        # rather than NaN which would cause downstream errors
        title_pattern = "|".join(TECH_TITLE_KEYWORDS)
        posts = posts[
            posts["job_title"].str.lower().str.contains(title_pattern, na=False)
        ]
        print(f"[join_skills] After tech title filter: {len(posts):,} rows")

        # Load skills
        skills_path = RAW_DIR / "job_skills_sample.csv"
        skills = pd.read_csv(skills_path)
        print(f"[join_skills] Skills rows: {len(skills):,}")

        # ── INNER JOIN on job_link ─────────────────────────────────────
        joined = posts.merge(skills, on="job_link", how="inner")
        print(f"[join_skills] After INNER JOIN: {len(joined):,} rows")
        print(f"[join_skills] Columns: {joined.columns.tolist()}")

        # Save to staging — pass file path through XCom, not DataFrame
        # XCom stores small values in Postgres — file path = 30 bytes,
        # DataFrame = 50MB. Always pass paths not data.
        staging_path = RAW_DIR / "silver_staged.csv"
        joined.to_csv(staging_path, index=False)
        print(f"[join_skills] Staged to {staging_path}")
        return str(staging_path)

    @task()
    def explode_skills(staged_path: str):
        """
        Task 3: Explode comma-separated skills into one row per skill.
        Apply Level 2 soft skill exclusion filter.

        Before: 1 row | job_link=X | job_skills="Python, SQL, Docker"
        After:  3 rows | job_link=X | skill="python"
                        | job_link=X | skill="sql"
                        | job_link=X | skill="docker"

        Why explode?
            To COUNT how many jobs required Python this week, Python
            must be on its own row. You can't COUNT() inside a string.

        Why soft skill exclusion?
            Even tech job postings contain soft skills (communication,
            teamwork). These would dominate the shelf-life scores.
            Exclusion list built from actual data analysis — not guesswork.
        """
        import json
        import pandas as pd

        # Load soft skill exclusion list from config
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        SOFT_SKILLS_TO_EXCLUDE = set(config["soft_skills_to_exclude"])

        df = pd.read_csv(staged_path)
        print(f"[explode_skills] Input: {len(df):,} rows")

        # Step 1: Split comma-separated string into Python list
        df["skill"] = df["job_skills"].str.split(",")

        # Step 2: Explode list into separate rows — one skill per row
        df = df.explode("skill")

        # Step 3: Clean — strip whitespace, lowercase, drop nulls/empty
        df["skill"] = df["skill"].str.strip().str.lower()
        df = df.dropna(subset=["skill"])
        df = df[df["skill"] != ""]

        print(f"[explode_skills] After explode: {len(df):,} rows")

        # ── Level 2: Soft skill exclusion ─────────────────────────────
        df = df[~df["skill"].isin(SOFT_SKILLS_TO_EXCLUDE)]
        print(f"[explode_skills] After soft skill filter: {len(df):,} rows")
        print(f"[explode_skills] Unique skills: {df['skill'].nunique():,}")

        print(f"[explode_skills] Top 10 skills:")
        print(df["skill"].value_counts().head(10))

        # Keep only columns needed for Gold scoring
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
        Silver = clean, tech-filtered, one-row-per-skill, analytics-ready.
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

    # ── Task wiring — output of each flows into the next ──────────────
    row_count = validate_bronze()
    staged = join_skills(row_count)
    exploded = explode_skills(staged)
    write_silver(exploded)


transform_silver()