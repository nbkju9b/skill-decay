"""
create_sample.py
Takes the latest 10,000 rows from linkedin_job_postings.csv
and matching rows from job_skills.csv.
Run once: python scripts/create_sample.py
"""
import pandas as pd

print("[1/4] Reading job postings...")
posts = pd.read_csv("data/raw/linkedin_job_postings.csv")
print(f"      Full dataset: {len(posts):,} rows")
print(f"      Columns: {list(posts.columns)}")

print("[2/4] Sampling latest 10,000 by first_seen date...")
posts["first_seen"] = pd.to_datetime(posts["first_seen"], errors="coerce")
sample = (
    posts
    .dropna(subset=["first_seen"])
    .sort_values("first_seen", ascending=False)
    .head(10_000)
)
print(f"      Date range: {sample['first_seen'].min()} → {sample['first_seen'].max()}")
print(f"      Sample size: {len(sample):,} rows")

print("[3/4] Saving sample postings...")
sample.to_csv("data/raw/linkedin_job_postings_sample.csv", index=False)

print("[4/4] Filtering matching job_skills rows...")
skills = pd.read_csv("data/raw/job_skills.csv")
print(f"      Full skills: {len(skills):,} rows")

sample_ids = set(sample["job_link"])
skills_sample = skills[skills["job_link"].isin(sample_ids)]
print(f"      Matched skills: {len(skills_sample):,} rows")
skills_sample.to_csv("data/raw/job_skills_sample.csv", index=False)

print("\n=== Done ===")
print(f"data/raw/linkedin_job_postings_sample.csv — {len(sample):,} rows")
print(f"data/raw/job_skills_sample.csv            — {len(skills_sample):,} rows")
