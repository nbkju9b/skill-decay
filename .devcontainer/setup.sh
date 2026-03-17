#!/usr/bin/env bash

##Installs Kaggle CLI (for downloading datasets), MLflow (experiment tracking), and Streamlit (for dashboards/apps)—all quietly via pip.
##These enable data ingestion, ML lifecycle management, and simple UIs, common in skill-decay ML projects (e.g., modeling skill degradation over time).
set -euo pipefail

echo "=== skill-decay setup ==="

# 1. Astro CLI — wraps Airflow so we don't manage Docker Compose manually
echo "[1/3] Installing Astro CLI..."
curl -sSL https://install.astronomer.io | sudo bash -s -- v1.25.0
astro version

# 2. Java home for PySpark local mode
if [ -z "${JAVA_HOME:-}" ]; then
  export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
fi
echo "[2/3] JAVA_HOME=$JAVA_HOME"

# 3. Kaggle CLI for dataset download
echo "[3/3] Installing Kaggle CLI..."
pip install --quiet kaggle mlflow streamlit

echo "=== Setup complete ==="
echo "Next: run 'astro dev init' then 'astro dev start'"
