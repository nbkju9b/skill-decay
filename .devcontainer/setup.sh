#!/usr/bin/env bash



set -euo pipefail

echo "=== skill-decay setup ==="

echo "[1/3] Installing Astro CLI..."
curl -L https://github.com/astronomer/astro-cli/releases/download/v1.40.1/astro_1.40.1_linux_amd64.tar.gz | tar xz
mv astro /usr/local/bin/
astro version

echo "[2/3] Setting JAVA_HOME for PySpark..."
if [ -z "${JAVA_HOME:-}" ]; then
  export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
fi
echo "JAVA_HOME=$JAVA_HOME"

echo "[3/3] Installing Python deps..."
pip install --quiet kaggle mlflow streamlit

echo "=== Setup complete ==="