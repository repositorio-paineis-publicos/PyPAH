#!/usr/bin/env bash
set -e

DB_PATH="/app/db/db.duckdb"
URL_GOLD="https://github.com/monteirogmb/pypah-dataset/releases/download/gold-v1/db.duckdb"

if [ ! -f "$DB_PATH" ]; then
  echo "Gold não encontrado. Baixando db.duckdb..."
  mkdir -p /app/db
  curl -L "$URL_GOLD" -o "$DB_PATH"
  echo "Gold baixado com sucesso!"
else
  echo "Gold já existe, pulando download."
fi

echo "🚀 Iniciando Streamlit..."
exec streamlit run Streamlit/dash_PyPAH.py --server.address=0.0.0.0 --server.port=8501