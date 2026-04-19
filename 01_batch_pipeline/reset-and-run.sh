#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "[reset] spark-warehouse, metastore_db, pipeline-storage 초기화..."
rm -rf spark-warehouse metastore_db pipeline-storage
mkdir -p pipeline-storage

echo "[run] spark-pipelines run..."
exec spark-pipelines run "$@"
