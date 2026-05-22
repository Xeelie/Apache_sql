#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${JAVA_HOME:-}" && -d "/opt/homebrew/opt/openjdk@17" ]]; then
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

if [[ -x "../Apache_spark/.venv/bin/spark-submit" ]]; then
  export PATH="../Apache_spark/.venv/bin:$PATH"
fi

spark-submit --conf "spark.ui.showConsoleProgress=false" covid_spark_analysis.py
