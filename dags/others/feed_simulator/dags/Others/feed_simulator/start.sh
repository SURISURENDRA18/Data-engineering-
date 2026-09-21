#!/bin/bash

set -e

LOG_DIR="/app/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/pipeline_$TIMESTAMP.log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "========================================"
echo "Feed simulator started at: $(date)"
echo "Log file: $LOG_FILE"
echo "========================================"

# ----------------------------
# Dynamic Inputs
# ----------------------------

RUN_DATE=${RUN_DATE:-$(date +%Y%m%d)}
LIMIT=${LIMIT:-5000}

echo "[CONFIG] RUN_DATE = $RUN_DATE"
echo "[CONFIG] LIMIT    = $LIMIT"

# ----------------------------
# Step 1
# ----------------------------
echo "[1] Running KYC_feed_generator.py"
python KYC_feed_generator.py "$RUN_DATE" "$LIMIT"

# ----------------------------
# Step 2
# ----------------------------
echo "[2] Running usage_cdr_simulator.py"
python usage_cdr_simulator.py "$RUN_DATE" "$LIMIT"

echo "========================================"
echo "Feed simulator completed at: $(date)"
echo "========================================"