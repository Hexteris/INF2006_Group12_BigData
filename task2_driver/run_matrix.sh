#!/usr/bin/env bash
# Run the timed comparison matrix on the EMR primary node, inside tmux:
#   tmux new -s task2 ; bash run_matrix.sh 2>&1 | tee run_matrix.log
# Every run is a fresh nbconvert execution (fresh kernel, fresh Hive database, fresh HDFS folders).
# Defaults give the 9 runs the brief asks for: dev/reference x3, full/reference x3, full/alt x3.
#   MATRIX="dev:reference full:reference full:alt"   REPS="1 2 3"   BUCKET=<optional S3 bucket>
set -u
NB=${NB:-INF2006_G12_task2_driver.ipynb}
MATRIX=${MATRIX:-"dev:reference full:reference full:alt"}
REPS=${REPS:-"1 2 3"}
EXEC_DIR=${EXEC_DIR:-$HOME/task2_runs/executed_notebooks}
mkdir -p "$EXEC_DIR"
for pc in $MATRIX; do
  for rep in $REPS; do
    export PROFILE=${pc%%:*} CONFIG=${pc##*:} REP=$rep RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
    echo "=== $PROFILE / $CONFIG / rep $REP  ($RUN_ID)"
    jupyter nbconvert --to notebook --execute "$NB" \
      --ExecutePreprocessor.timeout=-1 --ExecutePreprocessor.kernel_name=bash \
      --output-dir "$EXEC_DIR" --output "executed_${PROFILE}_${CONFIG}_rep${REP}_${RUN_ID}" \
      || echo "nbconvert reported an error for $PROFILE/$CONFIG/rep$REP (see the executed notebook and run_summary.txt)"
  done
done
python3 "${DRIVER_HOME:-$HOME/task2_driver}/assets/aggregate_runs.py" "${RUNS_ROOT:-$HOME/task2_runs}"
