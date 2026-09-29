#!/usr/bin/env bash
# Unattended chain after a model change: calibration -> subsample and screen sets -> quick
# Noord-Brabant scenario run + comparison -> Morris/LHS screen -> subsample validation.
# Each step logs to <OUT>/log_<step>.txt; progress (start, end, exit code) goes to
# <OUT>/overnight_chain.log. Stops only if the calibration fails or retains nothing; a later step
# that fails is logged and the chain goes on.
# Usage (Git Bash, from the repo root):  bash results_analysis/overnight_chain.sh [OUT] [OLD_NB]
#   OUT     calibration folder (fresh), default results/calib1000_v2
#   OLD_NB  where the current results/noord-brabant is moved to before the quick run
cd "$(dirname "$0")/.." || exit 1
export PYTHONIOENCODING=utf-8
PY=.venv/Scripts/python.exe
OUT=${1:-results/calib1000_v2}
OLD_NB=${2:-results/noord-brabant_calib1000_2026-09-29}
SCOPE=province:Noord-Brabant
mkdir -p "$OUT"
LOG="$OUT/overnight_chain.log"

step() {
    local name=$1; shift
    echo "=== $(date '+%F %T') START $name: $*" | tee -a "$LOG"
    "$@" > "$OUT/log_$name.txt" 2>&1
    local rc=$?
    echo "=== $(date '+%F %T') END   $name rc=$rc" | tee -a "$LOG"
    return $rc
}

if [ -e "$OUT/calibration_search.json" ]; then
    echo "=== $OUT/calibration_search.json exists: skipping the calibration" | tee -a "$LOG"
else
    step calibration $PY results_analysis/calibrate_weights.py search --samples 1000 --outdir "$OUT" \
        || { echo "=== calibration failed: stopping" | tee -a "$LOG"; exit 1; }
fi
N=$($PY -c "import json,sys; print(len(json.load(open(sys.argv[1]))['retained']))" "$OUT/calibration_search.json")
echo "=== retained sets: $N" | tee -a "$LOG"
[ "${N:-0}" -gt 0 ] || { echo "=== nothing retained: stopping (loosen the retention rule)" | tee -a "$LOG"; exit 1; }

step subsample $PY results_analysis/select_subsample.py select "$OUT" --n 25
step structure $PY results_analysis/ensemble_structure.py "$OUT"

# quick scenario run on Noord-Brabant (3 representative sets); keep the previous results for comparison
if [ -d results/noord-brabant ] && [ ! -e "$OLD_NB" ]; then
    mv results/noord-brabant "$OLD_NB" && echo "=== moved results/noord-brabant -> $OLD_NB" | tee -a "$LOG"
fi
step quickrun $PY results_analysis/pathway_batch.py --scope "$SCOPE" --search "$OUT/calibration_search.json" \
    --ensemble-weights representative --price-scenario baseline
[ -d "$OLD_NB" ] && step compare $PY results_analysis/compare_runs.py "$OLD_NB" results/noord-brabant \
    --years 2030,2050 --by-ownership

# Morris at the four screen sets + LHS at the central set + figures (takes its sets from
# calibration_subsample.json written above)
step morris $PY results_analysis/run_overnight.py --scope "$SCOPE" --outdir "$OUT" --skip-search
# do the 25 stand in for all retained sets?
step validate $PY results_analysis/select_subsample.py validate "$OUT" --scope "$SCOPE"
echo "=== $(date '+%F %T') chain finished" | tee -a "$LOG"
