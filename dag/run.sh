#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

if [ ! -f "$SCRIPT_DIR/exp1.json" ]; then
  python3 -m pysimgrid.tools.experiment "$SCRIPT_DIR/exp1_systems" "$SCRIPT_DIR/exp1_workflows" "$SCRIPT_DIR/algorithms.json" dag/exp1.json -j8  --simgrid-log-level=error
fi

if [ ! -f "$SCRIPT_DIR/exp1_inf.json" ]; then
  python3 -m pysimgrid.tools.experiment "$SCRIPT_DIR/exp1_systems_inf" "$SCRIPT_DIR/exp1_workflows" "$SCRIPT_DIR/algorithms.json" dag/exp1_inf.json -j8  --simgrid-log-level=error
fi

if [ ! -f "$SCRIPT_DIR/exp2.json" ]; then
  python3 -m pysimgrid.tools.experiment "$SCRIPT_DIR/exp2_systems" "$SCRIPT_DIR/exp2_workflows" "$SCRIPT_DIR/algorithms.json" dag/exp2.json -j8
fi

if [ ! -f "$SCRIPT_DIR/exp2_inf.json" ]; then
  python3 -m pysimgrid.tools.experiment "$SCRIPT_DIR/exp2_systems_inf" "$SCRIPT_DIR/exp2_workflows" "$SCRIPT_DIR/algorithms.json" dag/exp2_inf.json -j8
fi

popd
