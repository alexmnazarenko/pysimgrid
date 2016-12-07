#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

if [ ! -f "$SCRIPT_DIR/exp1.json" ]; then
  python3 -m pysimgrid.tools.experiment dag/plat_exp1 dag/tasks_exp1 dag/algorithms.json dag/exp1.json -j8 --simgrid-log-level=error
fi

if [ ! -f "$SCRIPT_DIR/exp2.json" ]; then
  python3 -m pysimgrid.tools.experiment dag/plat_exp1 dag/tasks_exp2 dag/algorithms.json dag/exp2.json -j8
fi

if [ ! -f "$SCRIPT_DIR/exp3.json" ]; then
  python3 -m pysimgrid.tools.experiment dag/plat_exp3 dag/tasks_exp3 dag/algorithms.json dag/exp3.json -j8
fi

popd
