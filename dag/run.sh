#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

python3 -m pysimgrid.tools.experiment dag/plat_exp1 dag/tasks_exp1 dag/algorithms.json dag/exp1.json -j8

popd
