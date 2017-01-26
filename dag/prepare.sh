#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DAGGEN_PATH="$HOME/devel/daggen/daggen"

pushd "$SCRIPT_DIR/.."

if [ ! -f "$DAGGEN_PATH" ]; then
  echo "daggen executable $DAGGEN_PATH not found"
  exit 1
fi

#
# Experiment 1/2:
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
if [ ! -d "$SCRIPT_DIR/plat_exp1" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 5 1-4 100 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 10 1-4 100 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 20 1-4 100 100
fi

#
# Experiment 3:
# Heterogeneous clusters with homogeneous network of different speeds.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
if [ ! -d "$SCRIPT_DIR/plat_exp3" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 100 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 100 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 100 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 20 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 20 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 20 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 10 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 10 100
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 10 100
fi

if [ ! -d "$SCRIPT_DIR/tasks_exp3" ]; then
  python3 -m pysimgrid.tools.dag_gen "$DAGGEN_PATH" "$SCRIPT_DIR/tasks_exp3" --count 20 40 60 80 100 --fat 0.2 0.4 0.6 --density 0.1 0.2 0.3 0.4 --jump 1 2 3 --regular 0.1 0.5 0.9 --repeat 5
fi

popd
