#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DAGGEN_PATH="$HOME/devel/daggen/daggen"

pushd "$SCRIPT_DIR/.."

if [ ! -f "$DAGGEN_PATH" ]; then
  echo "daggen executable $DAGGEN_PATH not found"
  exit 1
fi

#
# Experiment 1:
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
if [ ! -d "$SCRIPT_DIR/plat_exp1" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 5 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 10 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1" 100 cluster 20 1-4 100 100 --include_master
fi

#
# Experiment 1_inf:
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 1e12 MBps
# Latency:   0
#
if [ ! -d "$SCRIPT_DIR/plat_exp1_inf" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1_inf" 100 cluster 5 1-4 1e12 0 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1_inf" 100 cluster 10 1-4 1e12 0 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp1_inf" 100 cluster 20 1-4 1e12 0 --include_master
fi


if [ ! -d "$SCRIPT_DIR/plat_exp2" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp2" 1 cluster 10 1-4 100 100 --include_master
fi


if [ ! -d "$SCRIPT_DIR/plat_exp2_inf" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp2_inf" 1 cluster 10 1-4 1e12 0 --loopback_bandwidth=1e12 --loopback_latency=0 --include_master
fi


if [ ! -d "$SCRIPT_DIR/tasks_exp2" ]; then
  python3 -m pysimgrid.tools.dag_gen "$DAGGEN_PATH" "$SCRIPT_DIR/tasks_exp2" --count 100 --fat 0.5 0.65 0.8 --density 0.1 --jump 2 --force_ccr 1 10 100 500 1000 --repeat 100
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
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 20 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 20 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 20 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 5 1-4 10 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 10 1-4 10 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/plat_exp3" 1 cluster 20 1-4 10 100 --include_master
fi

if [ ! -d "$SCRIPT_DIR/tasks_exp3" ]; then
  python3 -m pysimgrid.tools.dag_gen "$DAGGEN_PATH" "$SCRIPT_DIR/tasks_exp3" --count 20 40 60 80 100 --fat 0.1 0.2 0.4 --density 0.1 0.2 0.3 0.4 --jump 1 2 3 --regular 0.1 0.5 0.9 --repeat 5
fi


popd
