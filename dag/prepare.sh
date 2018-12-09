#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd "$SCRIPT_DIR/.."

if [ -z "$DAGGEN_PATH" ]; then
  echo "Specify path to daggen executable in DAGGEN_PATH environment variable"
  exit 1
fi
if [ ! -f "$DAGGEN_PATH" ]; then
  echo "daggen executable $DAGGEN_PATH not found"
  exit 1
fi

SEED=1234

# EXPERIMENT 1 (REAL WORKFLOWS) ****************************************************************************************

#
# Real workflows (CyberShake, Epigenomics, LIGO Inspiral, Montage)
#
# Tasks: 100
# See tasks_exp1 directory
#

#
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Nodes:     5, 10, 20
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
if [ ! -d "$SCRIPT_DIR/exp1_systems" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems" 100 $SEED cluster 5 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems" 100 $SEED cluster 10 1-4 100 100 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems" 100 $SEED cluster 20 1-4 100 100 --include_master
fi

#
# Heterogeneous clusters with infinitely fast network (including loopback).
#
# Nodes:     5, 10, 20
# Speed:     1-4 GFLOPS
# Bandwidth: 1e12 MBps
# Latency:   0 us
#
if [ ! -d "$SCRIPT_DIR/exp1_systems_inf" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems_inf" 100 $SEED cluster 5 1-4 1e12 0 \
        --loopback_bandwidth=1e12 --loopback_latency=0 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems_inf" 100 $SEED cluster 10 1-4 1e12 0 \
        --loopback_bandwidth=1e12 --loopback_latency=0 --include_master
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp1_systems_inf" 100 $SEED cluster 20 1-4 1e12 0 \
        --loopback_bandwidth=1e12 --loopback_latency=0 --include_master
fi

# EXPERIMENT 2 (SYNTHETIC WORKFLOWS) ***********************************************************************************

#
# Synthetic workflows
#
# Tasks:     100
# Fat:       0.5, 0.65, 0.8 (10, 20, 40 tasks per layer)
# Density:   0.1
# Jump:      2
# Data:      1-1000 MB
# CCR:       1, 10, 100 MB/Gflop
#
if [ ! -d "$SCRIPT_DIR/exp2_workflows" ]; then
  python3 -m pysimgrid.tools.dag_gen \
        -n 100 --fat 0.5 0.65 0.8 --density 0.1 --jump 2 \
        --mindata 1e6 --maxdata 1e9 --ccr 1 10 100 \
        --repeat 100 --seed $SEED \
        "$SCRIPT_DIR/exp2_workflows"
fi

#
# Heterogeneous cluster with homogeneous network a-la 1Gbit Ethernet.
#
# Nodes:     10
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
if [ ! -d "$SCRIPT_DIR/exp2_systems" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp2_systems" 1 $SEED cluster 10 1-4 100 100 --include_master
fi

#
# Heterogeneous cluster with infinitely fast network (including loopback).
#
# Nodes:     10
# Speed:     1-4 GFLOPS
# Bandwidth: 1e12 MBps
# Latency:   0 us
#
if [ ! -d "$SCRIPT_DIR/exp2_systems_inf" ]; then
  python3 -m pysimgrid.tools.plat_gen "$SCRIPT_DIR/exp2_systems_inf" 1 $SEED cluster 10 1-4 1e12 0 \
        --loopback_bandwidth=1e12 --loopback_latency=0 --include_master
fi

popd
