#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

#
# Experiment 1:
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 20 cluster 5 1-4 100 10
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 20 cluster 10 1-4 100 10
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 20 cluster 20 1-4 100 10

popd
