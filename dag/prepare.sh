#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

#
# Experiment 1/2:
# Heterogeneous clusters with homogeneous network a-la 1Gbit Ethernet.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 100 cluster 5 1-4 100 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 100 cluster 10 1-4 100 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp1 100 cluster 20 1-4 100 100

#
# Experiment 3:
# Heterogeneous clusters with homogeneous network of different speeds.
#
# Speed:     1-4 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 5 1-4 100 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 10 1-4 100 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 20 1-4 100 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 5 1-4 20 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 10 1-4 20 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 20 1-4 20 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 5 1-4 10 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 10 1-4 10 100
python3 -m pysimgrid.tools.plat_gen dag/plat_exp3 1 cluster 20 1-4 10 100

popd
