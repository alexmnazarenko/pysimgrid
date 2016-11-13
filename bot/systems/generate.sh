#!/bin/bash

#
# Homogeneous clusters with infinitely fast network.
#
# Speed:     1 GFLOPS
# Bandwidth: 1e12 MBps
# Latency:   0 us
#
python ../gen/sys_gen.py cluster-0comm-hom-10 1 cluster 10 1 1e12 0
python ../gen/sys_gen.py cluster-0comm-hom-100 1 cluster 100 1 1e12 0
python ../gen/sys_gen.py cluster-0comm-hom-1000 1 cluster 1000 1 1e12 0
python ../gen/sys_gen.py cluster-0comm-hom-2000 1 cluster 2000 1 1e12 0

#
# Homogeneous clusters with homogeneous network a-la 1Gb Ethernet.
#
# Speed:     1 GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
python ../gen/sys_gen.py cluster-lan-hom-100 1 cluster 100 1 100 100

#
# Heterogeneous clusters with infinitely fast network.
#
# Speed:     1-2(5,10) GFLOPS
# Bandwidth: 1e12 MBps
# Latency:   0 us
#
python ../gen/sys_gen.py cluster-0comm-het2-10 100 cluster 10 1-2 1e12 0
python ../gen/sys_gen.py cluster-0comm-het2-100 100 cluster 100 1-2 1e12 0
python ../gen/sys_gen.py cluster-0comm-het2-1000 100 cluster 1000 1-2 1e12 0
python ../gen/sys_gen.py cluster-0comm-het2-2000 100 cluster 2000 1-2 1e12 0
python ../gen/sys_gen.py cluster-0comm-het5-10 100 cluster 10 1-5 1e12 0
python ../gen/sys_gen.py cluster-0comm-het5-100 100 cluster 100 1-5 1e12 0
python ../gen/sys_gen.py cluster-0comm-het5-1000 100 cluster 1000 1-5 1e12 0
python ../gen/sys_gen.py cluster-0comm-het5-2000 100 cluster 2000 1-5 1e12 0
python ../gen/sys_gen.py cluster-0comm-het10-10 100 cluster 10 1-10 1e12 0
python ../gen/sys_gen.py cluster-0comm-het10-100 100 cluster 100 1-10 1e12 0
python ../gen/sys_gen.py cluster-0comm-het10-1000 100 cluster 1000 1-10 1e12 0
python ../gen/sys_gen.py cluster-0comm-het10-2000 100 cluster 2000 1-10 1e12 0

#
# Heterogeneous clusters with homogeneous network a-la 1Gb Ethernet.
#
# Speed:     1-2(5,10) GFLOPS
# Bandwidth: 100 MBps
# Latency:   100 us
#
python ../gen/sys_gen.py cluster-lan-het2-100 100 cluster 100 1-2 100 100
python ../gen/sys_gen.py cluster-lan-het5-100 100 cluster 100 1-5 100 100
python ../gen/sys_gen.py cluster-lan-het10-100 100 cluster 100 1-10 100 100