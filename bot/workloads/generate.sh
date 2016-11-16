#!/bin/bash

# All workloads have 1000 tasks.

#
# Fixed size tasks with zero I/O.
#
# COMP: 100 GFLOPS
# I/O:  0
#
python ../gen/bot_gen.py 1000 0 1e11 0 fixed-0comm 1

#
# Varied size tasks with zero I/O.
#
# COMP: 10-(20,100,1000) GFLOPS
# I/O:  0
#
python ../gen/bot_gen.py 1000 0 u:1e10:2e10 0 varied2-0comm 100
python ../gen/bot_gen.py 1000 0 u:1e10:1e11 0 varied10-0comm 100
python ../gen/bot_gen.py 1000 0 u:1e10:1e12 0 varied100-0comm 100

#
# Fixed size tasks with different granularity.
#
# Granularity   COMP (GFLOPS)   I/O (MB)
# 1             100             100 / 10
# 10            100             10  / 1
# 100           100             1   / 0.1
#
python ../gen/bot_gen.py 1000 1e8 1e11 1e7 fixed-gran1 1
python ../gen/bot_gen.py 1000 1e7 1e11 1e6 fixed-gran10 1
python ../gen/bot_gen.py 1000 1e6 1e11 1e5 fixed-gran100 1

#
# Varied size tasks with different granularity.
#
# Granularity   COMP (GFLOPS)   I/O (MB)
# 1             10-20           10-20   / 1-2
# 1             10-100          10-100  / 1-10
# 1             10-1000         10-1000 / 1-100
# 10            10-20           1-2     / 0.1-0.2
# 10            10-100          1-10    / 0.1-1
# 10            10-1000         1-100   / 0.1-10
# 100           10-20           0.1-0.2 / 0.01-0.02
# 100           10-100          0.1-1   / 0.01-0.1
# 100           10-1000         0.1-10  / 0.01-1
#
python ../gen/bot_gen.py 1000 u:1e7:2e7 x:1e3 x:0.1 varied2-gran1 100
python ../gen/bot_gen.py 1000 u:1e7:1e8 x:1e3 x:0.1 varied10-gran1 100
python ../gen/bot_gen.py 1000 u:1e7:1e9 x:1e3 x:0.1 varied100-gran1 100
python ../gen/bot_gen.py 1000 u:1e6:2e6 x:1e4 x:0.1 varied2-gran10 100
python ../gen/bot_gen.py 1000 u:1e6:1e7 x:1e4 x:0.1 varied10-gran10 100
python ../gen/bot_gen.py 1000 u:1e6:1e8 x:1e4 x:0.1 varied100-gran10 100
python ../gen/bot_gen.py 1000 u:1e5:2e5 x:1e5 x:0.1 varied2-gran100 100
python ../gen/bot_gen.py 1000 u:1e5:1e6 x:1e5 x:0.1 varied10-gran100 100
python ../gen/bot_gen.py 1000 u:1e5:1e7 x:1e5 x:0.1 varied100-gran100 100