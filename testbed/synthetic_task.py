#!/usr/bin/python

import os
import subprocess
import sys
import time


if __name__ == '__main__':
    start = time.time()

    task_size = float(sys.argv[1])  # size should be in Gflop!
    host_speed = float(os.environ['HOST_SPEED_GFLOPS'])
    run_time = task_size / host_speed
    print('Task size: %f' % task_size)
    print('Host speed: %f' % host_speed)
    print('Task run time: %f' % run_time)

    for i in xrange(2, len(sys.argv)):
        parts = sys.argv[i].split(':')
        output_file = parts[0]
        output_size = float(parts[1]) * 1024 * 1024  # size should be in Mbytes!
        if output_size < 1:
            output_size = 1
        ret_code = subprocess.call(
            ['dd', 'if=/dev/zero', 'of=%s' % output_file, 'bs=%d' % output_size, 'count=1'],
            stdout=open(os.devnull, 'w'),
            stderr=subprocess.STDOUT
        )
        if ret_code == 0:
            print('Created output file: %s (%f bytes)' % (output_file, output_size))
        else:
            sys.exit(ret_code)

    sleep_time = run_time - (time.time() - start)
    if sleep_time > 0:
        time.sleep(sleep_time)
