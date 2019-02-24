# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# along with this library.  If not, see <http://www.gnu.org/licenses/>.
#

"""
Simple, but effective batch simulation tool.

Usage::

    python -m pysimgrid.tools.experiment [-h] [-j JOBS] [-l {debug,info,warning,error,critical}]
                         [--simgrid-log-level {trace,debug,verbose,info,warning,error,critical}]
                         [--stop-on-error] [--algo [ALGO [ALGO ...]]]
                         platforms tasks algorithms output

    positional arguments:
      platforms             path to file or directory containing platform
                            definitions (*.xml)
      tasks                 path to file or directory containing task definitions
                            (*.dax, *.dot)
      algorithms            path to json defining the algorithms to use
      output                path to the output file

    optional arguments:
      -h, --help            show this help message and exit
      -j JOBS, --jobs JOBS  number of parallel jobs to run
      -l {debug,info,warning,error,critical}, --log-level {debug,info,warning,error,critical}
                            job log level
      --simgrid-log-level {trace,debug,verbose,info,warning,error,critical}
                            simulator log level
      --stop-on-error       stop experiment on a first error
      --algo [ALGO [ALGO ...]]
                            name(s) of algorithms to use (case-sensitive; must
                            still be defined in algorithms file)
      --estimator           estimator to generate estimates (Accurate, SimpleDispersion:percentage)
      --make-charts         generate chart for each execution
"""

from __future__ import print_function

import argparse
import datetime
import fnmatch
import itertools
import json
import logging
import math
import multiprocessing
import ntpath
import os
import re
import textwrap
import time

logging.getLogger("matplotlib").setLevel(logging.WARNING)
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab

from .estimator import AccurateEstimator, SimpleDispersionEstimator
from .. import simdag

# Weird, but necessary workaround to use nested multiprocessing.
#
# Usecase: 
# * experiment tool uses process per experiment
# * some algorithms use processes to run SimGrid to assess partial schedules
#

import multiprocessing.pool

_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_FORMAT = "[%(name)s] [%(levelname)5s] [%(asctime)s] %(message)s"
_LOG_LEVEL_FROM_STRING = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}


class NoDaemonProcess(multiprocessing.context.SpawnProcess):
    @property
    def daemon(self):
        return True

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


######################


def file_list(file_or_dir, masks=["*"]):
    if not os.path.exists(file_or_dir):
        raise Exception("path %s does not exist" % file_or_dir)
    if os.path.isdir(file_or_dir):
        result = []
        for fname in os.listdir(file_or_dir):
            fpath = os.path.join(file_or_dir, fname)
            if os.path.isfile(fpath):
                if any((fnmatch.fnmatch(fname, pattern) for pattern in masks)):
                    result.append(os.path.join(file_or_dir, fname))
            else:
                result.extend(file_list(fpath, masks))
        return result
    else:
        return [os.path.abspath(file_or_dir)]


def import_algorithm(algorithm):
    name_parts = algorithm.split(".")
    module_name = ".".join(name_parts[:-1])
    module = __import__(module_name)
    result = module
    for name in name_parts[1:]:
        result = getattr(result, name)
    assert isinstance(result, type)
    return result


def run_experiment(job):
    platform, tasks, estimator, algorithm, config = job
    python_log_level, simgrid_log_level = config["log_level"], config["simgrid_log_level"]
    stop_on_error = config["stop_on_error"]
    make_charts = config["make_charts"]
    logging.basicConfig(level=config["log_level"], format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    logger = logging.getLogger("pysimgrid.tools.Experiment")
    logger.debug("Starting experiment (platform=%s, tasks=%s, algorithm=%s)", platform, tasks, algorithm["class"])
    scheduler_class = import_algorithm(algorithm["class"])
    # init return values with NaN's
    makespan, exec_time, comm_time, sched_time, exp_makespan = [float("NaN")] * 5
    try:
        with simdag.Simulation(platform, tasks, estimator,
                               log_config="root.threshold:" + simgrid_log_level) as simulation:
            scheduler = scheduler_class(simulation)
            scheduler.run()
            makespan = simulation.clock
            exec_time = sum([t.finish_time - t.start_time for t in simulation.tasks])
            comm_time = sum(
                [t.finish_time - t.start_time for t in simulation.all_tasks[simdag.TaskKind.TASK_KIND_COMM_E2E]])
            sched_time = scheduler.scheduler_time
            if scheduler.expected_makespan is not None:
                exp_makespan = scheduler.expected_makespan
            if make_charts:
                make_chart(simulation, platform, tasks, algorithm["name"], scheduler)
    except Exception:
        # output is not pretty, but complete and robust. it is a crash anyway.
        #   note the wrapping of job into a tuple
        message = "Simulation failed! Parameters: %s" % (job,)
        if stop_on_error:
            raise Exception(message)
        else:
            logger.exception(message)
    return job, makespan, exec_time, comm_time, sched_time, exp_makespan


def progress_reporter(iterable, length, logger):
    start_time = last_result_timestamp = time.time()
    average_time = 0.
    for idx, element in enumerate(iterable):
        current = time.time()
        elapsed = current - last_result_timestamp
        last_result_timestamp = current
        count = idx + 1
        average_time = average_time * (count - 1) / float(count) + elapsed / count
        remaining = (length - idx) * average_time
        eta_string = (" [ETA: %s]" % datetime.timedelta(seconds=remaining)) if idx > 10 else ""
        logger.info("%d/%d%s", idx + 1, length, eta_string)
        yield element
    logger.info("Finished. %d experiments in %f seconds", length, time.time() - start_time)


def make_chart(simulation, platform, tasks, algorithm, scheduler):
    TASK_COLOR1 = 'dodgerblue'
    TASK_COLOR2 = 'royalblue'
    UPLOAD_COLOR = 'lime'
    DOWNLOAD_COLOR = 'deeppink'

    params = {'legend.fontsize': 'small',
              'figure.figsize': (8, 5),
              'axes.labelsize': 'small',
              'axes.titlesize': 'small',
              'xtick.labelsize': 'small',
              'ytick.labelsize': 'small'}
    pylab.rcParams.update(params)

    platform_name = ntpath.basename(platform).split(".")[0]
    app_name = ntpath.basename(tasks).split(".")[0]
    fig_name = "%s_%s_%s" % (platform_name, app_name, algorithm)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.title("System: %s\nApplication: %s\nAlgorithm: %s\nMakespan: %.2f (%.2f)\n" %
              (platform_name, app_name, algorithm, simulation.clock,
               scheduler.expected_makespan if scheduler.expected_makespan is not None else math.nan),
              loc='left')
    plt.margins(x=0)

    # hosts on the chart are sorted by their speed in decreasing order
    # master is always placed on the top of the chart
    hosts = sorted(simulation.hosts,
                   key=lambda h: (h.speed if h.name != 'master' else float('Inf'),
                                  -int(h.name.replace("host", "")) if h.name != 'master' else float('Inf')))
    min_speed = min(hosts, key=lambda h: h.speed).speed
    host_labels = ["%s (%.1f)" % (host.name, host.speed / min_speed) for host in hosts]
    hosts = [host.name for host in hosts]

    # remove master from chart if there are no related data transfers
    master_comm_size = 0
    for comm in simulation.connections:
        if len(comm.hosts) != 2:
            continue
        src = comm.hosts[0].name
        dst = comm.hosts[1].name
        if src == 'master' or dst == 'master':
            master_comm_size += comm.finish_time - comm.start_time
    if master_comm_size < 1:
        hosts.remove('master')
        del host_labels[-1]

    hosts_idx = {host: idx for idx, host in enumerate(hosts)}
    task_count = len(simulation.tasks)
    host_task_count = {host: 0 for host in hosts}

    # draw task executions
    for task in sorted(simulation.tasks, key=lambda t: t.start_time):
        host = task.hosts[0].name
        duration = task.finish_time - task.start_time
        if task.name not in ["root", "end"]:
            idx = hosts_idx[host]
            host_task_count[host] += 1
            if host_task_count[host] % 2 != 0:
                ax.broken_barh([(task.start_time, duration)], (idx - 0.4, 0.8),
                               color=TASK_COLOR1, linewidth=0)
            else:
                ax.broken_barh([(task.start_time, duration)], (idx - 0.4, 0.8),
                               color=TASK_COLOR2, linewidth=0)
            # draw task names only for small apps
            if task_count <= 10:
                ax.text(task.start_time + duration / 2.0, idx, re.sub('[^0-9]', '', task.name),
                        ha='center', va='center', color='white')

    # draw data transfers
    for comm in simulation.connections:
        if len(comm.hosts) != 2:
            continue
        src = comm.hosts[0].name
        dst = comm.hosts[1].name
        if src != dst:
            duration = comm.finish_time - comm.start_time
            if duration > 0.1:
                ax.broken_barh([(comm.start_time, duration)], (hosts_idx[src] + 0.2, 0.2),
                               color=UPLOAD_COLOR, linewidth=0)
                ax.broken_barh([(comm.start_time, duration)], (hosts_idx[dst] - 0.4, 0.2),
                               color=DOWNLOAD_COLOR, linewidth=0)

    ax.set_yticks(range(len(hosts)))
    ax.set_yticklabels(host_labels)
    ax.set_xlabel("time")
    plt.tight_layout()
    fig = plt.gcf()
    fig.savefig(fig_name + ".png", dpi=400)


def main():
    parser = argparse.ArgumentParser(description="Run experiments for a set of scheduling algorithms")
    parser.add_argument("platforms", type=str, help="path to file or directory containing platform definitions (*.xml)")
    parser.add_argument("tasks", type=str, help="path to file or directory containing task definitions (*.dax, *.dot)")
    parser.add_argument("algorithms", type=str, help="path to json defining the algorithms to use")
    parser.add_argument("output", type=str, help="path to the output file")
    parser.add_argument("-j", "--jobs", type=int, default=1, help="number of parallel jobs to run")
    parser.add_argument("-l", "--log-level", type=str, choices=["debug", "info", "warning", "error", "critical"],
                        default="warning", help="job log level")
    parser.add_argument("--simgrid-log-level", type=str,
                        choices=["trace", "debug", "verbose", "info", "warning", "error", "critical"],
                        default="warning", help="simulator log level")
    parser.add_argument("--stop-on-error", action="store_true", default=False, help="stop experiment on a first error")
    # useful for algorithm debugging
    parser.add_argument("--algo", type=str, nargs="*",
                        help="name(s) of algorithms to use (case-sensitive; must still be defined in algorithms file)")
    parser.add_argument("--estimator", type=str,
                        help="estimator to generate estimates (Accurate, SimpleDispersion:percentage)")
    parser.add_argument("--make-charts", action="store_true", default=False, help="generate chart for each execution")
    args = parser.parse_args()

    logging.basicConfig(level=_LOG_LEVEL_FROM_STRING[args.log_level], format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    logger = logging.getLogger("Experiment")

    with open(args.algorithms, "r") as alg_file:
        algorithms = json.load(alg_file)
        if not isinstance(algorithms, list):
            algorithms = [algorithms]
        # we can allow non-unique algorithm names. but really shoudn't as it can mess up the results.
        algorithm_names = set([a["name"] for a in algorithms])
        if len(algorithms) != len(algorithm_names):
            raise Exception("algorithm names should be unique")
        if args.algo:
            for algo_name in args.algo:
                if algo_name not in algorithm_names:
                    raise Exception("algorithm %s is not defined" % algo_name)
            algorithms = [a for a in algorithms if a["name"] in args.algo]

    platforms = []
    for path in args.platforms.split(","):
        platforms.extend(file_list(path, ["*.xml"]))
    tasks = []
    for path in args.tasks.split(","):
        tasks.extend(file_list(path))

    estimator = None
    if args.estimator:
        if args.estimator == "Accurate":
            estimator = AccurateEstimator()
        elif args.estimator.startswith("SimpleDispersion"):
            percentage = float(args.estimator.split(":")[1])
            estimator = SimpleDispersionEstimator(percentage)
        else:
            raise Exception("Unknown estimator")

    config = [{
        "stop_on_error": args.stop_on_error,
        "log_level": _LOG_LEVEL_FROM_STRING[args.log_level],
        "simgrid_log_level": args.simgrid_log_level,
        "make_charts": args.make_charts
    }]

    # convert to list just get length nicely
    #   can be left as an iterator, but memory should not be the issue
    jobs = list(itertools.product(platforms, tasks, [estimator], algorithms, config))

    # report experiment setup
    #   looks scary, but it's probably a shortest way to do this in terms of LOC
    logger.info(textwrap.dedent("""\
  Starting the experiment.
    Total runs: %d

    Platform source: %s
    Platform count:  %d

    Tasks source:    %s
    Tasks count:     %d
    Estimator:       %s

    Algorithms count: %d
    Algorithms:
  %s

    Configuration:
  %s
  """) % (len(jobs), args.platforms, len(platforms), args.tasks, len(tasks), args.estimator, len(algorithms),
          "\n".join(["    " + a["name"] for a in algorithms]),
          "\n".join(["    %s: %s" % (k, v) for k, v in config[0].items()])
          ))

    results = []
    # using the spawn context is important
    #    by default, multiprocessing uses fork, which conflicts with coolhacks inside SimGrid/XBT (library constructors)
    # in more details:
    #    SimGrid library init stores the id of the main thread & creates some custom TLS (don't ask me why)
    #    'fork' doesn't lead to the reinit
    #    SimGrid crashes on unitialized TLS
    ctx = multiprocessing.get_context("spawn")
    with NoDaemonPool(processes=args.jobs, maxtasksperchild=1, context=ctx) as pool:
        for job, makespan, exec_time, comm_time, sched_time, exp_makespan in progress_reporter(
                pool.imap_unordered(run_experiment, jobs, 1), len(jobs), logger):
            platform, tasks, estimator, algorithm, _ = job
            results.append({
                "platform": platform,
                "tasks": tasks,
                "algorithm": algorithm,
                "makespan": makespan,
                "exec_time": exec_time,
                "comm_time": comm_time,
                "sched_time": sched_time,
                "expected_makespan": exp_makespan
            })

    with open(args.output, "w") as out_file:
        json.dump(results, out_file, indent=4)


if __name__ == "__main__":
    main()
