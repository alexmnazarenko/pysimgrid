# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from __future__ import print_function

import argparse
import concurrent.futures
import fnmatch
import itertools
import json
import logging
import multiprocessing
import os
from .. import simdag

def file_list(file_or_dir, masks=["*"]):
  if not os.path.exists(file_or_dir):
    raise Exception("path %s does not exist" % file_or_dir)
  if os.path.isdir(file_or_dir):
    result = []
    listing = os.listdir(file_or_dir)
    for fname in listing:
      if any((fnmatch.fnmatch(fname, pattern) for pattern in masks)):
        result.append(os.path.join(file_or_dir, fname))
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


def run_experiment(platform, tasks, algorithm):
  logger = logging.getLogger("pysimgrid.tools.Experiment")
  logger.info("Starting experiment (platform=%s, tasks=%s, algorithm=%s)", platform, tasks, algorithm["class"])
  scheduler_class = import_algorithm(algorithm["class"])
  with simdag.Simulation(platform, tasks) as simulation:
    scheduler = scheduler_class(simulation)
    scheduler.run()
    clock = simulation.clock
  return clock


def run_experiment_launcher(queue, job):
  queue.put(run_experiment(*job))


def main():
  _LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
  _LOG_FORMAT = "[%(name)s] [%(levelname)5s] [%(asctime)s] %(message)s"

  parser = argparse.ArgumentParser(description="Run experiments for a set of scheduling algorithms")
  parser.add_argument("platforms", type=str, help="path to file or directory containing platform definitions (*.xml)")
  parser.add_argument("tasks", type=str, help="path to file or directory containing task definitions (*.dax, *.dot)")
  parser.add_argument("algorithms", type=str, help="path to json defining the algorithms to use")
  parser.add_argument("output", type=str, help="path to the output file")
  #parser.add_argument("-j", "--jobs", type=int, default=4, help="number of parallel jobs to run")
  args = parser.parse_args()

  logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

  with open(args.algorithms, "r") as alg_file:
    algorithms = json.load(alg_file)
    if not isinstance(algorithms, list):
      algorithms = [algorithms]

  platforms = file_list(args.platforms)
  tasks = file_list(args.tasks)

  # TODO: multiprocessing high-level interface causes mysterious hangs. need to investigate
  results = []
  queue = multiprocessing.Queue()
  for job in itertools.product(platforms, tasks, algorithms):
    p = multiprocessing.Process(target=run_experiment_launcher, args=(queue, job))
    p.start()
    makespan = queue.get()
    platform, tasks, algorithm = job
    results.append({
      "platform": platform,
      "tasks": tasks,
      "algorithm": algorithm,
      "makespan": makespan
    })
    p.join()

  with open(args.output, "w") as out_file:
    json.dump(results, out_file)


if __name__ == "__main__":
  main()
