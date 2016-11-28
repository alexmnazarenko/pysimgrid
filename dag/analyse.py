import argparse
import collections
import json
import os
import numpy


def groupby(results, condition):
  groups = collections.defaultdict(list)
  for data in results:
    key = condition(data)
    groups[key].append(data)
  return groups.items()

def main():
  parser = argparse.ArgumentParser(description="Experiment results analysis")
  parser.add_argument("input_file", type=str, help="experiment results")
  args = parser.parse_args()

  with open(args.input_file) as input_file:
    results = json.load(input_file)

  get_no_hosts = lambda r: int(os.path.basename(r["platform"]).split("_")[1])
  get_task_type = lambda r: os.path.basename(r["tasks"]).split(".")[0]
  get_algorithm = lambda r: r["algorithm"]["name"]

  for no_hosts, byhost in groupby(results, get_no_hosts):
    for task, bytask in groupby(byhost, get_task_type):
      for algorithm, byalg in groupby(bytask, get_algorithm):
        makespans = [r["makespan"] for r in byalg]
        mean = numpy.mean(makespans)
        mm, MM = numpy.min(makespans), numpy.max(makespans)
        median = numpy.median(makespans)
        print("%s %s %s: %f [%f, %f] %f" % (no_hosts, task, algorithm, mean, mm, MM, median))





if __name__ == "__main__":
  main()
