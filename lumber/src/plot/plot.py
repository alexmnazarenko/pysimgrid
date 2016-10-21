# A. Nazarenko 2016
# -*- coding: utf-8 -*-

import argparse
import collections
import json
import numpy
import matplotlib.pyplot as plt

def gantt(config):
  with open(config.input) as f:
    data = json.load(f)
  # TODO: use jsonschema for validation
  hosts = [host["name"] for host in data["hosts"]]
  tasks = data["tasks"]

  tasks_by_host = collections.defaultdict(list)
  for t in tasks:
    for host in set(t["hosts"]):
      tasks_by_host[host].append(t)

  plt.figure(0)
  idx = 0

  BAR_HEIGHT = 0.8
  used_hosts = []
  for host, host_tasks in tasks_by_host.items():
    comp_tasks = zip(*[(task["start"], task["end"] - task["start"]) for task in host_tasks if task["type"] == "comp"])
    comm_tasks = zip(*[(task["start"], task["end"] - task["start"]) for task in host_tasks if task["type"] == "comm"])
    plt.barh([idx + 0.1] * len(comp_tasks[0]), comp_tasks[1], left=comp_tasks[0], height=BAR_HEIGHT, color="r")
    plt.barh([idx + 0.1] * len(comm_tasks[0]), comm_tasks[1], left=comm_tasks[0], height=BAR_HEIGHT, color="b")
    used_hosts.append(((idx + 0.5), host))
    idx += 1
  plt.yticks(*zip(*used_hosts))
  plt.show()


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("mode", type=str, choices=["gantt"], help="plotting mode. defines both output and required input.")
  parser.add_argument("input", type=str, help="input file.")
  config = parser.parse_args()

  MODES = {
    "gantt": gantt
  }

  MODES[config.mode](config)

if __name__ == '__main__':
  main()
