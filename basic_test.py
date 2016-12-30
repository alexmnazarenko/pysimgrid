"""
Temporary development test for quick-hacking.

Isn't very illustrative and should be deleted on 'release'.

For bettter examples on C API wrappers look at test/test_capi.py.
"""

from __future__ import print_function

import random
import logging
import multiprocessing

import networkx

from pysimgrid import simdag
import pysimgrid.simdag.algorithms as algorithms

_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_FORMAT = "[%(name)s] [%(levelname)5s] [%(asctime)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

class RandomSchedule(simdag.StaticScheduler):
  def get_schedule(self, simulation):
    schedule = {host: [] for host in simulation.hosts}
    graph = simulation.get_task_graph()
    for task in networkx.topological_sort(graph):
      schedule[random.choice(simulation.hosts)].append(task)
    return schedule


class SimpleDynamic(simdag.DynamicScheduler):
  def prepare(self, simulation):
    for h in simulation.hosts:
      h.data = {}

  def schedule(self, simulation, changed):
    for h in simulation.hosts:
      h.data["free"] = True
    for task in simulation.tasks[simdag.TaskState.TASK_STATE_RUNNING, simdag.TaskState.TASK_STATE_SCHEDULED]:
      task.hosts[0].data["free"] = False
    for t in simulation.tasks[simdag.TaskState.TASK_STATE_SCHEDULABLE]:
      free_hosts = simulation.hosts.by_data("free", True).sorted(lambda h: t.get_eet(h))
      if free_hosts:
        t.schedule(free_hosts[0])
        free_hosts[0].data["free"] = False
      else:
        break


_SCHEDULERS = {
  "RandomSchedule": algorithms.RandomStatic,
  "SimpleDynamic": SimpleDynamic,
  "MCT": algorithms.MCT,
  "OLB": algorithms.OLB,
  "HCPT": algorithms.HCPT,
  "HEFT": algorithms.HEFT,
  "Lookahead": algorithms.Lookahead,
  "PEFT": algorithms.PEFT
}


def run_simulation(scheduler):
  scheduler_class = _SCHEDULERS[scheduler]
  with simdag.Simulation("test/data/pl_4hosts.xml", "dag/tasks_exp2/testg0.6.dot") as simulation:
    print("Scheduler:", scheduler, scheduler_class)
    scheduler = scheduler_class(simulation)
    scheduler.run()
    print("Scheduler time:", scheduler.scheduler_time)


def main():
  # single run in current process mode, used for profiling
  if False:
    #with simdag.Simulation("test/data/pl_4hosts.xml", "test/data/basic_graph.dot") as simulation:
    #with simdag.Simulation("test/data/pl_4hosts.xml", "dag/tasks_exp2/testg0.6.dot") as simulation:
    with simdag.Simulation("dag/plat_exp1/cluster_20_1-4_100_100_0.xml", "dag/tasks_exp2/testg0.6.dot") as simulation:
      #graph = simulation.get_task_graph()
      #scheduler = heft.HEFTScheduler(simulation)
      scheduler = algorithms.Lookahead(simulation)
      #scheduler = peft.PEFTScheduler(simulation)
      scheduler.run()
      print(scheduler.scheduler_time, scheduler.total_time)
    return
  # example: how to run multiple simulations in a single script (circumventing SimGrid limitation of 'non-restartable' simulator state)
  for scheduler in _SCHEDULERS.keys():
    p = multiprocessing.Process(target=run_simulation, args=(scheduler,))
    p.start()
    p.join()


if __name__ == '__main__':
  main()
