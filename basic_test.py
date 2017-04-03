"""
Temporary development test for quick-hacking.

Probably should be deleted on 'final release'.
It's pretty illustrative, however.

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
  "MinMin": algorithms.BatchMin,
  "MaxMin": algorithms.BatchMax,
  "Sufferage": algorithms.BatchSufferage,
  "DLS": algorithms.DLS,
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
  with simdag.Simulation("test/data/pl_4hosts_master.xml", "dag/tasks_exp2/testg0.6.dot") as simulation:
    print("Scheduler:", scheduler, scheduler_class)
    scheduler = scheduler_class(simulation)
    scheduler.run()
    print("Scheduler time:", scheduler.scheduler_time)


def main():
  # single run in current process mode, used for profiling
  if True:
    #with simdag.Simulation("test/data/pl_4hosts.xml", "test/data/basic_graph.dot") as simulation:
    with simdag.Simulation("dag2/plat_exp1/cluster_5_1-4_100_100_0.xml", "dag2/tasks_exp1/CyberShake_100.xml") as simulation:
    #with simdag.Simulation("/home/panda/devel/simgrid_experiments/dag2/plat_exp2/cluster_10_1-4_100_100_0.xml",
    #"/home/panda/devel/simgrid_experiments/dag2_fromwork_14_02_17/tasks_exp2/daggen_100_0_2048_11264_2_0.800_0.900_0.100_10.000_35.dot") as simulation:
    #with simdag.Simulation("dag2/plat_exp1/cluster_5_1-4_100_100_0.xml", "dag/tasks_exp2/testg0.2.dot") as simulation:
    #with simdag.Simulation("dag/plat_exp1/cluster_20_1-4_100_100_0.xml", "dag/tasks_exp2/testg0.6.dot") as simulation:
      #graph = simulation.get_task_graph()
      scheduler = algorithms.HEFT(simulation)
      #scheduler = algorithms.DLS(simulation)
      #scheduler = algorithms.HCPT(simulation)
      #scheduler = algorithms.Lookahead(simulation)
      #scheduler = algorithms.OLB(simulation)
      #scheduler = algorithms.BatchMin(simulation)
      #scheduler = algorithms.PEFT(simulation)
      #scheduler = algorithms.SimHEFT(simulation)
      scheduler.run()
      for t in simulation.tasks.sorted(lambda t: t.start_time):
        print(t.name, t.start_time, t.finish_time, t.hosts[0].name)
      print(scheduler.scheduler_time, scheduler.total_time)
      print("EXEC", sum([(t.finish_time - t.start_time) for t in simulation.tasks]))
      print("COMM", sum([(t.finish_time - t.start_time) for t in simulation.connections]))
    return
  # example: how to run multiple simulations in a single script (circumventing SimGrid limitation of 'non-restartable' simulator state)
  for scheduler in _SCHEDULERS.keys():
    p = multiprocessing.Process(target=run_simulation, args=(scheduler,))
    p.start()
    p.join()


if __name__ == '__main__':
  main()
