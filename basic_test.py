"""
Temporary development test for quick-hacking.

Isn't very illustrative and should be deleted on 'release'.

For bettter examples on C API wrappers look at test/test_capi.py.
"""

from __future__ import print_function

import random
import logging

#import networkx
from pysimgrid import simdag

_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_FORMAT = "[%(name)s] [%(levelname)5s] [%(asctime)s] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

class RandomSchedule(simdag.StaticScheduler):
  def _schedule(self, simulation):
    for t in simulation.tasks:
      t.schedule(random.choice(simulation.hosts))


class GreedyDynamic(simdag.DynamicScheduler):
  def _prepare(self, simulation):
    for h in simulation.hosts:
      h.data = {}

  def _schedule(self, simulation, changed):
    for h in simulation.hosts:
      h.data["free"] = True
    for task in simulation.tasks[simdag.TaskState.TASK_STATE_RUNNING]:
      task.hosts[0].data["free"] = False
    for t in simulation.tasks[simdag.TaskState.TASK_STATE_SCHEDULABLE]:
      free_hosts = simulation.hosts.by_data("free", True)
      if free_hosts:
        t.schedule(free_hosts[0])
        free_hosts[0].data["free"] = False
      else:
        break


with simdag.Simulation("test/data/pl_4hosts.xml", "test/data/basic_graph.dot") as simulation:
  if False:
    RandomSchedule(simulation).run()
  else:
    GreedyDynamic(simulation).run()
