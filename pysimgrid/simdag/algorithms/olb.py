# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from .. import scheduler
from ... import csimdag


class OLBScheduler(scheduler.DynamicScheduler):
  def prepare(self, simulation):
    for h in simulation.hosts:
      h.data = {}
    self.queue = []

  def schedule(self, simulation, changed):
    for h in simulation.hosts:
      h.data["free"] = True
    for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
      task.hosts[0].data["free"] = False
    queue_set = set(self.queue)
    for t in simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]:
      if t not in queue_set:
        self.queue.append(t)
    clock = simulation.clock
    while self.queue:
      free_hosts = simulation.hosts.by_data("free", True)
      if free_hosts:
        t = self.queue.pop(0)
        free_hosts = free_hosts.sorted(lambda h: h.speed, reverse=True)
        target_host = free_hosts[0]
        t.schedule(target_host)
        target_host.data["free"] = False
      else:
        break
