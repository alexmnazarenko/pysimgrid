# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from copy import deepcopy

from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class HEFTScheduler(StaticScheduler):

  def get_schedule(self, simulation):
    taskflow = Taskflow().from_simulation(simulation)
    tasks_map = {t.name: t for t in simulation.tasks}
    raw_schedule = self.construct_heft_schedule(taskflow, simulation.hosts)

    schedule = {}
    for host in simulation.hosts:
      schedule[host] = []
      for elem in raw_schedule[host.name]["timesheet"]:
        task_name = elem[2][0]
        if task_name not in [taskflow.TRUE_ROOT, taskflow.TRUE_END]:
          schedule[host].append(tasks_map[task_name])

    return schedule

  @staticmethod
  def rankify_tasks(taskflow):
    ranked_tasks = {}
    for task in taskflow.topological_order(reverse=True):
      ecomt_and_rank = [
        ranked_tasks[t] + taskflow.matrix[taskflow.tasks.index(task), taskflow.tasks.index(t)]
        for t in taskflow.get_children(task)
        if t in ranked_tasks
        if not taskflow.matrix[taskflow.tasks.index(task), taskflow.tasks.index(t)] is False
      ] or [0]
      ranked_tasks[task] = taskflow.complexities[task] + max(ecomt_and_rank)
    return ranked_tasks

  @staticmethod
  def order_tasks(ranked_tasks):
    return sorted(ranked_tasks.items(), key=lambda x: x[1], reverse=True)

  @staticmethod
  def construct_heft_schedule(taskflow, simulation_hosts, initial_schedule=None, initial_taskname=None):

    def _timesheet_gaps(timesheet):
      pairs = zip(timesheet, timesheet[1:])
      return [(p[0][1], p[1][0]) for p in pairs if p[0][1] != p[1][0]]

    def _calc_host_start(est, amount, hosts):
      e_host_st = []
      for host in hosts:
        # Check host gaps
        duration = float(amount) / hosts[host]["speed"]
        gaps = _timesheet_gaps(hosts[host]["timesheet"])
        for gap in gaps:
          start = max(gap[0], est)
          end = gap[1]
          if end < est or duration > end - start:
            continue
          e_host_st.append((host, start, start + duration))
          break
        if host not in e_host_st:
          # End time of the last host task
          start = (
            hosts[host]["timesheet"][-1][1]
            if len(hosts[host]["timesheet"])
            else 0
          )
          start = max(start, est)
          e_host_st.append((host, start, start + duration))
      return min(e_host_st, key=lambda x: x[2])

    ranked_tasks = HEFTScheduler.rankify_tasks(taskflow)
    ordered_tasks = HEFTScheduler.order_tasks(ranked_tasks)

    initial_start_time = 0
    tasks_info = {}
    if not initial_schedule:
      hosts = {
        host.name: {
          "speed": host.speed,
          "timesheet": []
        }
        for host in simulation_hosts
      }
    else:
      hosts = deepcopy(initial_schedule)

      for host in hosts:
        for task_info in hosts[host]['timesheet']:
          start, end, task_element = task_info
          task_name, _ = task_element
          if task_name == initial_taskname:
            initial_start_time = end
          tasks_info[task_name] = {
            "host": host,
            "start_time": start,
            "end_time": end
          }

    for task in ordered_tasks:
      parents = taskflow.get_parents(task[0])
      if initial_taskname:
        parents.append(initial_taskname)
      est = max(
        [tasks_info[p]["end_time"] for p in parents if p in tasks_info] +
        [0] + 
        [initial_start_time]
      )
      host, start_time, end_time = _calc_host_start(
        est,
        taskflow.complexities[task[0]],
        hosts
      )
      tasks_info[task[0]] = {
        "host": host,
        "start_time": start_time,
        "end_time": end_time
      }
      hosts[host]["timesheet"].append((start_time, end_time, task))
      hosts[host]["timesheet"].sort()

    return hosts
