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
TODO: algorithm is WIP.
"""

import itertools
import multiprocessing
import tempfile

import networkx

from .. import scheduler
from ... import cscheduling

class _ExtrenalSchedule(scheduler.StaticScheduler):
  def __init__(self, simulation, schedule):
    super(_ExtrenalSchedule, self).__init__(simulation)
    self.__schedule = schedule
  
  def get_schedule(self, simulation):
    return self.__schedule


def _update_subgraph(full, subgraph, task):
  parents = full.pred[task]
  subgraph.add_node(task)
  for parent, edge_dict in parents.items():
    subgraph.add_edge(parent, task, edge_dict)


def _serialize_graph(graph, output_file):
  output_file.write("digraph G {\n")
  for task in graph:
    output_file.write('  "%s" [size="%f"];\n' % (task.name, task.amount))
  output_file.write("\n");
  for src, dst, data in graph.edges_iter(data=True):
    output_file.write('  "%s" -> "%s" [size="%f"];\n' % (src.name, dst.name, data["weight"]))
  output_file.write("}\n")
  output_file.flush()


def _serialize_schedule(timetable):
  result = {}
  for host, timesheet in timetable.items():
    result[host.name] = [task.name for (task, _, _) in timesheet]
  return result


def _restore_state(simulation, serialized):
  tasks = {t.name: t for t in simulation.tasks}
  hosts = {h.name: h for h in simulation.hosts}
  state = cscheduling.SchedulerState(simulation)
  for hostname, timesheet in serialized.items():
    for pos, (taskname, start, finish) in enumerate(timesheet):
      state.update(tasks[taskname], hosts[hostname], pos, start, finish)
  return state


def _restore_schedule(simulation, serialized):
  tasks = {t.name: t for t in simulation.tasks}
  hosts = {h.name: h for h in simulation.hosts}
  result = {}
  end_scheduled = False
  for hostname, tasknames in serialized.items():
    tasklist = []
    for taskname in tasknames:
      end_scheduled = end_scheduled or taskname == "end"
      tasklist.append(tasks[taskname])
    result[hosts[hostname]] = tasklist
  if not end_scheduled:
    for host_schedule in result.values():
      host_schedule.append(tasks["end"])
      break
  return result, end_scheduled


def _run_simulation(platform_path, tasks_path, schedule_by_name):
  import collections
  import logging
  from .. import simulation
  logging.getLogger().setLevel(logging.WARNING)
  with simulation.Simulation(platform_path, tasks_path, log_config="root.threshold:WARNING") as simulation:
    restored_schedule_state, final = _restore_schedule(simulation, schedule_by_name)
    scheduler = _ExtrenalSchedule(simulation, restored_schedule_state)
    scheduler.run()
    result = collections.defaultdict(list)
    for t in sorted(simulation.tasks, key=lambda t: t.start_time):
      if not final and t.name == "end":
        continue
      result[t.hosts[0].name].append((t.name, t.start_time, t.finish_time))
    return result


class SimHEFT(scheduler.StaticScheduler):
  """
  Modified version of HEFT scheduler using simulation estimates instead of linear models.
  """

  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)

    ordered_tasks = cscheduling.heft_order(nxgraph, platform_model)

    subgraph = networkx.DiGraph()

    # fork context is incompatible with SimGrid static variables
    ctx = multiprocessing.get_context("spawn")
    for task in ordered_tasks:
      print("SCHEDULING", task.name)
      _update_subgraph(nxgraph, subgraph, task)
      if cscheduling.try_schedule_boundary_task(task, platform_model, state):
        continue
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        if cscheduling.is_master_host(host):
          continue
        current_state = state.copy()
        est = platform_model.est(host, nxgraph.pred[task], current_state)
        eet = platform_model.eet(task, host)
        # 'correct' way
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        # TODO: try aggressive inserts
        current_state.update(task, host, pos, start, finish)
        with tempfile.NamedTemporaryFile("w", suffix=".dot") as temp_file:
          _serialize_graph(subgraph, temp_file)
          subschedule = _serialize_schedule(current_state.timetable)
          with ctx.Pool(1) as process:
            serialized_state = process.apply(_run_simulation, (simulation.platform_path, temp_file.name, subschedule))
          current_state = _restore_state(simulation, serialized_state)
          current_min.update((current_state.max_time, host.speed, host.name), current_state)
      state = current_min.value
    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan