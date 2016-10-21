# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

import os
import unittest
import random
import pysimgrid
# PY 2/3 compatibility tools
from pysimgrid.six import print_

DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
def _data_path(*relpath):
  return os.path.join(DATA_PATH, *relpath)

class TestSimDAG(unittest.TestCase):
  def setUp(self):
    """
    Initialize SimDAG library.
    """
    pysimgrid.csimdag.initialize()
    pysimgrid.csimdag.config("network/model", "LV08")

  def tearDown(self):
    """
    Initialize SimDAG library.
    """
    pysimgrid.csimdag.exit()

  def test_hosts(self):
    """
    Test platfrom C API - host/route properties.
    """
    hosts = pysimgrid.csimdag.load_platform(_data_path("pl_4hosts.xml"))
    static_hosts = pysimgrid.cplatform.get_hosts()
    self.assertSequenceEqual([h.native for h in hosts], [h.native for h in static_hosts], "host lists from load and current platform state must match")
    self.assertEqual(len(hosts), 4)
    for h in hosts:
      self.assertIsInstance(h, pysimgrid.cplatform.Host)
      self.assertNotEqual(h.native, 0)
      self.assertEqual(h.available_speed, 1.)
      self.assertEqual(h.speed, 2e9)
      self.assertEqual(h.native, pysimgrid.cplatform.host_by_name(h.name).native)
    with self.assertRaises(Exception):
      pysimgrid.cplatform.host_by_name("nicht exist")
    for h1 in hosts:
      for h2 in hosts:
        self.assertNotEqual(pysimgrid.cplatform.route(h1, h2), [])
        if h1 != h2:
          self.assertEqual(pysimgrid.cplatform.route_bandwidth(h1, h2), 1e8)
        else:
          self.assertEqual(pysimgrid.cplatform.route_bandwidth(h1, h2), 4.98e8)
        self.assertGreater(pysimgrid.cplatform.route_latency(h1, h2), 0)
    for h in hosts:
      h.dump()

  def test_random_schedule(self):
    """
    Test static random scheduling.
    """
    hosts = pysimgrid.csimdag.load_platform(_data_path("pl_4hosts.xml"))
    tasks = pysimgrid.csimdag.load_tasks(_data_path("basic_graph.dot"))
    print_("Static random scheduling...")
    for t in tasks:
      if t.kind == pysimgrid.csimdag.TASK_KIND_COMP_SEQ:
        t.schedule(random.choice(hosts))
        print("  Scheduled {} on host {}".format(t.name, t.hosts[0].name))
    print_("Starting the simulation...")
    pysimgrid.csimdag.simulate()
    for t in tasks:
      self.assertEqual(t.state, pysimgrid.csimdag.TASK_STATE_DONE)
    print_("Simulation time:", pysimgrid.csimdag.get_clock())

  def test_primitive_schedule(self):
    """
    Test simple dynamic schedule: send ready tasks to empty hosts.
    """
    hosts = pysimgrid.csimdag.load_platform(_data_path("pl_4hosts.xml"))
    tasks = pysimgrid.csimdag.load_tasks(_data_path("basic_graph.dot"))

    free_hosts = list(hosts)
    def schedule_all_schedulable():
      for t in filter(lambda t: t.kind == pysimgrid.csimdag.TASK_KIND_COMP_SEQ and t.state == pysimgrid.csimdag.TASK_STATE_SCHEDULABLE, tasks):
        if free_hosts:
          t.schedule(free_hosts.pop())
          print_("  Scheduled {} on host {}".format(t.name, t.hosts[0].name))
        else:
          break

    print_("Setting up watchpoints...")
    for t in filter(lambda t: t.kind == pysimgrid.csimdag.TASK_KIND_COMP_SEQ, tasks):
      t.watch(pysimgrid.csimdag.TASK_STATE_DONE)

    print_("First scheduling iteration:")
    schedule_all_schedulable()
    print_("Starting the simulation...")
    changed = pysimgrid.csimdag.simulate()
    while changed:
      print_("Watchpoint reached, changed tasks:", [t.name for t in changed])
      print_("  Schedulable:", [t.name for t in tasks if t.state == pysimgrid.csimdag.TASK_STATE_SCHEDULABLE])
      print_("  Free hosts:", [h.name for h in hosts])
      schedule_all_schedulable()
      for t in filter(lambda t: t.kind == pysimgrid.csimdag.TASK_KIND_COMP_SEQ and t.state == pysimgrid.csimdag.TASK_STATE_DONE, changed):
        free_hosts.append(t.hosts[0])
      changed = pysimgrid.csimdag.simulate()

    print_("\nFinal state:")
    for t in filter(lambda t: t.kind == pysimgrid.csimdag.TASK_KIND_COMP_SEQ, tasks):
      print_(" ", t.name, t.hosts[0].name, t.start_time, t.finish_time)

    for t in tasks:
      self.assertEqual(t.state, pysimgrid.csimdag.TASK_STATE_DONE)
    print_("Simulation time:", pysimgrid.csimdag.get_clock())


if __name__ == '__main__':
  unittest.main()
