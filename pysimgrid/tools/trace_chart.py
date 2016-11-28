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


import argparse
import sys
import matplotlib.pyplot as plt


def show_plot(raw_trace):
  trace = raw_trace.strip().split('\n')

  types  = {}
  hosts = {}
  links = {}
  schedule = {}
  end = 0

  for line in trace:
    if line.startswith('0 '):
      _, alias, type_, name = line.strip().split(' ')
      types[alias] = name
    elif line.startswith('6 '):
      _, _, alias, type_, container, name = line.strip().split(' ')
      if types[type_] == 'HOST':
          hosts[alias] = name
    elif line.startswith('8 '):
      # PageSetVariable
      # Init the work on the schedule
      _, time, type_, container, value = line.strip().split(' ')
      if container in hosts:
        schedule[hosts[container]] = []
    elif line.startswith('9 '):
      # PajeAddVariable
      # Start to calculate a task
      _, time, type_, container, value = line.strip().split(' ')
      if container in hosts:
        schedule[hosts[container]].append([float(time), None])
    elif line.startswith('10 '):
      # PajeSubVariable
      # Finish to calculate a task
      _, time, type_, container, value = line.strip().split(' ')
      if container in hosts:
        schedule[hosts[container]][-1][1] = float(time) - schedule[hosts[container]][-1][0]
        end = max(end, float(time))

  fig, ax = plt.subplots()
  for i, host in enumerate(sorted(schedule)):
      ax.broken_barh(schedule[host], (i*2, 1), facecolors="grey")
  ax.set_ylim(0, len(hosts) * 2 + 1)
  ax.set_xlim(0, end)
  ax.set_yticks(range(len(hosts) * 2))
  ax.grid(True)
  plt.show()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Host utilization visualization from SimGrid Paje trace files")
  parser.add_argument("trace_file", type=str, help="path to trace file to visualize")
  args = parser.parse_args()
  with open(args.trace_file, "r") as f:
    raw_trace = f.read()
    show_plot(raw_trace)
