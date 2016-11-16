# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


import sys


def show_plot(raw_trace):
  import matplotlib.pyplot as plt
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
      ax.broken_barh(schedule[host], (i*2, 1), facecolors='grey')
  ax.set_ylim(0, len(hosts) * 2 + 1)
  ax.set_xlim(0, end)
  ax.set_yticks(range(len(hosts) * 2))
  ax.grid(True)
  plt.show()

if __name__ == '__main__':
  if len(sys.argv) < 2:
    raise Exception('Trace file is required.')
  with open(sys.argv[1], 'r') as f:
    raw_trace = f.read()
    show_plot(raw_trace)
