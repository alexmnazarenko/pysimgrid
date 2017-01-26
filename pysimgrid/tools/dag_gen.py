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
Random workflow generator.

Uses an DAGGEN tool for actual generation (you will need to download and install it separately).
It can be found in `DAGGEN repository <https://github.com/frs69wq/daggen>`_.

The tool itself converts DAGGEN output to SimGrid friendly format and enables batch generation:
when given multiple arguments, it produces a cartesian product of configurations.

For example, if called like this::

    python -m pysimgrid.tools.dag_gen -n 10 20 --density 0.3 --repeat 5 output_dir

the tool will generate 5 workflows with 10 tasks and 5 workflows with 20 tasks (both using the edge density 0.3).

Usage::

    python -m pysimgrid.tools.dag_gen [-h] [-n [COUNT [COUNT ...]]]
                      [--ccr [{0,1,2,3} [{0,1,2,3} ...]]]
                      [--mindata [MINDATA [MINDATA ...]]]
                      [--maxdata [MAXDATA [MAXDATA ...]]]
                      [--jump [JUMP [JUMP ...]]] [--fat [FAT [FAT ...]]]
                      [--regular [REGULAR [REGULAR ...]]]
                      [--density [DENSITY [DENSITY ...]]] [--repeat REPEAT]
                      daggen_path output_dir

    Synthetic DAG generator

    positional arguments:
      daggen_path           path to daggen executable
      output_dir            path to output directory

    optional arguments:
      -h, --help            show this help message and exit
      -n [COUNT [COUNT ...]], --count [COUNT [COUNT ...]]
                            node count
      --ccr [{0,1,2,3} [{0,1,2,3} ...]]
                            type of CCR (see daggen docs)
      --mindata [MINDATA [MINDATA ...]]
                            min task input size
      --maxdata [MAXDATA [MAXDATA ...]]
                            max task input size
      --jump [JUMP [JUMP ...]]
                            max amount of levels to skip with a connection
      --fat [FAT [FAT ...]]
                            dag width (fat -> 1 = high parallelism)
      --regular [REGULAR [REGULAR ...]]
                            regularity of the distribution of tasks between the
                            different DAG levels
      --density [DENSITY [DENSITY ...]]
                            determines number of connections between different DAG
                            levels
      --repeat REPEAT       number of random graphs for each configuration
"""

import argparse
import itertools
import logging
import os
import subprocess
import networkx as nx

def _import_daggen(line_iter):
    _NODE_TYPES = {"ROOT", "END", "COMPUTATION", "TRANSFER"}
    result = nx.DiGraph()
    node_mapper = lambda nid: "task_%d" % nid
    nodes = {}
    skip = True
    for line in line_iter:
        line = line.strip()
        if line.startswith("NODE_COUNT"):
            skip=False
            continue
        if skip or not line:
            continue
        node_parts = line.split(" ")
        assert len(node_parts) == 6
        magic, nodeid, children, nodetype, cost, parallel_ratio = node_parts
        assert magic == "NODE"
        nodeid = int(nodeid)
        children = list(map(int, children.split(","))) if children != "-" else []
        assert nodetype in _NODE_TYPES
        cost = float(cost)
        # unused_for_now
        parallel_ratio = float(parallel_ratio)
        nodes[nodeid] = (nodetype, children, cost)
    for nodeid, (nodetype, _, cost) in nodes.items():
        if nodetype != "TRANSFER":
            result.add_node(node_mapper(nodeid), weight=cost)
    for nodeid, (nodetype, children, _) in nodes.items():
        if nodetype == "TRANSFER":
            continue
        for childid in children:
            childtype, grandchildren, transfercost = nodes[childid]
            if childtype == "TRANSFER":
                assert len(grandchildren) == 1
                destination = grandchildren[0]
                weight = transfercost
            else:
                assert nodetype == "ROOT" or childtype=="END"
                destination = childid
                # TODO: Should be 0.
                #
                # Kludge to force order in 3rd-party HEFT implementation
                # (nodes connected by edges with zero weight get mixed
                #  in HEFT priority list and violate precedence constraints)
                #
                # Can be removed as I can fix this BS in my HEFT
                weight = 1.
            result.add_edge(node_mapper(nodeid), node_mapper(destination), weight=weight)
    node_order = nx.topological_sort(result)
    return nx.relabel_nodes(result, {
      node_order[0]: "root",
      node_order[-1]: "end"
    })


def import_daggen(path):
    path = os.path.normpath(path)
    with open(path) as f:
        return _import_daggen(f)


def daggen(daggen_path, n=10, ccr=0, mindata=2048, maxdata=11264, jump=1, fat=0.5, regular=0.9, density=0.5):
    daggen_path = os.path.normpath(daggen_path)
    params = [
        ("-n", n),
        ("--ccr", ccr),
        ("--mindata", mindata),
        ("--maxdata", maxdata),
        ("--jump", jump),
        ("--fat", fat),
        ("--regular", regular),
        ("--density", density),
        ("--minalpha", 1),
        ("--maxalpha", 1)
    ]
    if not os.path.isfile(daggen_path):
        raise Exception("daggen executable '{}' does not exist".format(daggen_path))
    args = [daggen_path]
    for name, value in params:
        args.append(name)
        args.append(str(value))
    kwargs = {}
    if hasattr(subprocess, "DEVNULL"):
      kwargs["stderr"] = subprocess.DEVNULL
    output = subprocess.check_output(args, **kwargs)
    return _import_daggen(output.decode("ascii").split("\n"))


def main():
  parser = argparse.ArgumentParser(description="Synthetic DAG generator")
  parser.add_argument("daggen_path", type=str, help="path to daggen executable")
  parser.add_argument("output_dir", type=str, help="path to output directory")
  parser.add_argument("-n", "--count", type=int, nargs="*", default=[10], help="node count")
  parser.add_argument("--ccr", type=int, nargs="*", choices=[0, 1, 2, 3], default=[0], help="type of CCR (see daggen docs)")
  parser.add_argument("--mindata", type=int, nargs="*", default=[2048], help="min task input size")
  parser.add_argument("--maxdata", type=int, nargs="*", default=[11264], help="max task input size")
  parser.add_argument("--jump", type=int, nargs="*", default=[2], help="max amount of levels to skip with a connection")
  parser.add_argument("--fat", type=float, nargs="*", default=[0.5], help="dag width (fat -> 1 = high parallelism)")
  parser.add_argument("--regular", type=float, nargs="*", default=[0.9], help="regularity of the distribution of tasks between the different DAG levels")
  parser.add_argument("--density", type=float, nargs="*", default=[0.5], help="determines number of connections between different DAG levels")
  parser.add_argument("--repeat", type=int, default=1, help="number of random graphs for each configuration")

  args = parser.parse_args()
  if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)

  for config in itertools.product(args.count, args.ccr, args.mindata, args.maxdata, args.jump, args.fat, args.regular, args.density):
    for repeat_idx in range(args.repeat):
      graph = daggen(args.daggen_path, *config)
      output_filename = "daggen_%d_%d_%d_%d_%d_%.3f_%.3f_%.3f_%d.dot" % (config + (repeat_idx,))

      print("Generated %s" % output_filename)
      with open(os.path.join(args.output_dir, output_filename), "w") as output_file:
        output_file.write("digraph G {\n")
        for node, data in graph.nodes(True):
          output_file.write('  %s [size="%f"];\n' % (node, data["weight"]))
        output_file.write("\n");
        for src, dst, data in graph.edges_iter(data=True):
          output_file.write('  %s -> %s [size="%f"];\n' % (src, dst, data["weight"]))
        output_file.write("}\n")


if __name__ == "__main__":
  main()
