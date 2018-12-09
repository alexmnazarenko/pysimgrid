# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2018 Alexey Nazarenko and contributors
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

Uses an DAGGEN tool for DAG generation (you will need to download and install it separately).
It can be found in `DAGGEN repository <https://github.com/frs69wq/daggen>`_.

The tool itself converts DAGGEN output to SimGrid friendly format and enables batch generation:
when given multiple arguments, it produces a cartesian product of configurations.

For example, if called like this::

    python -m pysimgrid.tools.dag_gen -n 10 20 --density 0.3 --repeat 5 output_dir

the tool will generate 5 workflows with 10 tasks and 5 workflows with 20 tasks (both using the edge density 0.3).

Usage::

    python -m pysimgrid.tools.dag_gen [-h]
                      [-n [COUNT [COUNT ...]]]
                      [--fat [FAT [FAT ...]]]
                      [--regular [REGULAR [REGULAR ...]]]
                      [--density [DENSITY [DENSITY ...]]]
                      [--jump [JUMP [JUMP ...]]]
                      [--mindata [MINDATA [MINDATA ...]]]
                      [--maxdata [MAXDATA [MAXDATA ...]]]
                      [--ccr [CCR [CCR ...]]]
                      [--repeat REPEAT]
                      [--seed SEED]
                      daggen_path output_dir

    Synthetic DAG generator

    positional arguments:
      output_dir            path to output directory

    optional arguments:
      -h, --help            show this help message and exit
      -n [COUNT [COUNT ...]], --count [COUNT [COUNT ...]]
                            task count
      --fat [FAT [FAT ...]]
                            dag width (fat -> 1 = high parallelism)
      --regular [REGULAR [REGULAR ...]]
                            regularity of the distribution of tasks between the
                            different DAG levels
      --density [DENSITY [DENSITY ...]]
                            determines number of connections between different DAG
                            levels
      --jump [JUMP [JUMP ...]]
                            max amount of levels to skip with a connection
      --mindata [MINDATA [MINDATA ...]]
                            min task input size in bytes
      --maxdata [MAXDATA [MAXDATA ...]]
                            max task input size in bytes
      --ccr [CCR [CCR ...]]
                            communication-to-computation ratio in MBs/Gflops
      --repeat REPEAT       number of random graphs for each configuration
      --seed SEED           random seed
"""

import argparse
import itertools
import networkx as nx
import os
import random
import subprocess


def import_daggen(line_iter):
    _NODE_TYPES = {"ROOT", "END", "COMPUTATION", "TRANSFER"}
    result = nx.DiGraph()
    nodes = {}
    skip = True
    for line in line_iter:
        line = line.strip()
        if line.startswith("NODE_COUNT"):
            skip = False
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
    task_ids = {}
    task_count = 0
    node_mapper = lambda nid: "task_%d" % task_ids[nid]
    for nodeid, (nodetype, _, cost) in nodes.items():
        if nodetype != "TRANSFER":
            task_ids[nodeid] = task_count
            task_count += 1
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
                assert nodetype == "ROOT" or childtype == "END"
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
    node_order = list(nx.topological_sort(result))
    return nx.relabel_nodes(result, {
        node_order[0]: "root",
        node_order[-1]: "end"
    })


def compute_weights(graph, mindata, maxdata, ccr, scatter_gather=False):
    # converting CCR from MBytes/GFlops to bytes/flops
    ccr = ccr / 1000.0
    for node in graph:
        if node in ["root", "end"]:
            continue
        input_size = random.uniform(mindata, maxdata)
        num_parents = len(graph.pred[node].values())
        edge_size = input_size / num_parents
        for edge in graph.pred[node].values():
            edge["weight"] = edge_size
        graph.node[node]["weight"] = input_size / ccr
    if not scatter_gather:
        # root -> x edges are effectively zero
        for edge in graph.succ["root"].values():
            edge["weight"] = 1e-12
        # x -> end edges are effectively zero
        for edge in graph.pred["end"].values():
            edge["weight"] = 1e-12
    return graph


def daggen(daggen_path, n, fat, regular, density, jump, mindata, maxdata, ccr):
    daggen_path = os.path.normpath(daggen_path)
    params = [
        ("-n", n),
        ("--fat", fat),
        ("--regular", regular),
        ("--density", density),
        ("--jump", jump)
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
    graph = import_daggen(output.decode("ascii").split("\n"))
    return compute_weights(graph, mindata, maxdata, ccr)


def main():
    parser = argparse.ArgumentParser(description="Synthetic DAG generator")
    parser.add_argument("output_dir", type=str,
                        help="path to output directory")
    parser.add_argument("-n", "--count", type=int, nargs="*", default=[10],
                        help="task count")
    parser.add_argument("--fat", type=float, nargs="*", default=[0.5],
                        help="dag width (fat -> 1 = high parallelism)")
    parser.add_argument("--regular", type=float, nargs="*", default=[0.9],
                        help="regularity of the distribution of tasks between the different DAG levels")
    parser.add_argument("--density", type=float, nargs="*", default=[0.5],
                        help="determines number of connections between different DAG levels")
    parser.add_argument("--jump", type=int, nargs="*", default=[2],
                        help="max amount of levels to skip with a connection")
    parser.add_argument("--mindata", type=float, nargs="*", default=[1e6],
                        help="min task input size in bytes")
    parser.add_argument("--maxdata", type=float, nargs="*", default=[1e9],
                        help="max task input size in bytes")
    parser.add_argument("--ccr", type=float, nargs="*", default=[1.],
                        help="communication-to-computation ratio in MBs/Gflops")
    parser.add_argument("--repeat", type=int, default=1,
                        help="number of random graphs for each configuration")
    parser.add_argument("--seed", type=int, default=314,
                        help="random seed")

    args = parser.parse_args()

    daggen_path = os.environ.get('DAGGEN_PATH')

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    seed = args.seed
    random.seed(seed)

    for config in itertools.product(args.count, args.fat, args.regular, args.density, args.jump,
                                    args.mindata, args.maxdata, args.ccr):
        for repeat_idx in range(args.repeat):
            graph = daggen(daggen_path, *config)
            output_filename = "daggen_%d_%.2f_%.2f_%.2f_%d_%1.0e_%1.0e_%.2f_%d.dot" % (config + (repeat_idx,))
            with open(os.path.join(args.output_dir, output_filename), "w") as output_file:
                output_file.write("digraph G {\n")
                for node, data in graph.nodes(True):
                    output_file.write('  %s [size="%e"];\n' % (node, data["weight"]))
                output_file.write("\n")
                for src, dst, data in graph.edges(data='weight'):
                    output_file.write('  %s -> %s [size="%e"];\n' % (src, dst, data))
                output_file.write("}\n")
            print("Generated %s" % output_filename)


if __name__ == "__main__":
    main()
