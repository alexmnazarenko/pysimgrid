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
Bag of tasks (BOT) problem generator.

Usage::

    python -m pysimgrid.tools.bot_gen [-h]
                                      output_dir num_graphs seed 
                                      num_tasks input_size 
                                      comp_size output_size
                            

    Generator of synthetic graphs for bag-of-tasks applications

    positional arguments:
      output_dir   output directory
      num_graphs   number of generated graphs
      seed         random seed
      num_tasks    number of tasks
      input_size   task input size in bytes
      comp_size    task computational size in flops
      output_size  task output size in bytes

    optional arguments:
      -h, --help   show this help message and exit
"""

import argparse
import os
import random
import sys


def generate_tasks(num_tasks, input_size, comp_size, output_size):
    tasks = []
    for i in range(0, num_tasks):
        task = {
            'name': "task%d" % i
        }
        tasks.append(task)

    # populate task input sizes
    input_sizes = generate_values(input_size, num_tasks)
    for i in range(0, num_tasks):
        tasks[i]['input_size'] = input_sizes[i]

    # populate task computational sizes
    comp_sizes = generate_values(comp_size, num_tasks, input_sizes)
    for i in range(0, num_tasks):
        tasks[i]['comp_size'] = comp_sizes[i]

    # populate task output sizes
    output_sizes = generate_values(output_size, num_tasks, input_sizes)
    for i in range(0, num_tasks):
        tasks[i]['output_size'] = output_sizes[i]

    return tasks


def generate_values(spec, num, inputs=None):
    try:
        # fixed value
        fixed = float(spec)
        values = [fixed] * num

    except ValueError:
        parts = spec.split(':')
        dist_type = parts[0]
        params = parts[1:]

        # uniform distribution: u:min:max
        if dist_type == "u":
            min_value = float(params[0])
            max_value = float(params[1])
            values = [random.uniform(min_value, max_value) for _ in range(0, num)]

        # normal distribution: n:mean:std_dev
        elif dist_type == "n":
            mean = float(params[0])
            std_dev = float(params[1])
            values = [random.normalvariate(mean, std_dev) for _ in range(0, num)]

        # scaled values: x:factor
        elif dist_type == "x":
            factor = float(params[0])
            if inputs is not None:
                values = [inputs[i] * factor for i in range(0, num)]
            else:
                print("Inputs are not specified")
                sys.exit(-1)

        else:
            print("Unknown distribution")
            sys.exit(-1)

    return values


def save_as_dot_file(tasks, output_path):
    with open(output_path, "w") as f:
        f.write("digraph G {\n")
        for task in tasks:
            f.write('  root -> %s [size="%e"]\n' % (task['name'], task['input_size']))
            f.write('  %s [size="%e"]\n' % (task['name'], task['comp_size']))
            f.write('  %s -> end [size="%e"]\n' % (task['name'], task['output_size']))
        f.write("}\n")


def main(output_dir, num_graphs, seed, num_tasks, input_size, comp_size, output_size):
    random.seed(seed)
    os.mkdir(output_dir)
    for i in range(0, num_graphs):
        tasks = generate_tasks(num_tasks, input_size, comp_size, output_size)
        file_name = 'bot_%d_%s_%s_%s_%d.dot' % (num_tasks, input_size, comp_size, output_size, i)
        file_path = output_dir + '/' + file_name
        save_as_dot_file(tasks, file_path)
        print('Generated file: %s' % file_path)
    return 0


def _cli():
    parser = argparse.ArgumentParser(description="Generator of synthetic graphs for bag-of-tasks applications")
    parser.add_argument("output_dir", type=str, help="output directory")
    parser.add_argument("num_graphs", type=int, help="number of generated graphs")
    parser.add_argument("seed", type=int, help="random seed")
    parser.add_argument("num_tasks", type=int, help="number of tasks")
    parser.add_argument("input_size", type=str, help="task input size in bytes")
    parser.add_argument("comp_size", type=str, help="task computational size in flops")
    parser.add_argument("output_size", type=str, help="task output size in bytes")
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    main(**_cli())
