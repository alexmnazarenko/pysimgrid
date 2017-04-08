#!/usr/bin/python

import argparse
import json
import re
import signal
import sys
from pprint import pprint

try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree

from manager import Manager


def parse_platform_file(platform_file, hosts):
    hosts_idx = {}
    for idx, host in enumerate(hosts):
        hosts_idx[host['id']] = idx
    new_hosts = []

    tree = ElementTree.ElementTree(file=platform_file)
    for as_elem in tree.getroot():
        for elem in as_elem:
            if elem.tag == 'host':
                host_id = elem.attrib['id']
                host_speed = float(elem.attrib['speed'].replace('Gf', ''))
                if host_id in hosts_idx:
                    host = hosts[hosts_idx[host_id]]
                    host['speed'] = host_speed
                    new_hosts.append(host)
                    # print('Added host %s with speed %f' % (host_id, host_speed))
                else:
                    print('Unknown host: %s' % host_id)
                    sys.exit(1)
    return new_hosts


DOT_NODE_REGEX = re.compile("^\s*([\w\d_]+)\s+\[size=\"([\d.e]+)\"\];$")
DOT_EDGE_REGEX = re.compile('^\s*([\w\d_]+)\s+->\s+([\w\d_]+)\s+\[size=\"([\d.e]+)\"\];$')


def parse_dot_file(dot_file):
    tasks = []
    tasks_idx = {}
    task_outputs = {}

    for line in dot_file:

        match = re.match(DOT_NODE_REGEX, line)
        if match is not None:
            task_id = match.group(1)
            task_size = float(match.group(2)) / 1E9  # convert to Gflop!
            task = {
                'id': task_id,
                'spec': {
                    "command": "synthetic_task.py %.6f" % task_size,
                    "inputData": [],
                    "outputData": []
                }
            }
            tasks_idx[task_id] = len(tasks)
            tasks.append(task)
            task_outputs[task_id] = {}
            # print('Added task %s with size %f' % (task_id, task_size))
            continue

        match = re.match(DOT_EDGE_REGEX, line)
        if match is not None:
            parent_id = match.group(1)
            child_id = match.group(2)
            output_size = float(match.group(3)) / 1E6  # convert to Mbytes!

            parent_task = tasks[tasks_idx[parent_id]]
            output_file = None
            for file_name, file_size in task_outputs[parent_id].iteritems():
                if file_size == output_size:
                    output_file = file_name
                    break
            if output_file is None:
                parent_outputs = parent_task['spec']['outputData']
                if len(parent_outputs) == 0:
                    parent_outputs.append({'paths': []})
                paths = parent_outputs[0]['paths']
                output_file = 'out_%d' % (len(paths) + 1)
                parent_task['spec']['command'] += ' %s:%.7f' % (output_file, output_size)
                paths.append(output_file)
                task_outputs[parent_id][output_file] = output_size

            child_task = tasks[tasks_idx[child_id]]
            child_inputs = child_task['spec']['inputData']
            input_file = 'in_%d' % (len(child_inputs) + 1)
            child_inputs.append({
                'path': input_file,
                'uri': '$%s.%s' % (parent_id, output_file)
            })

    return tasks


def main(hosts_file, platform_file, dag_file, print_only):
    hosts = json.load(hosts_file)
    hosts = parse_platform_file(platform_file, hosts)
    pprint(hosts)
    tasks = parse_dot_file(dag_file)
    pprint(tasks)

    if not print_only:
        manager = Manager(hosts)

        def sig_handler(sig, frame):
            manager.shutdown()
            sys.exit(0)
        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        manager.run(tasks)
        manager.shutdown()


def _cli():
    parser = argparse.ArgumentParser(
            description="Runs DAG on specified hosts via Everest agents (SimGrid version)",
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('hosts_file', help="Hosts config file", type=argparse.FileType('r'))
    parser.add_argument('platform_file', help="Platform description in XML format", type=argparse.FileType('r'))
    parser.add_argument('dag_file', help="DAG description in DOT format", type=argparse.FileType('r'))
    parser.add_argument('--print-only', help="Print parsed hosts and tasks",
                        dest='print_only', action='store_true', default=False)
    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    main(**_cli())
