#!/usr/bin/python

import argparse
import json
import signal
import sys

from manager import Manager


def main(hosts_file, dag_file):
    hosts = json.load(hosts_file)
    tasks = json.load(dag_file)

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
            description="Runs DAG on specified hosts via Everest agents",
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('hosts_file', help="Hosts config file", type=argparse.FileType('r'))
    parser.add_argument('dag_file', help="DAG file in JSON format", type=argparse.FileType('r'))
    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    main(**_cli())