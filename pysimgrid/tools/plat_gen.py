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
Basic platform generator.

Generates simple star topology (all nodes connected to a central router).

Optional argument *--include_master* allows to add special *master* node that is useful
for some scheduling experiments. Idea is to simulate the submission node. It runs both first and final tasks
thus sending all the workflow inputs and collecting all the outputs.

Usage::

    python -m pysimgrid.tools.plat_gen output_dir num_systems seed system_type
                                       num_hosts host_speed
                                       link_bandwidth link_latency
                                       [--loopback_bandwidth] [--loopback_latency]
                                       [--include_master]

    positional arguments:
      output_dir        path to output directory
      num_systems       number of generated systems
      seed              random seed
      system_type       system type (only 'cluster' is supported in current version)

      num_hosts         number of hosts (excluding optional master host)
      host_speed        host speed in GFLOPS (e.g. '1', '1-10')
      link_bandwidth    link bandwidth in MBps as 'bandwidth[:master_bandwidth]'
                        (e.g. '125', '10-100:100')
      link_latency      link latency in us as 'latency[:master_latency]' (e.g.
                        '10', '10-100:10')

    optional arguments:
      -h, --help            show this help message and exit
      --loopback_bandwidth  loopback link bandwidth in MBps
      --loopback_latency    loopback link latency in us
      --include_master      include special 'master' host into the cluster
"""

from __future__ import print_function

import argparse
import os
import random


def generate_cluster(include_master, num_hosts, host_speed, host_bandwidth, host_latency,
                     master_bandwidth, master_latency, loopback_bandwidth, loopback_latency):
    hosts = []
    links = []
    routes = []

    # loopback link
    loopback_link = {
        "id": "link_loopback",
        "bandwidth": loopback_bandwidth,
        "latency": loopback_latency,
        "sharing_policy": "FATPIPE"
    }
    links.append(loopback_link)

    # master host
    if include_master:
        hosts.append({
            "id": "master",
            "speed": 1
        })
        master_link = {
            "id": "link_master",
            "bandwidth": generate_values(master_bandwidth, 1)[0],
            "latency": generate_values(master_latency, 1)[0],
        }
        links.append(master_link)
        routes.append({
            "src": "master",
            "dst": "router",
            "links": [
                master_link["id"]
            ]
        })
        routes.append({
            "src": "master",
            "dst": "master",
            "symmetrical": "NO",
            "links": [
                loopback_link["id"]
            ]
        })

    # worker hosts
    host_speeds = generate_values(host_speed, num_hosts)
    link_bandwidths = generate_values(host_bandwidth, num_hosts)
    link_latencies = generate_values(host_latency, num_hosts)
    for i in range(0, num_hosts):
        host = {
            "id": "host%d" % i,
            "speed": host_speeds[i]
        }
        hosts.append(host)
        link = {
            "id": "link%d" % i,
            "bandwidth": link_bandwidths[i],
            "latency": link_latencies[i]
        }
        links.append(link)
        routes.append({
            "src": host["id"],
            "dst": "router",
            "links": [
                link["id"]
            ]
        })
        routes.append({
            "src": host["id"],
            "dst": host["id"],
            "symmetrical": "NO",
            "links": [
                loopback_link["id"]
            ]
        })

    system = {
        "hosts": hosts,
        "links": links,
        "routes": routes
    }
    return system


def generate_values(spec, num):
    try:
        # fixed value
        fixed = float(spec)
        values = [fixed] * num

    except ValueError:
        # uniform distribution: min-max
        parts = spec.split("-")
        min_value = float(parts[0])
        max_value = float(parts[1])
        values = [random.uniform(min_value, max_value) for _ in range(0, num)]

    return values


def save_as_xml_file(system, output_path):
    with open(output_path, "w") as f:
        f.write("<?xml version='1.0'?>\n")
        f.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">\n')
        f.write('<platform version="4">\n')
        f.write('  <AS id="AS0" routing="Floyd">\n')

        for host in system["hosts"]:
            f.write('  <host id="%s" core="1" speed="%fGf"/>\n' % (host["id"], host["speed"]))
        f.write("\n")

        for link in system["links"]:
            f.write('  <link id="%s" bandwidth="%fMBps" latency="%fus" sharing_policy="%s"/>\n' % (
                link["id"], link["bandwidth"], link["latency"], link.get("sharing_policy", "SHARED")))
        f.write("\n")

        f.write('  <router id="router"/>\n')
        for route in system["routes"]:
            f.write('  <route src="%s" dst="%s" symmetrical="%s">\n' %
                    (route["src"], route["dst"], route.get("symmetrical", "YES")))
            for link in route["links"]:
                f.write('    <link_ctn id="%s"/>\n' % link)
            f.write('  </route>\n')

        f.write("  </AS>\n")
        f.write("</platform>\n")


def main(output_dir, num_systems, seed, system_type, num_hosts, host_speed, link_bandwidth, link_latency,
         loopback_bandwidth, loopback_latency, include_master):
    random.seed(seed)

    if system_type != 'cluster':
        print('Unsupported system type')
        return 1

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i in range(0, num_systems):
        # parse host/master bandwidth and latency
        if ":" in link_bandwidth:
            parts = link_bandwidth.split(":")
            host_bandwidth = parts[0]
            master_bandwidth = parts[1]
        else:
            host_bandwidth = link_bandwidth
            master_bandwidth = link_bandwidth
        if ":" in link_latency:
            parts = link_latency.split(":")
            host_latency = parts[0]
            master_latency = parts[1]
        else:
            host_latency = link_latency
            master_latency = link_latency

        # generate cluster
        system = generate_cluster(include_master, num_hosts, host_speed,
                                  host_bandwidth, host_latency, master_bandwidth, master_latency,
                                  loopback_bandwidth, loopback_latency)
        file_name = "cluster_%d_%s_%s_%s_%d.xml" % (
            num_hosts, host_speed, link_bandwidth, link_latency, i)

        file_path = output_dir + "/" + file_name
        save_as_xml_file(system, file_path)
        print("Generated file: %s" % file_path)

    return 0


def _cli():
    parser = argparse.ArgumentParser(description="Generator of synthetic systems")
    parser.add_argument("output_dir", type=str, help="path to output directory")
    parser.add_argument("num_systems", type=int, help="number of generated systems")
    parser.add_argument("seed", type=int, help="random seed")
    subparsers = parser.add_subparsers(dest="system_type",
                                       help="system type (only 'cluster' is supported in current version)")

    # cluster
    parser_cluster = subparsers.add_parser("cluster", help="collection of hosts with a flat topology")
    parser_cluster.add_argument("num_hosts", type=int, help="number of hosts (excluding optional master host)")
    parser_cluster.add_argument("host_speed", type=str, help="host speed in GFLOPS (e.g. '1', '1-10')")
    parser_cluster.add_argument("link_bandwidth", type=str,
                                help="link bandwidth in MBps as 'bandwidth[:master_bandwidth]' "
                                     "(e.g. '125', '10-100:100')")
    parser_cluster.add_argument("link_latency", type=str,
                                help="link latency in us as 'latency[:master_latency]' (e.g. '10', '10-100:10')")
    parser_cluster.add_argument("--loopback_bandwidth", type=float, default=5000,
                                help="loopback link bandwidth in MBps")
    parser_cluster.add_argument("--loopback_latency", type=float, default=1,
                                help="loopback link latency in us")
    parser_cluster.add_argument("--include_master", default=True, action="store_true",
                                help="include special 'master' host into the cluster")

    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    main(**_cli())
