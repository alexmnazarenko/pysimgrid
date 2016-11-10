import argparse
import os
import random
import sys


def generate_system(num_hosts, host_speed, link_bandwidth, link_latency):
    hosts = []
    links = []
    routes = []

    # master host
    master = {
        'id': 'master',
        'speed': 1e10
    }
    hosts.append(master)

    host_speeds = generate_values(host_speed, num_hosts)
    link_bandwidths = generate_values(link_bandwidth, num_hosts)
    link_latencies = generate_values(link_latency, num_hosts)
    for i in xrange(0, num_hosts):
        host = {
            'id': "host%d" % i,
            'speed': host_speeds[i]
        }
        hosts.append(host)
        link = {
            'id': "link%d" % i,
            'bandwidth': link_bandwidths[i],
            'latency': link_latencies[i]
        }
        links.append(link)
        route = {
            'src': 'master',
            'dst': host['id'],
            'link': link['id']
        }
        routes.append(route)

    system = {
        'hosts': hosts,
        'links': links,
        'routes': routes
    }
    return system


def generate_values(spec, num, inputs=None):
    try:
        # fixed value
        fixed = float(spec)
        values = [fixed] * num

    except ValueError:
        parts = spec.split(':')
        type = parts[0]
        params = parts[1:]

        # uniform distribution: u:min:max
        if type == "u":
            min = float(params[0])
            max = float(params[1])
            values = [random.uniform(min, max) for _ in xrange(0, num)]

        # normal distribution: n:mean:std_dev
        elif type == "n":
            mean = float(params[0])
            std_dev = float(params[1])
            values = [random.normalvariate(mean, std_dev) for _ in xrange(0, num)]

        # scaled values: x:factor
        elif type == "x":
            factor = float(params[0])
            if inputs is not None:
                values = [inputs[i] * factor for i in xrange(0, num)]
            else:
                print "Inputs are not specified"
                sys.exit(-1)

        else:
            print "Unknown distribution"
            sys.exit(-1)

    return values


def save_as_xml_file(system, output_path):
    with open(output_path, "w") as f:
        f.write("<?xml version='1.0'?>\n")
        f.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">\n')
        f.write('<platform version="4">\n')
        f.write('  <AS id="AS0" routing="Full">\n')
        for host in system['hosts']:
            f.write('    <host id="%s" core="1" speed="%eflops"/>\n' % (host['id'], host['speed']))
        f.write("\n")
        for link in system['links']:
            f.write('    <link id="%s" bandwidth="%eBps" latency="%es"/>\n' % (link['id'], link['bandwidth'], link['latency']))
        f.write("\n")
        for route in system['routes']:
            f.write('    <route src="%s" dst="%s"><link_ctn id="%s"/></route>\n' % (route['src'], route['dst'], route['link']))
        f.write("  </AS>\n")
        f.write("</platform>\n")


def main():
    parser = argparse.ArgumentParser(description="Generator of synthetic systems for running bag-of-tasks applications")
    parser.add_argument("num_hosts", type=int, help="number of hosts")
    parser.add_argument("host_speed", type=str, help="host speed (flops)")
    parser.add_argument("link_bandwidth", type=str, help="link bandwidth (bytes/second)")
    parser.add_argument("link_latency", type=str, help="link latency (seconds)")
    parser.add_argument("output_dir", type=str, help="output directory")
    parser.add_argument("num_files", type=int, help="number of generated systems")
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    for i in xrange(0, args.num_files):
        system = generate_system(args.num_hosts, args.host_speed, args.link_bandwidth, args.link_latency)
        file_name = 'simple_%d_%s_%s_%s_%d.xml' % (args.num_hosts, args.host_speed, args.link_bandwidth, args.link_latency, i)
        file_path = args.output_dir + '/' + file_name
        save_as_xml_file(system, file_path)
        print('Generated file: %s' % file_path)

    return 0


if __name__ == '__main__':
    main()