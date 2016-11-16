import argparse
import os
import random
import sys


def generate_tasks(num_tasks, input_size, comp_size, output_size):
    tasks = []
    for i in xrange(0, num_tasks):
        task = {
            'name': "task%d" % i
        }
        tasks.append(task)

    # populate task input sizes
    input_sizes = generate_values(input_size, num_tasks)
    for i in xrange(0, num_tasks):
        tasks[i]['input_size'] = input_sizes[i]

    # populate task computational sizes
    comp_sizes = generate_values(comp_size, num_tasks, input_sizes)
    for i in xrange(0, num_tasks):
        tasks[i]['comp_size'] = comp_sizes[i]

    # populate task output sizes
    output_sizes = generate_values(output_size, num_tasks, input_sizes)
    for i in xrange(0, num_tasks):
        tasks[i]['output_size'] = output_sizes[i]

    return tasks


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


def save_as_dot_file(tasks, output_path):
    with open(output_path, "w") as f:
        f.write("digraph G {\n")
        for task in tasks:
            f.write('  root -> %s [size="%e"]\n' % (task['name'], task['input_size']))
            f.write('  %s [size="%e"]\n' % (task['name'], task['comp_size']))
            f.write('  %s -> end [size="%e"]\n' % (task['name'], task['output_size']))
        f.write("}\n")


def main():
    parser = argparse.ArgumentParser(description="Generator of synthetic graphs for bag-of-tasks applications")
    parser.add_argument("num_tasks", type=int, help="number of tasks")
    parser.add_argument("input_size", type=str, help="task input size in bytes")
    parser.add_argument("comp_size", type=str, help="task computational size in flops")
    parser.add_argument("output_size", type=str, help="task output size in bytes")
    parser.add_argument("output_dir", type=str, help="output directory")
    parser.add_argument("num_graphs", type=int, help="number of generated graphs")
    args = parser.parse_args()

    os.mkdir(args.output_dir)

    for i in xrange(0, args.num_graphs):
        tasks = generate_tasks(args.num_tasks, args.input_size, args.comp_size, args.output_size)
        file_name = 'bot_%d_%s_%s_%s_%d.dot' % (args.num_tasks, args.input_size, args.comp_size, args.output_size, i)
        file_path = args.output_dir + '/' + file_name
        save_as_dot_file(tasks, file_path)
        print('Generated file: %s' % file_path)

    return 0


if __name__ == '__main__':
    main()