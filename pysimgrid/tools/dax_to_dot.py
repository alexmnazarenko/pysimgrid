import argparse
import networkx as nx

from collections import defaultdict

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


def strip_namespace(tag):
    if '}' in tag:
        tag = tag.split('}', 1)[1]
    return tag


class Task:
    def __init__(self, id, name, runtime, input_files={}, output_files={}):
        self.id = id
        self.name = name
        self.runtime = runtime
        self.input_files = input_files
        self.output_files = output_files
        self.parents = set()


class File:
    def __init__(self, name, size, transfer):
        self.name = name
        self.size = size
        self.transfer = transfer


def main():
    parser = argparse.ArgumentParser(description="DAX to DOT converter")
    parser.add_argument("input_file", type=str, help="path to DAX file")
    parser.add_argument("output_file", type=str, help="path to DOT file")
    parser.add_argument("-s", "--speed", type=int, default=4200000000,
                        help="host speed in flop/s used to convert task runtime to flop, "
                             "default value is 4200000000 (see simdag/sd_daxloader.cpp)")
    parser.add_argument("--no-boundary", action="store_true", default=False, help="omit root and end tasks")

    args = parser.parse_args()

    tree = ET.parse(args.input_file)
    tasks = {}
    file_producers = defaultdict(list)
    file_consumers = defaultdict(list)

    root = Task('root', 'root', 0.)
    end = Task('end', 'end', 0.)
    tasks[root.id] = root
    tasks[end.id] = end

    for el in tree.getroot():
        tag = strip_namespace(el.tag)
        if tag == 'job':
            task_id = el.attrib['id']
            task_name = el.attrib['name']
            runtime = float(el.attrib['runtime'])
            input_files = {}
            output_files = {}
            for sub_el in el:
                sub_tag = strip_namespace(sub_el.tag)
                if sub_tag == 'uses':
                    file_name = sub_el.attrib['file']
                    file_size = float(sub_el.attrib['size'])
                    link = sub_el.attrib['link']
                    transfer = sub_el.attrib['transfer']

                    file = File(file_name, file_size, transfer)

                    if link == 'input':
                        input_files[file_name] = file
                        file_consumers[file_name].append(task_id)
                    elif link == 'output':
                        output_files[file_name] = file
                        # assert file_name not in file_producers, \
                        #     "Multiple producers for file %s: %s, %s" % (file_name, file_producers[file_name], task_id)
                        file_producers[file_name].append(task_id)
            task = Task(task_id, task_name, runtime, input_files, output_files)
            assert task_id not in tasks, "Duplicate task id"
            tasks[task_id] = task
            # dag.add_node(task, weight=task.runtime)
        elif tag == 'child':
            task = tasks[el.attrib['ref']]
            for sub_el in el:
                sub_tag = strip_namespace(sub_el.tag)
                if sub_tag == 'parent':
                    parent = tasks[sub_el.attrib['ref']]
                    task.parents.add(parent.id)

    dag = nx.DiGraph()

    for task in tasks.values():
        weight = task.runtime * args.speed
        dag.add_node(task, weight=weight)

    for task in tasks.values():
        for file in task.input_files.values():
            if file.name in file_producers:
                # assert file.transfer == 'false'
                producers = file_producers[file.name]
            else:
                # input files not produced in DAG are outputs of the end task
                # assert file.transfer == 'true', 'Wrong file transfer for file %s in task %s?' % (file.name, task.id)
                root = tasks['root']
                if file.name not in root.output_files:
                    root.output_files[file.name] = file
                producers = [root.id]
                task.parents.add(root.id)
            for producer_id in producers:
                if producer_id in task.parents:
                    parent = tasks[producer_id]
                    parent_file = parent.output_files[file.name]
                    weight = file.size
                    try:
                        assert parent_file.size == file.size, \
                            "File %s sizes differ: producer %s %f, consumer %s %f)" % \
                            (file.name, parent.id, parent_file.size, task.id, file.size)
                    except AssertionError as e:
                        print(e)
                        weight = parent_file.size

                    # pysimgrid does not support multigraphs, i.e. multiple data transfers between same tasks
                    # (see simulation.get_task_graph())
                    # therefore multiple data transfers are converted to a single edge with total data size
                    if not dag.has_edge(parent, task):
                        dag.add_edge(parent, task, weight=weight)
                    else:
                        print("!!! Duplicate edge: %s -> %s" % (parent.id, task.id))
                        dag[parent][task]['weight'] += weight

        # output files not consumed in DAG are inputs to the end task
        for file in task.output_files.values():
            if file.name not in file_consumers or len(file_consumers[file.name]) == 0:
                assert file.transfer == 'true'
                if not dag.has_edge(task, tasks['end']):
                    dag.add_edge(task, tasks['end'], weight=file.size)
                    tasks['end'].parents.add(task.id)
                else:
                    dag[task][tasks['end']]['weight'] += file.size
            # else:
            #     assert file.transfer == 'false', 'Wrong file transfer for file %s in task %s?' % (file.name, task.id)

        # check that edges for all parent tasks exist
        for parent_id in task.parents:
            parent = tasks[parent_id]
            assert dag.has_edge(parent, task), "Non-data dependency: %s -> %s" % (parent_id, task.id)

        # check that no edges for non-parent tasks exist
        for src, dst in dag.edges():
            assert src.id in dst.parents, "Wrong edge, %s is not parent of %s" % (src.id, dst.id)

    with open(args.output_file, 'w') as out:
        out.write('digraph DAG {\n')
        out.write('  ranksep=5.0\n')
        out.write('  node [style=filled,color="#444444",fillcolor="#ffed6f"]\n')
        out.write('  edge [arrowhead=normal,arrowsize=1.0]\n')
        out.write("\n")
        for task, data in dag.nodes(True):
            if not args.no_boundary or task.id not in ['root', 'end']:
                if task.name != task.id:
                    task_label = task.name + "_" + task.id
                else:
                    task_label = task.name
                out.write('  %s [label="%s",size="%e"];\n' % (task.id, task_label, data["weight"]))
        out.write("\n")
        for src, dst, data in dag.edges(data='weight'):
            if not args.no_boundary or (src.id not in ['root', 'end'] and dst.id not in ['root', 'end']):
                out.write('  %s -> %s [size="%e"];\n' % (src.id, dst.id, data))
        out.write("}\n")


if __name__ == "__main__":
    main()
