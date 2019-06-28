# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2019 Alexey Nazarenko and contributors
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

import argparse
import collections
import functools
import json
import numpy
import os
import textwrap


def get_app_name(item):
    return str(os.path.basename(item["tasks"]).rsplit(".", 1)[0])


def get_system_name(item):
    return str(os.path.basename(item["platform"]).rsplit(".", 1)[0])


def get_algorithm(item):
    return item["algorithm"]["name"]


def filter_results(results, filter):
    selector, value = filter.split("=")
    entity = selector[0]
    pos = int(selector[1:]) - 1
    if entity == 'A':
        return [item for item in results if get_app_name(item).split("_")[pos] == value]
    elif entity == 'S':
        return [item for item in results if get_system_name(item).split("_")[pos] == value]
    else:
        raise Exception("Unsupported selector")


def compute_metric(results, metric, baseline_algo=None):
    for app, byapp in groupby(results, get_app_name):
        for system, bysys in groupby(byapp, get_system_name):
            algorithm_results = groupby(bysys, get_algorithm, False)
            for algorithm, byalg in algorithm_results.items():
                if metric == "makespan":
                    byalg[0]["result"] = byalg[0]["makespan"]
                if metric == "norm_makespan":
                    assert len(algorithm_results[baseline_algo]) == 1
                    baseline = algorithm_results[baseline_algo][0]
                    byalg[0]["result"] = byalg[0]["makespan"] / baseline["makespan"]
                elif metric == "norm_exp_makespan":
                    byalg[0]["result"] = byalg[0]["makespan"] / byalg[0]["expected_makespan"]


def get_group(selector, item):
    app = get_app_name(item)
    system = get_system_name(item)
    entity = selector[0]
    pos = int(selector[1:]) - 1
    if entity == 'A':
        return app.split("_")[pos]
    elif entity == 'S':
        return system.split("_")[pos]
    else:
        raise Exception("Unsupported selector")


def create_group(group_spec):
    label, selector = group_spec.split(":")
    group = {
        "label": label,
        "func": functools.partial(get_group, selector)
    }
    return group


def groupby(results, func, asitems=True):
    groups = collections.defaultdict(list)
    for data in results:
        key = func(data)
        groups[key].append(data)
    return groups.items() if asitems else groups


def group_results(results, algorithms, group1_func, group2_func):
    grouped_results = []
    for c1, bygroup1 in sorted(groupby(results, group1_func)):
        res1 = {'group': c1, 'results': []}
        for c2, bygroup2 in sorted(groupby(bygroup1, group2_func)):
            res2 = {'group': c2, 'results': []}
            for algorithm, byalg in sorted(groupby(bygroup2, get_algorithm), key=lambda pair: algorithms.index(pair[0])):
                mean = numpy.mean([r["result"] for r in byalg])
                std = numpy.std([r["result"] for r in byalg])
                res2['results'].append({'mean': mean, 'std': std})
            res1['results'].append(res2)
        grouped_results.append(res1)
    return grouped_results


def output_plain(results, algorithms, label1, label2, std):
    print("")
    print(("  %s" % label2).ljust(20), end="")
    for alg in algorithms:
        if std is True:
            print(alg.ljust(15), end="")
        else:
            print(alg[:6].ljust(8), end="")
    print("")
    for group1_res in results:
        print("\n%s: %s\n" % (label1, str(group1_res['group'])))
        for group2_res in group1_res['results']:
            print(("  %s" % (str(group2_res['group']))).ljust(20), end="")
            for res in group2_res['results']:
                if std is True:
                    print(("%5.3f (%5.3f)" % (res['mean'], res['std'])).ljust(15), end="")
                else:
                    print(("%5.3f" % res['mean']).ljust(8), end="")
            print("")
    print("")


def par(string):
    return textwrap.dedent(string).strip()


def output_latex(results, algorithms, label1, label2, std):
    print(par(r"""
  \begin{table}
  \caption{TODO}
  \begin{center}
     \small\begin{tabular}{*{%d}{l}}
  \toprule
  %s & %s \\ \midrule
  """) % (len(algorithms) + 1, label2, " &  ".join(algorithms),))
    for group1_res in results:
        print(par(r"""
    \multicolumn{%d}{l}{%s=%s} \\ \midrule
    """) % (len(algorithms) + 1, label1, str(group1_res['group'])))
        for group2_res in group1_res['results']:
            print("%-15s" % str(group2_res['group']), end="")
            for res in group2_res['results']:
                if std is True:
                    print(" & %5.3f (%5.3f)" % (res['mean'], res['std']), end="")
                else:
                    print(" & %5.3f" % res['mean'], end="")
            print(r" \\")
        print(r"\midrule")
    print(par(r"""
  \bottomrule
  \end{tabular}
  \end{center}
  \label{tab:TODO}
  \end{table}
  """))


def main():
    metrics = [
        "makespan",
        "norm_makespan",
        "norm_exp_makespan"
    ]

    output_modes = ["plain", "latex"]

    parser = argparse.ArgumentParser(description="Tool for processing and viewing the results of experiments")
    parser.add_argument("results", type=str, help="path to results file")
    parser.add_argument("-f", "--filter", type=str, help="results filter expression")
    parser.add_argument("-a", "--algorithms", type=str, help="algorithms")
    parser.add_argument("-m", "--metric", type=str, default="norm_makespan", choices=list(metrics), help="metric")
    parser.add_argument("-b", "--baseline", type=str, help="baseline algorithm (for norm_makespan metric)")
    parser.add_argument("-g1", "--group1", type=str, default="Application:A1", help="group 1 specification")
    parser.add_argument("-g2", "--group2", type=str, default="Hosts:S2", help="group 2 specification")
    parser.add_argument("-o", "--output", type=str, default="plain", choices=list(output_modes), help="output mode")
    parser.add_argument("--std", action='store_true', help="print standard deviation")

    args = parser.parse_args()

    with open(args.results) as f:
        results = json.load(f)

    if args.filter is not None:
        results = filter_results(results, args.filter)

    if args.algorithms is not None:
        algorithms = args.algorithms.split(",")
    else:
        algorithms = list(sorted({get_algorithm(item) for item in results}))

    compute_metric(results, args.metric, args.baseline)

    group1 = create_group(args.group1)
    group2 = create_group(args.group2)
    grouped = group_results(results, algorithms, group1.get("func"), group2.get("func"))

    if args.output == 'plain':
        output_plain(grouped, algorithms, group1.get("label"), group2.get("label"), args.std)
    elif args.output == 'latex':
        output_latex(grouped, algorithms, group1.get("label"), group2.get("label"), args.std)


if __name__ == "__main__":
    main()
