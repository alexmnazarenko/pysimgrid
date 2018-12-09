"""
Experiment results analysis tool.

Not yet generic. Should be included in pysimgrid.tools when is.
"""

from __future__ import print_function

import argparse
import collections
import json
import numpy
import os
import textwrap


def groupby(results, condition, asitems=True):
    groups = collections.defaultdict(list)
    for data in results:
        key = condition(data)
        groups[key].append(data)
    return groups.items() if asitems else groups


def get_app_filename(item):
    return str(os.path.basename(item["tasks"]).rsplit(".", 1)[0])


def get_app_group(item):
    return "_".join(get_app_filename(item).split("_")[:2])


def get_fat(item):
    return float(get_app_filename(item).split("_")[2])


def get_ccr(item):
    return float(get_app_filename(item).split("_")[8])


def get_platform_name(item):
    return os.path.basename(item["platform"])


def get_host_count(item):
    return int(get_platform_name(item).split("_")[1])


def get_algorithm(item):
    return item["algorithm"]["name"]


def main():
    algorithms = ["OLB", "MCT", "MinMin", "MaxMin", "Sufferage", "DLS", "HEFT", "HCPT", "Lookahead", "PEFT"]

    metrics = [
        "makespan",
        "norm_makespan",
        "norm_exp_makespan"
    ]

    groups = {
        "app": {"label": "Application", "cond": get_app_group},
        "hcount": {"label": "Hosts", "cond": get_host_count},
        "fat": {"label": "Fat", "cond": get_fat},
        "ccr": {"label": "CCR", "cond": get_ccr}
    }

    output_modes = ["plain", "latex"]

    parser = argparse.ArgumentParser(description="Experiment results analysis")
    parser.add_argument("input_file", type=str, help="experiment results")
    parser.add_argument("-a", "--algorithms", type=str, default=",".join(algorithms), help="algorithms")
    parser.add_argument("-m", "--metric", type=str, default="norm_makespan", choices=list(metrics), help="metric")
    parser.add_argument("-g1", "--group1", type=str, default="app", choices=list(groups), help="group 1")
    parser.add_argument("-g2", "--group2", type=str, default="hcount", choices=list(groups), help="group 2")
    parser.add_argument("-o", "--output", type=str, default="plain", choices=list(output_modes), help="output mode")
    parser.add_argument("--std", action='store_true', help="print standard deviation")

    args = parser.parse_args()
    algorithms = args.algorithms.split(',')
    group1 = groups.get(args.group1)
    group2 = groups.get(args.group2)

    with open(args.input_file) as input_file:
        results = json.load(input_file)

    compute_metric(results, args.metric)
    grouped = group_results(results, algorithms, group1.get("cond"), group2.get("cond"))

    if args.output == 'plain':
        output_plain(grouped, algorithms, group1.get("label"), group2.get("label"), args.std)
    elif args.output == 'latex':
        output_latex(grouped, algorithms, group1.get("label"), group2.get("label"), args.std)


def compute_metric(results, metric):
    for task, bytask in groupby(results, get_app_filename):
        for platform, byplat in groupby(bytask, get_platform_name):
            algorithm_results = groupby(byplat, get_algorithm, False)
            for algorithm, byalg in algorithm_results.items():
                if metric == "makespan":
                    byalg[0]["result"] = byalg[0]["makespan"]
                if metric == "norm_makespan":
                    reference_algo = "OLB"
                    assert len(algorithm_results[reference_algo]) == 1
                    reference = algorithm_results[reference_algo][0]
                    byalg[0]["result"] = byalg[0]["makespan"] / reference["makespan"]
                elif metric == "norm_exp_makespan":
                    byalg[0]["result"] = byalg[0]["expected_makespan"] / byalg[0]["makespan"]


def group_results(results, algorithms, cond1, cond2):
    grouped_results = []
    for c1, bycond1 in sorted(groupby(results, cond1)):
        res1 = {'group': c1, 'results': []}
        for c2, bycond2 in sorted(groupby(bycond1, cond2)):
            res2 = {'group': c2, 'results': []}
            for algorithm, byalg in sorted(groupby(bycond2, get_algorithm), key=lambda pair: algorithms.index(pair[0])):
                mean = numpy.mean([r["result"] for r in byalg])
                std = numpy.std([r["result"] for r in byalg])
                res2['results'].append({'mean': mean, 'std': std})
            res1['results'].append(res2)
        grouped_results.append(res1)
    return grouped_results


def output_plain(results, algorithms, label1, label2, std):
    print("".ljust(16), end="")
    for alg in algorithms:
        if std is True:
            print(alg.ljust(15), end="")
        else:
            print(alg[:6].ljust(8), end="")
    print("")
    for group1_res in results:
        print("%s=%s" % (label1, str(group1_res['group'])))
        for group2_res in group1_res['results']:
            print(("  %s=%s" % (label2, str(group2_res['group']))).ljust(16), end="")
            for res in group2_res['results']:
                if std is True:
                    print(("%5.3f (%5.3f)" % (res['mean'], res['std'])).ljust(15), end="")
                else:
                    print(("%5.3f" % res['mean']).ljust(8), end="")
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


if __name__ == "__main__":
    main()
