import argparse
import collections
import json
import os
import textwrap
import numpy


def groupby(results, condition, asitems=True):
  groups = collections.defaultdict(list)
  for data in results:
    key = condition(data)
    groups[key].append(data)
  return groups.items() if asitems else groups

def par(string):
  return textwrap.dedent(string).strip()

def main():
  parser = argparse.ArgumentParser(description="Experiment results analysis")
  parser.add_argument("input_file", type=str, help="experiment results")
  args = parser.parse_args()

  with open(args.input_file) as input_file:
    results = json.load(input_file)

  # Filter out HCPT for bugginess and bad performance
  # TODO: fix or remove it completely
  results = list(filter(lambda r: r["algorithm"]["name"] != "HCPT", results))

  get_no_hosts = lambda r: int(os.path.basename(r["platform"]).split("_")[1])
  get_task_type = lambda r: os.path.basename(r["tasks"]).rsplit(".", 1)[0]
  get_algorithm = lambda r: r["algorithm"]["name"]
  get_platform = lambda r: os.path.basename(r["platform"])

  # evaluate normalized results
  REFERENCE_ALGO = "OLB"
  for no_hosts, byhost in groupby(results, get_no_hosts):
    for task, bytask in groupby(byhost, get_task_type):
      for platform, byplat in groupby(bytask, get_platform):
        algorithm_results = groupby(byplat, get_algorithm, False)
        focus = algorithm_results[REFERENCE_ALGO][0]
        for algorithm, byalg in algorithm_results.items():
          byalg[0]["normalized"] = byalg[0]["makespan"] / focus["makespan"]

  # print results as latex table
  # a lot of hardcode there for now. not sure if can be avoided without excessively generic code.
  algorithm_order = ["OLB", "MCT", "Random", "RoundRobin", "HEFT", "Lookahead", "PEFT"]
  print(par(r"""
  \begin{table}
  \caption{TODO}
  \begin{center}
     \small\begin{tabular}{*{8}{l}}
  \toprule
  Nodes TODO & OLB  & MCT  & Random   & RoundRobin  & HEFT  & Lookahead & PEFT \\ \midrule
  """))
  for task, bytask in sorted(groupby(results, get_task_type)):
    print(par(r"""
    \multicolumn{8}{l}{%s} \\ \midrule
    """) % (task))
    for no_hosts, byhost in sorted(groupby(bytask, get_no_hosts)):
      print("%-15s" % no_hosts, end="")
      for algorithm, byalg in sorted(groupby(byhost, get_algorithm), key=lambda pair: algorithm_order.index(pair[0])):
        mean = numpy.mean([r["normalized"] for r in byalg])
        print(" & %10.4f" % mean, end="")
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
