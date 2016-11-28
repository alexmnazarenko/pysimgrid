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

  results = list(filter(lambda r: r["algorithm"]["name"] != "HCPT", results))

  get_no_hosts = lambda r: int(os.path.basename(r["platform"]).split("_")[1])
  get_task_type = lambda r: os.path.basename(r["tasks"]).rsplit(".", 1)[0]
  get_algorithm = lambda r: r["algorithm"]["name"]
  get_platform = lambda r: os.path.basename(r["platform"])

  for no_hosts, byhost in groupby(results, get_no_hosts):
    for task, bytask in groupby(byhost, get_task_type):
      for platform, byplat in groupby(bytask, get_platform):
        algorithm_results = groupby(byplat, get_algorithm, False)
        focus = algorithm_results["OLB"][0]
        for algorithm, byalg in algorithm_results.items():
          byalg[0]["normalized"] = byalg[0]["makespan"] / focus["makespan"]

  """

\multicolumn{7}{l}{Степень неоднородности задач: 2} \\ \midrule
5             & 1.562   & 1.067       & 1.0   & 1.200   & 1.405   & 1.017     \\
10            & 1.763   & 1.071       & 1.0   & 0.965   & 0.971   & 1.000     \\
20            & 1.776   & 1.069       & 1.0   & 0.964   & 0.936   & 1.000     \\ \midrule
\multicolumn{7}{l}{Степень неоднородности задач: 10} \\ \midrule
5             & 1.598   & 1.186       & 1.0   & 1.002   & 1.357   & 1.022     \\
10            & 1.795   & 1.239       & 1.0   & 0.972   & 0.940   & 0.997     \\
20            & 1.798   & 1.242       & 1.0   & 0.978   & 0.909   & 0.999     \\ \midrule
\multicolumn{7}{l}{Степень неоднородности задач: 100} \\ \midrule
5             & 1.613   & 1.217       & 1.0   & 0.971   & 1.346   & 1.057     \\
10            & 1.841   & 1.299       & 1.0   & 0.979   & 0.937   & 0.998     \\
20            & 1.831   & 1.281       & 1.0   & 0.983   & 0.901   & 0.993     \\ \midrule
\bottomrule
\end{tabular}
\end{center}
\label{tab:bot-exp4}
\end{table}
  """

  algorithm_order = ["OLB", "MCT", "Random", "RoundRobin", "HEFT", "Lookahead", "PEFT"]
  print(par(r"""
  \begin{table}
  \caption{Результаты экспериментов TODO}
  \begin{center}
     \small\begin{tabular}{*{8}{l}}
  \toprule
  Количество узлов & OLB  & MCT  & Random   & RoundRobin  & HEFT  & Lookahead & PEFT \\ \midrule
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
