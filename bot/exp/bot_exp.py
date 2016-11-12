from __future__ import print_function

import abc
import argparse
import logging
import multiprocessing
import numpy as np
import os
import random

from pysimgrid import simdag

_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_FORMAT = "[%(levelname)5s] [%(asctime)s] %(message)s"


# STATIC SCHEDULERS ----------------------------------------------------------------------------------------------------

class StaticScheduler(simdag.DynamicScheduler):
    def __init__(self, simulation):
        super(StaticScheduler, self).__init__(simulation)
        self.tasks = simulation.tasks[simdag.TaskState.TASK_STATE_NOT_SCHEDULED]
        self.master = simulation.hosts.by_prop('name', 'master')[0]
        self.hosts = simulation.hosts.by_prop('name', 'master', negate=True)
        self.host_tasks = {}
        self.host_states = {}
        for host in self.hosts:
            self.host_states[host.name] = 'FREE'
            self.host_tasks[host.name] = []

    @abc.abstractmethod
    def prepare(self, simulation):
        raise NotImplementedError()

    def schedule(self, simulation, changed):
        logging.debug("%.2f -----------------------------------------------------------------------" % simulation.clock)
        # mark freed hosts
        for task in changed:
            if task.kind == simdag.TaskKind.TASK_KIND_COMP_SEQ and task.state == simdag.TaskState.TASK_STATE_DONE:
                host = task.hosts[0]
                if host.name != 'master':
                    self.host_states[host.name] = 'FREE'
                logging.debug("%20s: %s (%s)" % (task.name, str(task.state), host.name))
            else:
                logging.debug("%20s: %s" % (task.name, str(task.state)))
        # schedule tasks on free hosts
        for host in self.hosts:
            if self.host_states[host.name] == 'FREE':
                tasks = self.host_tasks[host.name]
                if len(tasks) > 0:
                    task = tasks.pop(0)
                    task.schedule(host)
                    self.host_states[host.name] = 'BUSY'
                    logging.debug("%20s -> %s" % (task.name, host.name))


class Random(StaticScheduler):
    def prepare(self, simulation):
        for task in self.tasks:
            host = random.choice(self.hosts)
            self.host_tasks[host.name].append(task)
            logging.debug("%s -> %s" % (task.name, host.name))


class RoundRobin(StaticScheduler):
    def prepare(self, simulation):
        num_hosts = len(self.hosts)
        for (task_idx, task) in enumerate(self.tasks):
            host_idx = task_idx % num_hosts
            host = self.hosts[host_idx]
            self.host_tasks[host.name].append(task)
            logging.debug("%s -> %s" % (task.name, host.name))


class ListHeuristic(StaticScheduler):
    MIN_FIRST = 0
    MAX_FIRST = 1
    SUFFERAGE = 2

    def __init__(self, simulation, strategy):
        super(ListHeuristic, self).__init__(simulation)
        self.strategy = strategy

    def prepare(self, simulation):
        num_tasks = len(self.tasks)

        # build ECT matrix
        ECT = np.zeros((num_tasks, len(self.hosts)))
        for t, task in enumerate(self.tasks):
            stage_in = task.parents[0]
            for h, host in enumerate(self.hosts):
                ect = stage_in.get_ecomt(self.master, host) + task.get_eet(host)
                ECT[t][h] = ect
        # print(ECT)

        # build schedule
        task_idx = np.arange(num_tasks)
        for _ in range(0, len(self.tasks)):
            min_hosts = np.argmin(ECT, axis=1)
            min_times = ECT[np.arange(ECT.shape[0]), min_hosts]

            if self.strategy == ListHeuristic.MIN_FIRST:
                t = np.argmin(min_times)
            elif self.strategy == ListHeuristic.MAX_FIRST:
                t = np.argmax(min_times)
            elif self.strategy == ListHeuristic.SUFFERAGE:
                if ECT.shape[1] > 1:
                    min2_times = np.partition(ECT, 1)[:,1]
                    sufferages = min2_times - min_times
                    t = np.argmax(sufferages)
                else:
                    t = np.argmin(min_times)

            task = self.tasks[int(task_idx[t])]
            h = int(min_hosts[t])
            host = self.hosts[h]
            ect = min_times[t]

            self.host_tasks[host.name].append(task)
            logging.debug("%s -> %s" % (task.name, host.name))

            task_idx = np.delete(task_idx, t)
            ECT = np.delete(ECT, t, 0)
            task_ect = stage_in.get_ecomt(self.master, host) + task.get_eet(host)
            ECT[:,h] += task_ect
            # print(ECT)


class MinMin(ListHeuristic):
    def __init__(self, simulation):
        super(MinMin, self).__init__(simulation, ListHeuristic.MIN_FIRST)


class MaxMin(ListHeuristic):
    def __init__(self, simulation):
        super(MaxMin, self).__init__(simulation, ListHeuristic.MAX_FIRST)


class Sufferage(ListHeuristic):
    def __init__(self, simulation):
        super(Sufferage, self).__init__(simulation, ListHeuristic.SUFFERAGE)


# DYNAMIC SCHEDULERS ---------------------------------------------------------------------------------------------------

class OLB(simdag.DynamicScheduler):
    def prepare(self, simulation):
        self.tasks = simulation.tasks[simdag.TaskState.TASK_STATE_NOT_SCHEDULED]
        self.task_count = len(self.tasks)
        self.scheduled_count = 0
        self.hosts = simulation.hosts.by_prop('name', 'master', negate=True)
        self.host_states = {}
        for host in self.hosts:
            self.host_states[host.name] = 'FREE'

    def schedule(self, simulation, changed):
        logging.debug("%.2f -----------------------------------------------------------------------" % simulation.clock)
        # mark freed hosts
        for task in changed:
            if task.kind == simdag.TaskKind.TASK_KIND_COMP_SEQ and task.state == simdag.TaskState.TASK_STATE_DONE:
                host = task.hosts[0]
                if host.name != 'master':
                    self.host_states[host.name] = 'FREE'
                logging.debug("%20s: %s (%s)" % (task.name, str(task.state), host.name))
            else:
                logging.debug("%20s: %s" % (task.name, str(task.state)))
        # schedule tasks on free hosts
        for host in self.hosts:
            if self.host_states[host.name] == 'FREE':
                if self.scheduled_count < self.task_count:
                    task = self.tasks[self.scheduled_count]
                    task.schedule(host)
                    self.scheduled_count += 1
                    self.host_states[host.name] = 'BUSY'
                    logging.debug("%20s -> %s" % (task.name, host.name))
                else:
                    break


# SIMULATION -----------------------------------------------------------------------------------------------------------

def run_simulation(system, workload, scheduler):
    with simdag.Simulation(system, workload) as simulation:
        logging.info("Scheduler: %s" % (scheduler))

        # schedule root and end tasks on master host
        master_host = simulation.hosts.by_prop('name', 'master')[0]
        root_task = simulation.tasks.by_prop('name', 'root')[0]
        logging.debug("%s -> %s" % (root_task.name, master_host.name))
        root_task.schedule(master_host)
        end_task = simulation.tasks.by_prop('name', 'end')[0]
        logging.debug("%s -> %s" % (end_task.name, master_host.name))
        end_task.schedule(master_host)

        sched_class = globals()[scheduler]
        sched_class(simulation).run()

        return simulation.clock


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run experiments for bag-of-tasks applications")
    parser.add_argument("system_path", type=str, help="file or directory with systems")
    parser.add_argument("workload_path", type=str, help="file or directory with workloads")
    parser.add_argument("output_file", type=str, help="output file")
    parser.add_argument("--debug", dest="debug", action="store_true", default=False, help="print debug output")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    else:
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    systems = []
    workloads = []
    schedulers = ['Random', 'RoundRobin', 'MinMin', 'MaxMin', 'Sufferage', 'OLB']

    if os.path.isfile(args.system_path):
        systems.append(args.system_path)
    else:
        for root, directories, filenames in os.walk(args.system_path):
            for filename in filenames:
                if filename.endswith('.xml'):
                    systems.append(os.path.join(root, filename))

    if os.path.isfile(args.workload_path):
        workloads.append(args.workload_path)
    else:
        for root, directories, filenames in os.walk(args.workload_path):
            for filename in filenames:
                if filename.endswith('.dot'):
                    workloads.append(os.path.join(root, filename))

    scheduler_runs = []
    for scheduler in schedulers:
        if scheduler != 'Random':
            scheduler_runs.append(scheduler)
        else:
            # run Random 10 times and average results
            scheduler_runs.extend([scheduler] * 10)

    with open(args.output_file, 'w') as output:
        for system in systems:
            for workload in workloads:
                results = {scheduler: [] for scheduler in schedulers}

                # run schedulers in parallel
                runs = [(system, workload, scheduler) for scheduler in scheduler_runs]
                with multiprocessing.Pool(processes=len(scheduler_runs), maxtasksperchild=1) as pool:
                    run_results = pool.starmap(run_simulation, runs)
                    for (scheduler, makespan) in zip(scheduler_runs, run_results):
                        results[scheduler].append(makespan)

                # write results to the output file
                output.write("%s\n" % system)
                output.write("%s\n" % workload)
                for scheduler in schedulers:
                    result_str = '%20s\t%.2f\n' % (scheduler, np.mean(results[scheduler]))
                    output.write(result_str)
                output.write("\n")