# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
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

import logging
import networkx

from .. import scheduler


class RoundRobinStatic(scheduler.StaticScheduler):
    """
    Round robin static scheduler.

    The important part there is static - all tasks are scheduled simultaneously.
    For more details please see RandomScheduler description.

    Anyway, not a sensible approach by any means.
    """

    def get_schedule(self, simulation):
        schedule = {host: [] for host in simulation.hosts}
        master_host = simulation.hosts.by_prop('name', scheduler.Scheduler.MASTER_HOST_NAME)[0]
        exec_hosts = simulation.hosts.by_prop('name', scheduler.Scheduler.MASTER_HOST_NAME, negate=True)
        hosts_count = len(exec_hosts)
        i = 0
        for task in networkx.topological_sort(simulation.get_task_graph()):
            if task.name in scheduler.Scheduler.BOUNDARY_TASKS:
                host = master_host
            else:
                host = exec_hosts[i % hosts_count]
                i += 1
            schedule[host].append(task)
            # logging.info("%s -> %s" % (task.name, host.name))
        return schedule
