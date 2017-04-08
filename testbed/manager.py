import datetime
import os
import time

from agent_client import AgentClient


class Manager:

    def __init__(self, hosts):
        self.hosts = hosts
        self.hosts_idx = {}
        for idx, host in enumerate(hosts):
            self.hosts_idx[host['id']] = idx
        self.tasks = None
        self.tasks_idx = None
        self.completed_tasks = 0
        self.start_ts = None
        self.finish_ts = None
        self.run_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        for host in self.hosts:
            token = '%s:%s' % (self.run_id, host['token'])
            host['token'] = token
            client = AgentClient(host['endpoint'], token, self)
            client.start()
            time.sleep(0.1)
            host['client'] = client
            host['state'] = 'IDLE'

    def run(self, tasks):
        self.tasks = tasks
        self.tasks_idx = {}
        for idx, task in enumerate(tasks):
            task['spec']['name'] = task['id']
            task['state'] = 'READY'
            task['deps'] = []
            for input in task['spec']['inputData']:
                if input['uri'].startswith('$'):
                    parent_id = input['uri'][1:].split('.')[0]
                    task['deps'].append(parent_id)
            self.tasks_idx[task['id']] = idx
        self.completed_tasks = 0
        self.start_ts = time.time()
        self.finish_ts = None

        self.schedule()

        while self.completed_tasks < len(self.tasks):
            time.sleep(1)

        if self.finish_ts is not None:
            run_time = self.finish_ts - self.start_ts
            print("Run time: %f" % run_time)

    def schedule(self):
        # print('Schedule...')

        # find idle hosts
        idle_hosts = []
        for host in self.hosts:
            if host['state'] == 'IDLE':
                idle_hosts.append(host['id'])

        # schedule ready tasks to idle hosts
        for task in self.tasks:
            if len(idle_hosts) == 0:
                break
            if task['state'] == 'READY':
                # print('Found ready task: ' + task['id'])
                parent_states = set(map(lambda dep: self.get_task(dep)['state'], task['deps']))
                # print('Parent states: ' + str(parent_states))
                if len(parent_states) == 0 or parent_states == {'DONE'}:
                    # set inputs
                    for input in task['spec']['inputData']:
                        if input['uri'].startswith('$'):
                            parts = input['uri'][1:].split('.')
                            source_id = parts[0]
                            output = parts[1]
                            source_task = self.get_task(source_id)
                            source_host = self.get_host(source_task['host'])
                            output_uri = source_task['outputs'][output]
                            input['uri'] = 'http://%s%s' % (source_host['endpoint'], output_uri)
                            input['auth'] = 'EverestAgentClientToken %s' % source_host['token']
                    task['state'] = 'SCHEDULED'
                    host_id = idle_hosts.pop(0)
                    host = self.get_host(host_id)
                    task['host'] = host_id
                    host['state'] = 'ALLOCATED'

                    # synthetic tasks
                    if task['spec']['command'].startswith('synthetic_task.py'):
                        task['spec']['environment'] = {
                            'PATH': '$PATH@PATH_SEP%s' % os.getcwd(),
                            'HOST_SPEED_GFLOPS': str(host['speed'])
                        }

                    client = self.get_host(host_id)['client']
                    client.submit_task(self.run_id + "-" + task['id'], task['spec'])
                    print('Scheduled task %s on %s' % (task['id'], host_id))
                elif 'FAILED' in parent_states:
                    task['state'] = 'FAILED'
                    self.completed_tasks += 1
                elif 'CANCELED' in parent_states:
                    task['state'] = 'CANCELED'
                    self.completed_tasks += 1

    def on_task_state_change(self, task_id, task_state, task_info):
        local_task_id = task_id.replace(self.run_id + '-', '')
        task = self.get_task(local_task_id)
        task['state'] = task_state
        if task_state == 'STAGED_OUT':
            outputs = {}
            for output in task_info['outputData']:
                outputs[output['path']] = output['uri']
            task['outputs'] = outputs
        if task_state in ['DONE', 'FAILED', 'CANCELED']:
            # start timer after the root task is completed for synthetic DAG
            if task['id'] == 'root':
                self.start_ts = time.time()

            host = self.get_host(task['host'])
            host['state'] = 'IDLE'

            self.completed_tasks += 1
            if self.completed_tasks < len(self.tasks):
                self.schedule()
            else:
                self.finish_ts = time.time()

    def get_host(self, host_id):
        return self.hosts[self.hosts_idx[host_id]]

    def get_task(self, task_id):
        return self.tasks[self.tasks_idx[task_id]]

    def shutdown(self):
        for host in self.hosts:
            host['client'].close()
