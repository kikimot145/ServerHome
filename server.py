import argparse
import socket
import uuid
import json
import datetime
import os
import sys
import pathlib

class TaskQueueServer:
    FILENAME = "json_file.json"

    def __init__(self, ip, port, path, timeout):
        self.ip = ip
        self.port = port
        self.path = path
        self.timeout = timeout
        self.map_task_queu = dict()
        self.load()

    def load(self):
        try:
            with open(os.path.join(self.path, self.FILENAME), 'r') as f:
                for one_line in f:
                    queue = json.loads(one_line)
                    self.map_task_queu[queue['name']] = QueueTask(queue['name'], self.timeout)

                    for one_task in queue['tasks_list']:
                        self.map_task_queu[queue['name']].push(Task.task_load(one_task))
        except OSError:
            return 'Error'

    def add_task(self, queue, length, data):
        if not self.map_task_queu.get(queue):
            self.map_task_queu[queue] = QueueTask(queue, self.timeout)

        new_task = Task(length, data, queue)
        self.map_task_queu[queue].push(new_task)

        return new_task.id

    def get_task(self, queue):
        try:
            return self.map_task_queu[queue].pop()
        except KeyError:
            return None

    def ack_task(self, queue, id_task):
        if self.map_task_queu[queue].delete(id_task):
            return 'YES'
        else:
            return 'NO'

    def check_task(self, queue, id_task):
        if self.map_task_queu[queue].check(id_task):
            return 'YES'
        else:
            return 'NO'

    def save(self):
        with open(os.path.join(self.path, self.FILENAME), 'w') as f:
            for key, task_queue in self.map_task_queu.items():
                f.writelines(json.dumps(task_queue.get_info_taskqueue()) + '\n')

    def run(self):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.bind((self.ip, self.port))
        print(self.ip, self.port)
        connection.listen(4)
        try:
            while True:
                conn, addr = connection.accept()
                while True:
                    data = conn.recv(10000)
                    req = data.decode().split()

                    try:
                        if req[0] == 'ADD':
                            return self.add_task(req[1], req[2], req[3])
                        if req[0] == 'GET':
                            return self.get_task(req[1])
                        if req[0] == 'ACK':
                            return self.ack_task(req[1], req[2])
                        if req[0] == 'IN':
                            return self.check_task(req[1], req[2])
                        if req[0] == 'SAVE':
                            return self.save()
                    except KeyError:
                        return 'Error'

                    break
        except KeyboardInterrupt:
            print('stop server')
            if conn:
                conn.close()
            sys.exit(0)

class QueueTask(object):
    def __init__(self, name, timeout):
        self.name = name
        self.timeout = timeout
        self.tasks_list = []

    def push(self, new_task):
        self.tasks_list.append(new_task)

    def pop(self):
        for one_task in self.tasks_list:
            if one_task.status == Task.STATUS_WAIT:
                if (datetime.datetime.now - one_task.time_start).seconds < self.timeout:
                    continue
                else:
                    return one_task.get_data()
            elif one_task.status == Task.STATUS_TO_DO:
                return one_task.get_data()
            else:
                raise NotImplementedError()
        return None

    def delete(self, id_task):
        for idx, one_task in enumerate(self.tasks_list):
            if one_task.id == id_task:
                self.tasks_list.pop(idx)
                return True
        return False

    def check(self, id_task):
        for one_task in self.tasks_list:
            if one_task.id == id_task:
                return True
        return False

    def get_info_taskqueue(self):
        return{
            "name": self.name,
            "tasks_list": [task.get_info_task() for task in self.tasks_list],
            "size_list": len(self.tasks_list)
        }

class Task(object):
    STATUS_TO_DO = 'TO_DO'
    STATUS_WAIT = 'WAIT'

    def __init__(self, length, data, queue):
        self.id = uuid.uuid4().hex
        self.length = length
        self.data = data
        self.time_start = None
        self.queue = queue
        self.status = "TO_DO"

    @classmethod
    def task_load(cls, data):
        print(data, type(data))
        task = cls(data['length'], data['data'], data['queue'])
        task.id = data['id']
        task.status = data['status']
        if data['timeout']:
            task.time_start = data['timeout'].strptime('%Y-%m-%d %H:%M:%S')
        else:
            task.time_start = None
        return task


    def get_task(self):
        self._status = self.STATUS_WAIT
        self.time_start = datetime.datetime.now()
        return "{}{}{}".format(self.id, self.length, self.data)

    def get_info_task(self):
        if self.time_start:
            time = self.time_start.strftime("%Y-%m-%d %H:%M:%S")
        else:
            time = None
        return{
            "id": self.id,
            "length": self.length,
            "data": self.data,
            "timeout": time,
            "queue": self.queue,
            "status": self.status
        }


def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='0.0.0.0',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='./',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=300,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    server = TaskQueueServer(**args.__dict__)
    server.run()