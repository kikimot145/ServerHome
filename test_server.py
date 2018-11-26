import unittest
from unittest import TestCase,__unittest

import time
import socket

import subprocess

from server import TaskQueueServer


class ServerBaseTest(TestCase):
    def setUp(self):
        self.server = subprocess.Popen(['python', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def tearDown(self):
        self.server.terminate()
        self.server.wait()

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_base_scenario(self):
        task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))

        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 5 12345', self.send(b'GET 1'))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_four_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        three_task_id = self.send(b'ADD 1 3 123')
        four_task_id = self.send(b'ADD 1 2 3')

        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + three_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + four_task_id))

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id, + b' 5 12345', self.send(b'GET 1'))

        self.assertEqual(three_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + three_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + four_task_id))
        self.assertEqual(four_task_id, + b' 3 123', self.send(b'GET 1'))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + first_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + first_task_id))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + three_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + three_task_id))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + four_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + four_task_id))

    def test_timoeout(self):
        time.sleep(10)
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        three_task_id = self.send(b'ADD 1 3 123')

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(second_task_id, + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(three_task_id + b' 3 123', self.send(b'GET 1'))

        time.sleep(350)

        self.assertEqual(b'NO', self.send(b'ACK 1 ' + first_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + three_task_id))



if __name__ == '__main__':
    unittest.main()
