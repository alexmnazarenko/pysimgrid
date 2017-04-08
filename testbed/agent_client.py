import logging
import traceback

from threading import Thread

from tornado import escape
from tornado import gen
from tornado import httputil
from tornado import ioloop
from tornado import httpclient
from tornado.websocket import websocket_connect

DEFAULT_CONNECT_TIMEOUT = 60
DEFAULT_REQUEST_TIMEOUT = 60
MAX_BODY_SIZE = 200000000
FILE_CHUNK_SIZE = 1024 * 1024


class AgentClient(Thread):

    def __init__(self, agent_address, token, manager,
                 connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                 request_timeout=DEFAULT_REQUEST_TIMEOUT):
        Thread.__init__(self)

        logging.getLogger('tornado.access').setLevel(logging.DEBUG)
        logging.getLogger('tornado.application').setLevel(logging.DEBUG)
        logging.getLogger('tornado.general').setLevel(logging.DEBUG)

        self.agent_address = agent_address
        self.token = token
        self.manager = manager
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.http_client = httpclient.AsyncHTTPClient(max_buffer_size=MAX_BODY_SIZE)
        self.ioloop = ioloop.IOLoop.instance()
        self._ws_connection = None

    def run(self):
        try:
            self.connect()
            self.ioloop.handle_callback_exception = self.handle_exception
            self.ioloop.start()
        except Exception as e:
            print(e)
            self.close()

    def connect(self):
        connect_uri = 'ws://%s/agent/ws' % self.agent_address
        headers = httputil.HTTPHeaders({
            'Authorization': 'EverestAgentClientToken %s' % self.token,
            'Sec-WebSocket-Protocol': 'v1.agent.everest'
        })
        request = httpclient.HTTPRequest(url=connect_uri,
                                         connect_timeout=self.connect_timeout,
                                         request_timeout=self.request_timeout,
                                         headers=headers)
        ws_conn = websocket_connect(request)
        ws_conn.add_done_callback(self._connect_callback)

    def send(self, data):
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is closed.')
        self._ws_connection.write_message(escape.json_encode(data))
        print(data)

    def send_message(self, msgType, *data):
        self.send([msgType] + list(data))

    def close(self):
        if self._ws_connection is not None:
            self._ws_connection.close()
        self.ioloop.add_callback(self.ioloop.stop)

    def _connect_callback(self, future):
        if future.exception() is None:
            self._ws_connection = future.result()
            self.on_connection_success()
            self._read_messages()
        else:
            self.ioloop.stop()
            self.on_connection_error(future.exception())

    @gen.coroutine
    def _read_messages(self):
        while True:
            msg = yield self._ws_connection.read_message()
            if msg is None:
                self.on_connection_close()
                break

            msg = escape.json_decode(msg)
            self.on_message(msg)

    def on_message(self, msg):
        print(msg)
        msgType = msg[0]
        if msgType == 'TASK_STATE':
            task_id = msg[1]
            task_state = msg[2]
            task_info = msg[3]
            self.manager.on_task_state_change(task_id, task_state, task_info)

    def on_connection_success(self):
        print('Connected!')

    def on_connection_close(self):
        print('Connection closed!')

    def on_connection_error(self, exception):
        print('Connection error: %s', exception)

    def handle_exception(self, callback):
        traceback.print_exc()
        self.close()
        self.ioloop.stop()

    def submit_task(self, task_id, task_spec, task_context = {}):
        self.send_message('TASK_SUBMIT', task_id, task_spec, task_context)

    def download_file(self, file_uri, file):
        headers = {
            'Authorization': 'EverestAgentClientToken %s' % self.token
        }
        return self.http_client.fetch(file_uri, streaming_callback=file.write, headers=headers)
