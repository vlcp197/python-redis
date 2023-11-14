from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error


# Using Exceptions to notify the connection-handling loop of problems
class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


Error = namedtuple('Error', ('message',))


class ProtocolHandler:

    def handle_request(self, socket_file):
        """
            Parse a request from the client
            into it's component parts.
        """
        pass

    def write_response(self, socket_file, data):
        """
            Serialize the response data
            and send it to the client
        """
        pass


class Server:
    def __init__(self, host='127.0.0.1', port=6379, max_clients=64):
        """
            Using port 6379, since it's the
            convention for the Redis port."
        """
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handler(self, conn, address):
        """
            Convert "conn", a socket object,
            into a file-like objects
        """

        socket_file = conn.makefile('rwb')
        while True:
            # Process client requests until client disconnects
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as e:
                resp = Error(e.args[0])

        self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        """
            Unpack the data sent by the client,
            execute the command specified,
            and pass back the return value.
        """

        pass

    def run(self):
        self._server.serve_forever()
