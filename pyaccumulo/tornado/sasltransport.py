import socket
from cStringIO import StringIO
from struct import pack, unpack

from thrift.transport.TTransport import TTransportBase, TTransportException
from tornado import ioloop, gen, iostream
from puresasl.client import SASLClient


class TornadoSaslClientTransport(TTransportBase):
    """
    Tornado SASL transport - if TTornadoStreamTransport and TSaslClientTransport had a baby...
    """

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, host, port, service, mechanism='GSSAPI', **sasl_kwargs):
        self.host = host
        self.port = port
        self.is_queuing_reads = False
        self.read_queue = []

        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = StringIO()
        self.__rbuf = StringIO()

        # Tornado specific
        self.io_loop = ioloop.IOLoop.current()
        self.stream = None

    @gen.engine
    def open(self, callback):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.stream = iostream.IOStream(sock)

        def on_close_in_connect(*_):
            message = 'could not connect to {}:{}'.format(self.host, self.port)
            raise TTransportException(
                type=TTransportException.NOT_OPEN,
                message=message)
        self.stream.set_close_callback(on_close_in_connect)

        self.is_queuing_reads = True
        yield gen.Task(self.stream.connect, (self.host, self.port))
        self._set_close_callback()

        yield gen.Task(self.send_sasl_msg, self.START, self.sasl.mechanism)
        yield gen.Task(self.send_sasl_msg, self.OK, self.sasl.process())

        while True:
            status, challenge = yield gen.Task(self.recv_sasl_msg)
            if status == self.OK:
                yield gen.Task(self.send_sasl_msg, self.OK, self.sasl.process(challenge))
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    self.is_queuing_reads = False
                    raise TTransportException("The server erroneously indicated "
                                              "that SASL negotiation was complete")
                else:
                    break
            else:
                self.is_queuing_reads = False
                raise TTransportException("Bad SASL negotiation status: %d (%s)"
                                          % (status, challenge))

        self.is_queuing_reads = False
        callback(self)

    def _set_close_callback(self):
        self.stream.set_close_callback(self.close)

    def close(self):
        self.sasl.dispose()
        # don't raise if we intend to close
        self.stream.set_close_callback(None)
        self.stream.close()

    def read(self, _):
        # The generated code for Tornado shouldn't do individual reads -- only
        # frames at a time
        assert False, "you're doing it wrong"

    @gen.engine
    def send_sasl_msg(self, status, body, callback):
        header = pack(">BI", status, len(body))
        yield gen.Task(self.stream.write, header + body)
        callback()

    @gen.engine
    def recv_sasl_msg(self, callback):
        header = yield gen.Task(self.stream.read_bytes, 5)
        status, length = unpack(">BI", header)
        if length > 0:
            payload = yield gen.Task(self.stream.read_bytes, length)
        else:
            payload = ""
        callback((status, payload))

    @gen.engine
    def readFrame(self, callback):
        self.read_queue.append(callback)
        if self.is_queuing_reads:
            # If a read is already in flight, then the while loop below should
            # pull it from self.read_queue
            return

        self.is_queuing_reads = True
        while self.read_queue:
            next_callback = self.read_queue.pop()
            result = yield gen.Task(self._readFrameFromStream)
            next_callback(result)
        self.is_queuing_reads = False

    @gen.engine
    def _readFrameFromStream(self, callback):
        frame_header = yield gen.Task(self.stream.read_bytes, 4)
        if len(frame_header) == 0:
            raise iostream.StreamClosedError('Read zero bytes from stream')
        frame_length, = unpack('!i', frame_header)
        encoded = yield gen.Task(self.stream.read_bytes, frame_length)
        callback(self.sasl.unwrap(encoded))

    def write(self, buf):
        self.__wbuf.write(buf)

    def flush(self, callback=None):
        frame = self.sasl.wrap(self.__wbuf.getvalue())
        # reset wbuf before write/flush to preserve state on underlying failure
        frame_length = pack('!i', len(frame))
        self.__wbuf = StringIO()
        self.stream.write(frame_length + frame, callback)
