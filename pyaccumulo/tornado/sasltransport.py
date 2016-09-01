import socket
from cStringIO import StringIO
from struct import pack, unpack

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport.TTransport import TTransportBase, TTransportException
from thrift.protocol import TCompactProtocol

from puresasl.client import SASLClient

from pyaccumulo.proxy import AccumuloProxy
from pyaccumulo.sasltransport import SaslClientTransport

from tornado import ioloop, gen, iostream


class TornadoSaslClientTransport(TTransportBase):
    """
    Tornado SASL transport - if TTornadoStreamTransport and TSaslClientTransport had a baby...
    """

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, host, port, primary, mechanism='GSSAPI', **sasl_kwargs):
        self.host = host
        self.port = port
        self.is_queuing_reads = False
        self.read_queue = []

        candidates = [] # we must search through instances until we connect, this is where we store them
        if 'instance' in sasl_kwargs:
            candidates.extend(sasl_kwargs.pop('instance').split(','))
        else:
            candidates.append(host)

        # unfortunately we need to do the same synchronous hostname ping in the tornado version of pyaccumulo
        # in order to get the current accumulo master
        # this adds overhead when initially establishing a connection, but since we can't know if and when an
        # instance will go down, we're left with no other choice but to offload some of this pain on the
        # connection pool
        instance = self._get_available_instance(host, port, primary, mechanism, candidates, **sasl_kwargs)
        self.sasl = SASLClient(instance, primary, mechanism, **sasl_kwargs)

        self.__wbuf = StringIO()
        self.__rbuf = StringIO()

        # Tornado specific
        self.io_loop = ioloop.IOLoop.current()
        self.stream = None

    def _get_available_instance(self, host, port, primary, mechanism, candidates, **sasl_kwargs):
        socket = TSocket.TSocket(host, port)
        if 'timeout' in sasl_kwargs:
            socket.setTimeout(sasl_kwargs.pop('timeout'))
        if not candidates:
            return None

        # simple round-robin attempt at establishing a connection when there are multiple instances defined but
        # we don't know which master is listening.
        while len(candidates) > 0:
            instance = candidates.pop()

            self.transport = SaslClientTransport(socket, instance, primary, mechanism, **sasl_kwargs)
            self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
            self.client = AccumuloProxy.Client(self.protocol)

            try:
                self.transport.open()
            except TTransportException:
                if len(candidates) <= 0:
                    instance = None
            finally:
                self.transport.close()

        return instance

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
                    raise TTransportException(message="The server erroneously indicated "
                                              "that SASL negotiation was complete")
                else:
                    break
            else:
                self.is_queuing_reads = False
                raise TTransportException(message="Bad SASL negotiation status: %d (%s)"
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
