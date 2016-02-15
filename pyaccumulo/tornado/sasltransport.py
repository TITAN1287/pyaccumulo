import socket
from cStringIO import StringIO
from contextlib import contextmanager
from struct import pack, unpack

from thrift.TTornado import _Lock
from thrift.transport.TTransport import TTransportBase, TTransportException
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

    def __init__(self, host, port, service, mechanism='GSSAPI', **sasl_kwargs):
        from puresasl.client import SASLClient

        self.host = host
        self.port = port
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = StringIO()
        self.__rbuf = StringIO()

        # Tornado specific
        self.io_loop = ioloop.IOLoop.current()
        self._read_lock = _Lock()
        self.stream = None

    def with_timeout(self, timeout, future):
        return gen.with_timeout(timeout, future, self.io_loop)

    @gen.coroutine
    def open(self, timeout=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.stream = iostream.IOStream(sock)

        with (yield self._read_lock.acquire()):
            with self.io_exception_context():
                try:
                    connect = self.stream.connect((self.host, self.port))
                    if timeout is not None:
                        yield self.with_timeout(timeout, connect)
                    else:
                        yield connect
                except (socket.error, IOError, ioloop.TimeoutError) as e:
                    message = 'could not connect to {}:{} ({})'.format(self.host, self.port, e)
                    raise TTransportException(
                        type=TTransportException.NOT_OPEN,
                        message=message)

                yield self.send_sasl_msg(self.START, self.sasl.mechanism)
                yield self.send_sasl_msg(self.OK, self.sasl.process())

                while True:
                    status, challenge = yield self.recv_sasl_msg()
                    if status == self.OK:
                        yield self.send_sasl_msg(self.OK, self.sasl.process(challenge))
                    elif status == self.COMPLETE:
                        if not self.sasl.complete:
                            raise TTransportException("The server erroneously indicated "
                                                      "that SASL negotiation was complete")
                        else:
                            break
                    else:
                        raise TTransportException("Bad SASL negotiation status: %d (%s)"
                                                  % (status, challenge))

        raise gen.Return(self)

    def set_close_callback(self, callback):
        """
        Should be called only after open() returns
        """
        self.stream.set_close_callback(callback)

    def close(self):
        self.sasl.dispose()
        # don't raise if we intend to close
        self.stream.set_close_callback(None)
        self.stream.close()

    def read(self, _):
        # The generated code for Tornado shouldn't do individual reads -- only
        # frames at a time
        assert False, "you're doing it wrong"

    @gen.coroutine
    def send_sasl_msg(self, status, body):
        header = pack(">BI", status, len(body))
        yield self.stream.write(header + body)

    @gen.coroutine
    def recv_sasl_msg(self):
        header = yield self.stream.read_bytes(5)
        status, length = unpack(">BI", header)
        if length > 0:
            payload = yield self.stream.read_bytes(length)
        else:
            payload = ""
        raise gen.Return((status, payload))

    @contextmanager
    def io_exception_context(self):
        try:
            yield
        except (socket.error, IOError) as e:
            raise TTransportException(
                type=TTransportException.END_OF_FILE,
                message=str(e))
        except iostream.StreamBufferFullError as e:
            raise TTransportException(
                type=TTransportException.UNKNOWN,
                message=str(e))

    @gen.coroutine
    def readFrame(self):
        # IOStream processes reads one at a time
        with (yield self._read_lock.acquire()):
            with self.io_exception_context():
                frame_header = yield self.stream.read_bytes(4)
                if len(frame_header) == 0:
                    raise iostream.StreamClosedError('Read zero bytes from stream')
                frame_length, = unpack('!i', frame_header)
                encoded = yield self.stream.read_bytes(frame_length)
                frame = StringIO(self.sasl.unwrap(encoded))
                raise gen.Return(frame.getvalue())

    def write(self, buf):
        self.__wbuf.write(buf)

    def flush(self):
        frame = self.sasl.wrap(self.__wbuf.getvalue())
        # reset wbuf before write/flush to preserve state on underlying failure
        frame_length = pack('!i', len(frame))
        self.__wbuf = StringIO()
        with self.io_exception_context():
            return self.stream.write(frame_length + frame)
