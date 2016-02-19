from thrift import TTornado
from thrift.protocol import TCompactProtocol
from tornado import gen
from pyaccumulo import BaseIterator, Cell, _get_scan_columns
from pyaccumulo.tornado.proxy import AccumuloProxy
from pyaccumulo.tornado.proxy.ttypes import TimeType, WriterOptions, IteratorSetting, ScanOptions, BatchScanOptions, \
    UnknownWriter
from pyaccumulo.tornado.sasltransport import TornadoSaslClientTransport

BW_DEFAULTS = dict(
    max_memory=10*1024,
    latency_ms=30*1000,
    timeout_ms=5*1000,
    threads=10,
)

# The number of entries returned with a single scan.next()
SCAN_BATCH_SIZE = 10


def _check_and_raise_exc(res):
    """
    The thrift library returns exceptions instead of raising them - so we have to manually check the result and re-raise
    if we want to catch them...
    """
    if isinstance(res, Exception):
        raise res


class BatchWriter(object):
    def __init__(self, conn):
        super(BatchWriter, self).__init__()
        self.client = conn.client
        self.login = conn.login
        self._writer = None

    @staticmethod
    @gen.engine
    def create(conn, table, max_memory, latency_ms, timeout_ms, threads, callback):
        bw = BatchWriter(conn)
        bw_options = WriterOptions(maxMemory=max_memory, latencyMs=latency_ms, timeoutMs=timeout_ms, threads=threads)
        res = yield gen.Task(bw.client.createWriter, bw.login, table, bw_options)
        _check_and_raise_exc(res)
        bw._writer = res
        bw._is_closed = False
        callback(bw)

    @gen.engine
    def add_mutations(self, muts, callback):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        if self._writer is None:
            raise UnknownWriter("Cannot write to a closed writer")
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        # NOTE: this is fire and forget - we don't wait for a callback
        self.client.update(self._writer, cells)
        callback()

    @gen.engine
    def add_mutation(self, mut, callback):
        if self._writer is None:
            raise UnknownWriter("Cannot write to a closed writer")
        # NOTE: this is fire and forget - we don't wait for a callback
        self.client.update(self._writer, {mut.row: mut.updates})
        callback()

    @gen.engine
    def flush(self, callback):
        if self._writer is None:
            raise UnknownWriter("Cannot flush a closed writer")
        res = yield gen.Task(self.client.flush, self._writer)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def close(self, callback):
        if self._writer is not None:
            res = yield gen.Task(self.client.closeWriter, self._writer)
            _check_and_raise_exc(res)
            self._writer = None
        callback()


class Scanner(object):
    def __init__(self, conn):
        super(Scanner, self).__init__()
        self.client = conn.client
        self.login = conn.login
        self._scanner = None
        self.batch = None

    @staticmethod
    def _get_range(scanrange):
        if scanrange:
            return scanrange.to_range()
        else:
            return None

    @staticmethod
    def _get_ranges(scanranges):
        if scanranges:
            return [scanrange.to_range() for scanrange in scanranges]
        else:
            return None

    def _get_iterator_settings(self, iterators):
        if not iterators:
            return None
        return [self._process_iterator(i) for i in iterators]

    @staticmethod
    def _process_iterator(iterator):
        if isinstance(iterator, IteratorSetting):
            return iterator
        elif isinstance(iterator, BaseIterator):
            return iterator.get_iterator_setting()
        else:
            raise Exception("Cannot process iterator: %s" % iterator)

    @staticmethod
    @gen.engine
    def create(conn, table, scanrange, cols, auths, iterators, callback):
        scanner = Scanner(conn)
        options = ScanOptions(auths, scanner._get_range(scanrange), _get_scan_columns(cols),
                              scanner._get_iterator_settings(iterators), bufferSize=None)
        res = yield gen.Task(scanner.client.createScanner, scanner.login, table, options)
        _check_and_raise_exc(res)
        scanner._scanner = res
        callback(scanner)

    @staticmethod
    @gen.engine
    def create_batch(conn, table, scanranges, cols, auths, iterators, callback):
        scanner = Scanner(conn)
        options = BatchScanOptions(auths, scanner._get_ranges(scanranges), _get_scan_columns(cols),
                                   scanner._get_iterator_settings(iterators), threads=None)
        res = yield gen.Task(scanner.client.createBatchScanner, scanner.login, table, options)
        _check_and_raise_exc(res)
        scanner._scanner = res
        callback(scanner)

    @gen.engine
    def next(self, callback, batchsize=SCAN_BATCH_SIZE):
        res = yield gen.Task(self.client.nextK, self._scanner, batchsize)
        _check_and_raise_exc(res)
        self.batch = res
        entries = []
        if self.batch.results:
            entries = [Cell(e.key.row, e.key.colFamily, e.key.colQualifier, e.key.colVisibility, e.key.timestamp,
                            e.value) for e in self.batch.results]
        callback(entries)

    def has_next(self):
        if self.batch is None:
            return True
        return self.batch.more

    @gen.engine
    def close(self, callback):
        res = yield gen.Task(self.client.closeScanner, self._scanner)
        _check_and_raise_exc(res)
        callback()


class Accumulo(object):
    def __init__(self, host="localhost", port=50096, **kwargs):
        super(Accumulo, self).__init__()

        # depending on extra args, we may want an SASL transport
        if 'mechanism' in kwargs:
            mechanism = kwargs.pop('mechanism')
            if mechanism != 'GSSAPI':
                raise ValueError('Only supported mechanism is "GSSAPI", but "%s" was passed in.' % kwargs['mechanism'])
            service = 'accumulo'
            if 'service' in kwargs:
                service = kwargs.pop('service')
            self.transport = TornadoSaslClientTransport(host, port, service, mechanism, **kwargs)
        else:
            self.transport = TTornado.TTornadoStreamTransport(host, port)

        self.pfactory = TCompactProtocol.TCompactProtocolFactory()
        self.client = AccumuloProxy.Client(self.transport, self.pfactory)
        self.login = None

    @staticmethod
    @gen.engine
    def create_and_connect(host, port, user, password, **kwargs):
        callback = kwargs.pop('callback')
        acc = Accumulo(host, port, **kwargs)
        yield gen.Task(acc.connect, user, password)
        callback(acc)

    @gen.engine
    def connect(self, user, password, callback):
        yield gen.Task(self.transport.open)
        res = yield gen.Task(self.client.login, user, {'password': password})
        _check_and_raise_exc(res)
        self.login = res
        callback()

    def close(self):
        self.transport.close()

    @gen.engine
    def list_tables(self, callback):
        res = yield gen.Task(self.client.listTables, self.login)
        _check_and_raise_exc(res)
        tables = [t for t in res]
        callback(tables)

    @gen.engine
    def table_exists(self, table, callback):
        res = yield gen.Task(self.client.tableExists, self.login, table)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def create_table(self, table, callback):
        res = yield gen.Task(self.client.createTable, self.login, table, True, TimeType.MILLIS)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def delete_table(self, table, callback):
        res = yield gen.Task(self.client.deleteTable, self.login, table)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def clone_table(self, table, new_table, flush, props_to_set, props_to_exclude, callback):
        res = yield gen.Task(self.client.cloneTable, self.login, table, new_table, flush,
                             props_to_set, props_to_exclude)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def export_table(self, table, export_dir, callback):
        res = yield gen.Task(self.client.exportTable, self.login, table, export_dir)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def import_table(self, table, import_dir, callback):
        res = yield gen.Task(self.client.importTable, self.login, table, import_dir)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def offline_table(self, table, wait, callback):
        res = yield gen.Task(self.client.offlineTable, self.login, table, wait)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def online_table(self, table, wait, callback):
        res = yield gen.Task(self.client.onlineTable, self.login, table, wait)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def delete_rows(self,  table, start_row, end_row, callback):
        res = yield gen.Task(self.client.deleteRows, self.login, table, start_row, end_row)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def rename_table(self, oldtable, newtable, callback):
        res = yield gen.Task(self.client.renameTable, self.login, oldtable, newtable)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def write(self, table, muts, callback):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        writer = yield gen.Task(self.create_batch_writer, table)
        writer.add_mutations(muts)
        yield gen.Task(writer.close)
        callback()

    @gen.engine
    def create_scanner(self, table, callback, scanrange=None, cols=None, auths=None, iterators=None):
        scanner = yield gen.Task(Scanner.create, self, table, scanrange, cols, auths, iterators)
        callback(scanner)

    @gen.engine
    def create_batch_scanner(self, table, callback, scanranges=None, cols=None, auths=None, iterators=None):
        scanner = yield gen.Task(Scanner.create_batch, self, table, scanranges, cols, auths, iterators)
        callback(scanner)

    @gen.engine
    def create_batch_writer(self, table, callback):
        bw = yield gen.Task(BatchWriter.create, self, table, **BW_DEFAULTS)
        callback(bw)

    @gen.engine
    def attach_iterator(self, table, setting, scopes, callback):
        res = yield gen.Task(self.client.attachIterator, self.login, table, setting, scopes)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def remove_iterator(self, table, iterator, scopes, callback):
        res = yield gen.Task(self.client.removeIterator, self.login, table, iterator, scopes)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def following_key(self, key, part, callback):
        res = yield gen.Task(self.client.getFollowing, key, part)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def get_max_row(self, table, callback, auths=None, srow=None, sinclude=None, erow=None, einclude=None):
        res = yield gen.Task(self.client.getMaxRow, self.login, table, auths, srow, sinclude, erow, einclude)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def add_mutations(self, table, muts, callback):
        """
        Add mutations to a table without the need to create and manage a batch writer.
        """
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        res = yield gen.Task(self.client.updateAndFlush, self.login, table, cells)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def create_user(self, user, password, callback):
        res = yield gen.Task(self.client.createLocalUser, self.login, user, password)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def drop_user(self, user, callback):
        res = yield gen.Task(self.client.dropLocalUser, self.login, user)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def list_users(self, callback):
        res = yield gen.Task(self.client.listLocalUsers, self.login)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def set_user_authorizations(self, user, auths, callback):
        res = yield gen.Task(self.client.changeUserAuthorizations, self.login, user, auths)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def get_user_authorizations(self, user, callback):
        res = yield gen.Task(self.client.getUserAuthorizations, self.login, user)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def grant_system_permission(self, user, perm, callback):
        res = yield gen.Task(self.client.grantSystemPermission, self.login, user, perm)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def revoke_system_permission(self, user, perm, callback):
        res = yield gen.Task(self.client.revokeSystemPermission, self.login, user, perm)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def has_system_permission(self, user, perm, callback):
        res = yield gen.Task(self.client.hasSystemPermission, self.login, user, perm)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def grant_table_permission(self, user, table, perm, callback):
        res = yield gen.Task(self.client.grantTablePermission, self.login, user, table, perm)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def revoke_table_permission(self, user, table, perm, callback):
        res = yield gen.Task(self.client.revokeTablePermission, self.login, user, table, perm)
        _check_and_raise_exc(res)
        callback()

    @gen.engine
    def has_table_permission(self, user, table, perm, callback):
        res = yield gen.Task(self.client.hasTablePermission, self.login, user, table, perm)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def add_splits(self, table, splits, callback):
        res = yield gen.Task(self.client.addSplits, self.login, table, splits)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def add_constraint(self, table, class_name, callback):
        res = yield gen.Task(self.client.addConstraint, self.login, table, class_name)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def list_constraints(self, table, callback):
        res = yield gen.Task(self.client.listConstraints, self.login, table)
        _check_and_raise_exc(res)
        callback(res)

    @gen.engine
    def remove_constraint(self, table, constraint, callback):
        """
        :param table: table name
        :param constraint: the constraint number as returned by list constraints
        """
        res = yield gen.Task(self.client.removeConstraint, self.login, table, constraint)
        _check_and_raise_exc(res)
        callback()