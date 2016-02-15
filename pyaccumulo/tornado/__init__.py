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


class BatchWriter(object):
    def __init__(self, conn):
        super(BatchWriter, self).__init__()
        self.client = conn.client
        self.login = conn.login
        self._writer = None

    @staticmethod
    @gen.coroutine
    def create(conn, table, max_memory, latency_ms, timeout_ms, threads):
        bw = BatchWriter(conn)
        bw_options = WriterOptions(maxMemory=max_memory, latencyMs=latency_ms, timeoutMs=timeout_ms, threads=threads)
        res = yield bw.client.createWriter(bw.login, table, bw_options)
        bw._writer = res
        bw._is_closed = False
        raise gen.Return(bw)

    @gen.coroutine
    def add_mutations(self, muts):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        if self._writer is None:
            raise UnknownWriter("Cannot write to a closed writer")
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        # NOTE: this is fire and forget - we don't wait for a callback
        self.client.update(self._writer, cells)

    @gen.coroutine
    def add_mutation(self, mut):
        if self._writer is None:
            raise UnknownWriter("Cannot write to a closed writer")
        # NOTE: this is fire and forget - we don't wait for a callback
        self.client.update(self._writer, {mut.row: mut.updates})

    @gen.coroutine
    def flush(self):
        if self._writer is None:
            raise UnknownWriter("Cannot flush a closed writer")
        res = yield self.client.flush(self._writer)
        raise gen.Return(res)

    @gen.coroutine
    def close(self):
        if self._writer is not None:
            res = yield self.client.closeWriter(self._writer)
            self._writer = None
            raise gen.Return(res)


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
    @gen.coroutine
    def create(conn, table, scanrange, cols, auths, iterators):
        scanner = Scanner(conn)
        options = ScanOptions(auths, scanner._get_range(scanrange), _get_scan_columns(cols),
                              scanner._get_iterator_settings(iterators), bufferSize=None)
        res = yield scanner.client.createScanner(scanner.login, table, options)
        scanner._scanner = res
        raise gen.Return(scanner)

    @staticmethod
    @gen.coroutine
    def create_batch(conn, table, scanranges, cols, auths, iterators):
        scanner = Scanner(conn)
        options = BatchScanOptions(auths, scanner._get_ranges(scanranges), _get_scan_columns(cols),
                                   scanner._get_iterator_settings(iterators), threads=None)
        res = yield scanner.client.createBatchScanner(scanner.login, table, options)
        scanner._scanner = res
        raise gen.Return(scanner)

    @gen.coroutine
    def next(self, batchsize=SCAN_BATCH_SIZE):
        res = yield self.client.nextK(self._scanner, batchsize)
        self.batch = res
        entries = []
        if self.batch.results:
            entries = [Cell(e.key.row, e.key.colFamily, e.key.colQualifier, e.key.colVisibility, e.key.timestamp,
                            e.value) for e in self.batch.results]
        raise gen.Return(entries)

    def has_next(self):
        if self.batch is None:
            return True
        return self.batch.more

    @gen.coroutine
    def close(self):
        res = yield self.client.closeScanner(self._scanner)
        raise gen.Return(res)


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
    @gen.coroutine
    def create_and_connect(host, port, user, password, **kwargs):
        acc = Accumulo(host, port, **kwargs)
        yield acc.connect(user, password)
        raise gen.Return(acc)

    @gen.coroutine
    def connect(self, user, password):
        yield self.transport.open()
        res = yield self.client.login(user, {'password': password})
        self.login = res

    def close(self):
        self.transport.close()

    @gen.coroutine
    def list_tables(self):
        res = yield self.client.listTables(self.login)
        tables = [t for t in res]
        raise gen.Return(tables)

    @gen.coroutine
    def table_exists(self, table):
        res = yield self.client.tableExists(self.login, table)
        raise gen.Return(res)

    @gen.coroutine
    def create_table(self, table):
        res = yield self.client.createTable(self.login, table, True, TimeType.MILLIS)
        raise gen.Return(res)

    @gen.coroutine
    def delete_table(self, table):
        res = yield self.client.deleteTable(self.login, table)
        raise gen.Return(res)

    @gen.coroutine
    def clone_table(self, table, new_table, flush, props_to_set, props_to_exclude):
        res = yield self.client.cloneTable(self.login, table, new_table, flush, props_to_set, props_to_exclude)
        raise gen.Return(res)

    @gen.coroutine
    def export_table(self, table, export_dir):
        res = yield self.client.exportTable(self.login, table, export_dir)
        raise gen.Return(res)

    @gen.coroutine
    def import_table(self, table, import_dir):
        res = yield self.client.importTable(self.login, table, import_dir)
        raise gen.Return(res)

    @gen.coroutine
    def offline_table(self, table, wait):
        res = yield self.client.offlineTable(self.login, table, wait)
        raise gen.Return(res)

    @gen.coroutine
    def online_table(self, table, wait):
        res = yield self.client.onlineTable(self.login, table, wait)
        raise gen.Return(res)

    @gen.coroutine
    def delete_rows(self,  table, start_row, end_row):
        res = yield self.client.deleteRows(self.login, table, start_row, end_row)
        raise gen.Return(res)

    @gen.coroutine
    def rename_table(self, oldtable, newtable):
        res = yield self.client.renameTable(self.login, oldtable, newtable)
        raise gen.Return(res)

    @gen.coroutine
    def write(self, table, muts):
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        writer = yield self.create_batch_writer(table)
        yield writer.add_mutations(muts)
        yield writer.close()

    @gen.coroutine
    def create_scanner(self, table, scanrange=None, cols=None, auths=None, iterators=None):
        scanner = yield Scanner.create(self, table, scanrange, cols, auths, iterators)
        raise gen.Return(scanner)

    @gen.coroutine
    def create_batch_scanner(self, table, scanranges=None, cols=None, auths=None, iterators=None):
        scanner = yield Scanner.create_batch(self, table, scanranges, cols, auths, iterators)
        raise gen.Return(scanner)

    @gen.coroutine
    def create_batch_writer(self, table):
        bw = yield BatchWriter.create(self, table, **BW_DEFAULTS)
        raise gen.Return(bw)

    @gen.coroutine
    def attach_iterator(self, table, setting, scopes):
        res = yield self.client.attachIterator(self.login, table, setting, scopes)
        raise gen.Return(res)

    @gen.coroutine
    def remove_iterator(self, table, iterator, scopes):
        res = yield self.client.removeIterator(self.login, table, iterator, scopes)
        raise gen.Return(res)

    @gen.coroutine
    def following_key(self, key, part):
        res = yield self.client.getFollowing(key, part)
        raise gen.Return(res)

    @gen.coroutine
    def get_max_row(self, table, auths=None, srow=None, sinclude=None, erow=None, einclude=None):
        res = yield self.client.getMaxRow(self.login, table, auths, srow, sinclude, erow, einclude)
        raise gen.Return(res)

    @gen.coroutine
    def add_mutations(self, table, muts):
        """
        Add mutations to a table without the need to create and manage a batch writer.
        """
        if not isinstance(muts, list) and not isinstance(muts, tuple):
            muts = [muts]
        cells = {}
        for mut in muts:
            cells.setdefault(mut.row, []).extend(mut.updates)
        res = yield self.client.updateAndFlush(self.login, table, cells)
        raise gen.Return(res)

    @gen.coroutine
    def create_user(self, user, password):
        res = yield self.client.createLocalUser(self.login, user, password)
        raise gen.Return(res)

    @gen.coroutine
    def drop_user(self, user):
        res = yield self.client.dropLocalUser(self.login, user)
        raise gen.Return(res)

    @gen.coroutine
    def list_users(self):
        res = yield self.client.listLocalUsers(self.login)
        raise gen.Return(res)

    @gen.coroutine
    def set_user_authorizations(self, user, auths):
        res = yield self.client.changeUserAuthorizations(self.login, user, auths)
        raise gen.Return(res)

    @gen.coroutine
    def get_user_authorizations(self, user):
        res = yield self.client.getUserAuthorizations(self.login, user)
        raise gen.Return(res)

    @gen.coroutine
    def grant_system_permission(self, user, perm):
        res = yield self.client.grantSystemPermission(self.login, user, perm)
        raise gen.Return(res)

    @gen.coroutine
    def revoke_system_permission(self, user, perm):
        res = yield self.client.revokeSystemPermission(self.login, user, perm)
        raise gen.Return(res)

    @gen.coroutine
    def has_system_permission(self, user, perm):
        res = yield self.client.hasSystemPermission(self.login, user, perm)
        raise gen.Return(res)

    @gen.coroutine
    def grant_table_permission(self, user, table, perm):
        res = yield self.client.grantTablePermission(self.login, user, table, perm)
        raise gen.Return(res)

    @gen.coroutine
    def revoke_table_permission(self, user, table, perm):
        res = yield self.client.revokeTablePermission(self.login, user, table, perm)
        raise gen.Return(res)

    @gen.coroutine
    def has_table_permission(self, user, table, perm):
        res = yield self.client.hasTablePermission(self.login, user, table, perm)
        raise gen.Return(res)

    @gen.coroutine
    def add_splits(self, table, splits):
        res = yield self.client.addSplits(self.login, table, splits)
        raise gen.Return(res)

    @gen.coroutine
    def add_constraint(self, table, class_name):
        res = yield self.client.addConstraint(self.login, table, class_name)
        raise gen.Return(res)

    @gen.coroutine
    def list_constraints(self, table):
        res = yield self.client.listConstraints(self.login, table)
        raise gen.Return(res)

    @gen.coroutine
    def remove_constraint(self, table, constraint):
        """
        :param table: table name
        :param constraint: the constraint number as returned by list constraints
        """
        res = yield self.client.removeConstraint(self.login, table, constraint)
        raise gen.Return(res)
