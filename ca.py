import struct
import socket
import collections
import time
import datetime
import uuid

from tornado.stack_context import wrap
from tornado.ioloop import IOLoop
from tornado.concurrent import return_future
from tornado.iostream import IOStream
from tornado.stack_context import wrap

import uuid
import tornado.gen

class Cursor:
    def __init__(self,curid,db):
        self._curid = curid
        self._db = db

        self._res = []
        self.rowcount = 0

    @return_future
    def execute(self,statem,param = [],callback = None):
        def _cb(data,err = None):
            if err != None:
                raise err
            
            if data == None:
                self._res = []
                self.rowcount = 0

            else:
                self._res = data
                self.rowcount = len(data)

            callback(None)

        self._db._execute(self._curid,statem,param,_cb)

    def fetchone(self):
        return self._res[0]

    def __iter__(self):
        return self._res.__iter__()

class AsyncCA:
    def __init__(self,keysp):
        def _conn_cb():
            self._send_req(0x01,self._notat_map({
                'CQL_VERSION':'3.0.0'
            }),_res_cb,force = True)

        def _res_cb(opcode,data):
            if opcode == 0x02:
                self._execute(0,'USE %s;'%(keysp),[],_ready_cb,force = True)

            else:
                print('error')

        def _ready_cb(data,err = None):
            if err != None:
                print('error')
                return

            self._state = self.STATE_READY
            self._run_pend()

        self.STATE_PEND = 0
        self.STATE_READY = 1

        self._ioloop = IOLoop.instance()
        self._state = self.STATE_PEND
        self._free_sidlist = list(range(1,128))
        self._wait_sidmap = {}
        self._pend_reqmap = collections.OrderedDict()
        self._pend_lastid = 0
        self._cur_lastid = 0    #0 is for itself
        self._cur_idmap = {}

        self._stm = IOStream(socket.socket(socket.AF_INET,socket.SOCK_STREAM,0))
        self._stm.connect(('localhost',9042),_conn_cb)

        self._recv_loop()

    @return_future
    def cursor(self,callback):
        self._cur_lastid += 1
        cur = Cursor(self._cur_lastid,self)
        self._cur_idmap[self._cur_lastid] = set()

        callback(cur)

    def _parse_res(self,data):
        kind, = struct.unpack('!I',data[0:4])

        if kind == 0x0001:
            return None

        elif kind == 0x0002:
            flags,col_count = struct.unpack('!II',data[4:12])
            
            off = 12
            if (flags & 0x0001) != 0x0001:  #Global_tables_spec
                col_count -= 2

            if (flags & 0x0002) == 0x0002:  #Has_more_pages
                size, = struct.unpack('!I',data[12:16])
                page_stat = data[12:12 + size]
                print(page_stat)

            if (flags & 0x0004) == 0x0004:  #No_metadata
                return None

            size, = struct.unpack('!H',data[off:off + 2])
            kp_name = data[off + 2:off + 2 + size].decode('utf-8')
            off += 2 + size

            size, = struct.unpack('!H',data[off:off + 2])
            tb_name = data[off + 2:off + 2 + size].decode('utf-8')
            off += 2 + size

            collist = []
            for colidx in range(col_count):
                size, = struct.unpack('!H',data[off:off + 2])
                col_name = data[off + 2:off + 2 + size].decode('utf-8')
                off += 2 + size

                type_id, = struct.unpack('!H',data[off:off + 2])
                off += 2
                
                collist.append((col_name,type_id))

            row_count, = struct.unpack('!I',data[off:off + 4])
            off += 4
            reslist = []
            for rowidx in range(row_count):
                rowlist = []
                for colidx in range(col_count):
                    col_name,type_id = collist[colidx]

                    size, = struct.unpack('!i',data[off:off + 4])
                    off += 4
                    if size == -1:
                        val = None

                    else:
                        if type_id == 0x0000:      #Custom
                            stlen, = struct.unpack('!H',data[off:off + 2])
                            val = data[off + 2:off + 2 + stlen].decode('utf-8')

                        elif type_id == 0x0001:    #ascii
                            raise NotImplementedError
                        elif type_id == 0x0002:    #bigint
                            raise NotImplementedError
                        elif type_id == 0x0003:    #blob
                            val = data[off:off + size]
                            off += size

                        elif type_id == 0x0004:    #boolean
                            assert size == 1,'Boolean > 1 byte'
                            val = bool(data[off:off + 1][0])

                        elif type_id == 0x0005:    #counter
                            val, = struct.unpack('!q',data[off:off + 8])

                        elif type_id == 0x0006:    #decimal
                            raise NotImplementedError
                        elif type_id == 0x0007:    #double
                            raise NotImplementedError
                        elif type_id == 0x0008:    #float
                            raise NotImplementedError
                        elif type_id == 0x0009:    #int
                            val, = struct.unpack('!i',data[off:off + 4])

                        elif type_id == 0x000B:    #timestamp
                            ts, = struct.unpack('!Q',data[off:off + 8])
                            val = datetime.datetime.utcfromtimestamp(
                                    ts // 1000)
                            val = val.replace(tzinfo = datetime.timezone.utc)

                        elif type_id == 0x000C or type_id == 0x000F:    #uuid
                            val = uuid.UUID(bytes = data[off:off + size])

                        elif type_id == 0x000D:    #varchar
                            val = data[off:off + size].decode('utf-8')

                        elif type_id == 0x000E:    #varint
                            raise NotImplementedError
                        elif type_id == 0x0010:    #inet
                            raise NotImplementedError
                        elif type_id == 0x0020:    #list
                            raise NotImplementedError
                        elif type_id == 0x0021:    #map
                            raise NotImplementedError
                        elif type_id == 0x0022:    #set
                            raise NotImplementedError

                        off += size

                    rowlist.append(val)

                reslist.append(rowlist)

            return reslist
            
        elif kind == 0x0003:
            size, = struct.unpack('!H',data[4:6])
            return data[6:6 + size].decode('utf-8')

    def _execute(self,curid,statem,param,callback,force = False):
        def _cb(opcode,data):
            if opcode == 0x08:
                callback(self._parse_res(data))

            else:
                code,size = struct.unpack('!IH',data[:6])
                val = data[6:6 + size].decode('utf-8')
                callback(None,err = Exception(val))

        def _bytify(val):
            if isinstance(val,str):
                return val.encode('utf-8')

            elif isinstance(val,int):
                return struct.pack('!I',val)

            elif isinstance(val,uuid.UUID):
                return val.bytes

            elif isinstance(val,datetime.datetime):
                if val.tzinfo == None:
                    delta = datetime.timedelta(seconds=time.timezone)
                    utctime = (val + delta).replace(
                            tzinfo = datetime.timezone.utc)

                else:
                    utctime = val.astimezone(datetime.timezone.utc)

                return struct.pack('!Q',round(utctime.timestamp() * 1000))

            else:
                raise TypeError

        data = bytearray(self._notat_lstring(statem))
        data.extend(struct.pack('!HBH',0x0001,0x01,len(param)))
        for val in param:
            if isinstance(val,list):
                listdata = bytearray(struct.pack('!H',len(val)))
                for elem in val:
                    elemdata = _bytify(elem)
                    listdata.extend(struct.pack('!H',len(elemdata)))
                    listdata.extend(elemdata)

                data.extend(self._notat_bytes(listdata))

            else:
                data.extend(self._notat_bytes(_bytify(val)))

        pendid = self._send_req(0x07,data,_cb,force)
        if pendid != None:
            self._cur_idmap[curid].add(pendid)

    def _send_req(self,opcode,body,callback,force = False):
        callback = wrap(callback)

        if len(self._free_sidlist) > 0 and (self._state == self.STATE_READY or
                force == True):
            sid = self._free_sidlist.pop()

            self._wait_sidmap[sid] = callback
            data = bytearray(
                    struct.pack('!BBBBI',0x02,0x00,sid,opcode,len(body)))
            data.extend(body)

            self._stm.write(bytes(data))

            return None

        else:
            self._pend_lastid += 1
            self._pend_reqmap[self._pend_lastid] = (opcode,body,callback)

            return self._pend_lastid

    def _recv_loop(self):
        def _recv_cb(data):
            def __body_cb(data):
                self._stm.read_bytes(8,_recv_cb)
                
                self._free_sidlist.append(sid)
                try:
                    self._wait_sidmap.pop(sid)(opcode,data)

                except KeyError:
                    pass

                self._run_pend()

            ver,flags,sid,opcode,body_len = struct.unpack('!BBBBI',data)
            self._stm.read_bytes(body_len,__body_cb)

        self._stm.read_bytes(8,_recv_cb)

    def _run_pend(self):
        if self._state == self.STATE_READY:
            pendids = list(self._pend_reqmap.keys())
            for pendid in pendids:
                if len(self._free_sidlist) == 0:
                    break

                args = self._pend_reqmap.pop(pendid)
                assert self._send_req(*args) == None

    def _notat_map(self,mp):
        data = bytearray(struct.pack('!H',len(mp)))
        items = mp.items();
        for key,val in items:
            key = key.encode('utf-8')
            val = val.encode('utf-8')

            data.extend(
                    struct.pack('!H',len(key)) +
                    key + struct.pack('!H',len(val)) + val)

        return data

    def _notat_string(self,st):
        st = st.encode('utf-8')
        data = bytearray(struct.pack('!H',len(st)))
        data.extend(st)

        return data

    def _notat_lstring(self,st):
        st = st.encode('utf-8')
        data = bytearray(struct.pack('!I',len(st)))
        data.extend(st)

        return data

    def _notat_bytes(self,by):
        data = bytearray(struct.pack('!I',len(by)))
        data.extend(by)

        return data

@tornado.gen.coroutine
def test():
    cur = yield db.cursor('sprout')
    yield cur.execute('SELECT aaa FROM POST;')

    for data in cur:
        print(data)

if __name__ == '__main__':
    db = AsyncCA('SPROUT')

    test()

    IOLoop.instance().start()
