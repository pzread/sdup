import socket
import struct
import json
import tornado.iostream
from tornado.ioloop import IOLoop
from tornado.stack_context import wrap
from tornado.concurrent import return_future

import time
from tornado.gen import coroutine

class AsyncMCD:
    def __init__(self):
        self.TYPE_INT = 0
        self.TYPE_BYTES = 1
        self.TYPE_STR = 2
        self.TYPE_JSON = 3

        self._ioloop = IOLoop.instance()
        self._opaque_count = 0
        self._opaque_map = {}

        self._stm = tornado.iostream.IOStream(
                socket.socket(socket.AF_INET,socket.SOCK_STREAM,0))
        self._stm.connect(('localhost',11211))

        self._recv_loop()

    @return_future
    def get(self,ori_key,callback):
        def _recv(opcode,status,opaque,cas,extra,key,value):
            del self._opaque_map[opaque]

            if status != 0:
                callback(None)

            else:
                flag, = struct.unpack('!I',extra)

                if flag == self.TYPE_INT:
                    ret = int(value)

                elif flag == self.TYPE_BYTES:
                    ret = value

                elif flag == self.TYPE_STR:
                    ret = value.decode('utf-8')

                elif flag == self.TYPE_JSON:
                    ret = json.loads(value.decode('utf-8'))

                callback(ret)

        if not isinstance(ori_key,str):
            raise TypeError

        key = bytes(ori_key,'utf-8')
        keylen = len(key)

        opaque = self._get_opaque(_recv)
        header = self._request_header(0x00,keylen,0,0,keylen,opaque,0)
        data = bytes(bytearray().join([header,key]))

        self._stm.write(data)
    
    @return_future
    def mget(self,keys,callback):
        def _recv(opcode,status,opaque,cas,extra,key,value):
            del self._opaque_map[opaque]

            if status == 0:
                flag, = struct.unpack('!I',extra)
                if flag == self.TYPE_INT:
                    ret = int(value)

                elif flag == self.TYPE_BYTES:
                    ret = value

                elif flag == self.TYPE_STR:
                    ret = value.decode('utf-8')

                elif flag == self.TYPE_JSON:
                    ret = json.loads(value.decode('utf-8'))

                rets[key_opaqmap[opaque]] = ret

        def _lrecv(*args,**kwargs):
            _recv(*args,**kwargs)
            callback(rets)

        for key in keys:
            if not isinstance(key,str):
                raise TypeError

        rets = {}
        key_opaqmap = {}
        qkeys = keys[:-1]

        for ori_key in qkeys:
            key = bytes(ori_key,'utf-8')
            keylen = len(key)

            opaque = self._get_opaque(_recv)
            key_opaqmap[opaque] = ori_key
            header = self._request_header(0x09,keylen,0,0,keylen,opaque,0)
            data = bytes(bytearray().join([header,key]))

            self._stm.write(data)
        
        key = bytes(keys[-1],'utf-8')
        keylen = len(key)

        opaque = self._get_opaque(_lrecv)
        key_opaqmap[opaque] = keys[-1]
        header = self._request_header(0x00,keylen,0,0,keylen,opaque,0)
        data = bytes(bytearray().join([header,key]))

        self._stm.write(data)

    @return_future
    def set(self,key,value,expiration = 0,callback = None):
        self._store(0x01,key,value,expiration,callback)

    @return_future
    def add(self,key,value,expiration = 0,callback = None):
        self._store(0x02,key,value,expiration,callback)

    @return_future
    def replace(self,key,value,expiration = 0,callback = None):
        self._store(0x03,key,value,expiration,callback)

    def _store(self,opcode,ori_key,value,expiration,callback):
        def _recv(opcode,status,opaque,cas,extra,key,value):
            del self._opaque_map[opaque]
            callback(None)

        if not isinstance(ori_key,str):
            raise TypeError

        key = bytes(ori_key,'utf-8')
        keylen = len(key)

        if isinstance(value,int):
            value_type = self.TYPE_INT
            value = bytes(str(value),'ascii')

        elif isinstance(value,bytes):
            value_type = self.TYPE_BYTES

        elif isinstance(value,str):
            value_type = self.TYPE_STR
            value = bytes(value,'utf-8')

        else:
            value_type = 2
            value = bytes(json.dumps(value),'utf-8')

        valuelen = len(value)

        extra = struct.pack('!II',value_type,expiration)
        extralen = len(extra)

        opaque = self._get_opaque(_recv)
        header = self._request_header(
                opcode,keylen,extralen,0,extralen + keylen + valuelen,opaque,0)
        data = bytes(bytearray().join([header,extra,key,value]))

        self._stm.write(data)

    @return_future
    def delete(self,ori_key,callback):
        def _recv(opcode,status,opaque,cas,extra,key,value):
            del self._opaque_map[opaque]
            callback(None)

        if not isinstance(ori_key,str):
            raise TypeError

        key = bytes(ori_key,'utf-8')
        keylen = len(key)

        opaque = self._get_opaque(_recv)
        header = self._request_header(0x04,keylen,0,0,keylen,opaque,0)
        data = bytes(bytearray().join([header,key]))

        self._stm.write(data)


    def _get_opaque(self,data):
        self._opaque_count += 1
        self._opaque_map[self._opaque_count] = data

        return self._opaque_count

    def _request_header(self,opcode,keylen,extralen,vid,totallen,opaque,cas):
        return struct.pack('!BBHBBHIIQ',0x80,opcode,keylen,extralen,0x0,vid,totallen,opaque,cas)

    def _recv_loop(self):
        def __recv(data):
            def ___recvdata(data):
                extra = data[0:extralen]
                key = data[extralen:extralen + keylen]
                value = data[extralen + keylen:totallen]

                self._opaque_map[opaque](opcode,status,opaque,cas,extra,key,value)
                self._stm.read_bytes(24,__recv)

            header = struct.unpack('!BBHBBHIIQ',data)
            opcode = header[1]
            keylen = header[2]
            extralen = header[3]
            status = header[5]
            totallen = header[6]
            opaque = header[7]
            cas = header[8]

            if totallen == 0:
                self._opaque_map[opaque](opcode,status,opaque,cas,bytes(),bytes(),bytes())
                self._stm.read_bytes(24,__recv)

            else:
                self._stm.read_bytes(totallen,___recvdata)

        self._stm.read_bytes(24,__recv)


'''
@coroutine
def test():
    ret = yield mc.get('test')
    yield mc.delete('test')
    print(ret)

mc = AsyncMCD()
test()
tornado.ioloop.IOLoop.instance().start()
'''
