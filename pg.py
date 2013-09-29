import random
from collections import deque

import psycopg2
from tornado.stack_context import wrap
from tornado.ioloop import IOLoop
from tornado.concurrent import return_future

class WrapCursor:
    def __init__(self,db,cur):
        self._db = db
        self._cur = cur
        self._init_member()

    def __iter__(self):
        return self._cur

    @return_future
    def execute(self,sql,param = None,callback = None):
        def _cb(err = None):
            if err != None:
                raise err

            self.arraysize = self._cur.arraysize
            self.itersize = self._cur.itersize
            self.rowcount = self._cur.rowcount
            self.rownumber = self._cur.rownumber
            self.lastrowid = self._cur.lastrowid
            self.query = self._cur.query
            self.statusmessage = self._cur.statusmessage

            callback()

        self._db._execute(self._cur,sql,param,_cb) 
        
    def _init_member(self):
        self.fetchone = self._cur.fetchone
        self.fetchmany = self._cur.fetchmany
        self.fetchall = self._cur.fetchall
        self.scroll = self._cur.scroll
        self.cast = self._cur.cast
        self.tzinfo_factory = self._cur.tzinfo_factory

        self.arraysize = 0
        self.itersize = 0
        self.rowcount = 0
        self.rownumber = 0
        self.lastrowid = None
        self.query = ''
        self.statusmessage = ''

class AsyncPG:
    def __init__(self,dbname,dbuser,dbpasswd,dbschema = 'public'):
        self.OPER_CURSOR = 0
        self.OPER_EXECUTE = 1
        
        self._ioloop = IOLoop.instance()
        self._dbname = dbname
        self._dbuser = dbuser
        self._dbpasswd = dbpasswd
        self._dbschema = dbschema
        self._share_connpool = []
        self._free_connpool = []
        self._conn_fdmap = {}

        for i in range(4):
            conn = self._create_conn()
            self._share_connpool.append(conn)

            self._ioloop.add_handler(conn[0],self._dispatch,IOLoop.ERROR)

            conn[2] = True
            self._ioloop.add_callback(self._dispatch,conn[0],0)

        for i in range(0):
            conn = self._create_conn()
            self._free_connpool.append(conn)

            self._ioloop.add_handler(conn[0],self._dispatch,IOLoop.ERROR)

            conn[2] = True
            self._ioloop.add_callback(self._dispatch,conn[0],0)

    @return_future
    def cursor(self,callback):
        def _cb(cur,err = None):
            if err != None:
                raise err

            callback(WrapCursor(self,cur))

        self._cursor(callback = _cb)

    def _cursor(self,conn = None,callback = None):
        def _cb(err = None):
            if err != None:
                callback(None,err)

            callback(conn[4].cursor())

        if conn == None:
            conn = self._share_connpool[
                    random.randrange(len(self._share_connpool))]

        conn[1].append((self.OPER_CURSOR,None,wrap(_cb)))

        if conn[2] == False:
            conn[2] = True
            self._ioloop.add_callback(self._dispatch,conn[0],0)

    def _execute(self,cur,sql,param,callback):
        conn = self._conn_fdmap[cur.connection.fileno()]
        
        conn[1].append((self.OPER_EXECUTE,(cur,sql,param),wrap(callback)))

        if conn[2] == False:
            conn[2] = True
            self._ioloop.add_callback(self._dispatch,conn[0],0)

    def _create_conn(self):
        dbconn = psycopg2.connect(database = self._dbname,user = self._dbuser,
                    password = self._dbpasswd,async = 1,
                    options = '-c search_path=' + self._dbschema)
    
        conn = [dbconn.fileno(),deque(),False,None,dbconn]
        self._conn_fdmap[conn[0]] = conn

        return conn

    def _close_conn(self,conn):
        self._conn_fdmap.pop(conn[0],None)
        conn[4].close()

    def _dispatch(self,fd,evt):
        err = None

        try:
            conn = self._conn_fdmap[fd]

        except KeyError:
            self._ioloop.remove_handler(fd)
            return

        try:
            stat = conn[4].poll()

        except Exception as e:
            err = e

        if err != None or stat == psycopg2.extensions.POLL_OK:
            self._ioloop.update_handler(fd,IOLoop.ERROR)

        elif stat == psycopg2.extensions.POLL_READ:
            self._ioloop.update_handler(fd,IOLoop.READ | IOLoop.ERROR)
            return

        elif stat == psycopg2.extensions.POLL_WRITE:
            self._ioloop.update_handler(fd,IOLoop.WRITE | IOLoop.ERROR)
            return

        cb = conn[3]
        if cb != None:
            conn[3] = None
            cb(err)

        else:
            try:
                oper,data,cb = conn[1].popleft()
                
            except IndexError:
                conn[2] = False
                return

            try:
                if oper == self.OPER_CURSOR:
                    conn[3] = cb

                elif oper == self.OPER_EXECUTE:
                    cur,sql,param = data
                    cur.execute(sql,param)
                    conn[3] = cb

            except Exception as e:
                conn[3] = None
                cb(e)

        self._ioloop.add_callback(self._dispatch,fd,0)
