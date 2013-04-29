#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author : Thomas Huang
Date : 2013-05-01
Description:
    A MySQL connection and connection pool
"""
from itertools import chain

import os
import MySQLdb
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
from Queue import LifoQueue
import time

from threading import Condition, Semaphore

class Connection:
    """MySQL Connection
    """
    def __init__(self, **kwargs):
        """
        @Args:
            kwargs.keys ->[host, db, user, passwd, use_unicode, charset,...]
        @Return:
        """
        self._db = None
        self._db_settings = kwargs
        self.max_idle_sec = 25200
        self._last_use_sec = time.time()
        self._lock = Semaphore()
        self._locked = False
        
        
    def __del__(self):
        self.disconnet()
        
    def cursor(self, cursor_type = None):
        self._ensure_connected()
        if cursortype == None:
            return self._db.cursor()
        
        return self._db.cursor(cursor_type)
        
    def reconnect(self):
        self.disconnet()
        self._db = MySQLdb.connect(**self._db_settings)
        print self._db
        
    def disconnect(self):
         if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None
                   
    def commit(self):
        self._db.commit()
        
    def rollback(self):
        self._db.rollback()
        
    def _ensure_connected(self):
        if self._db is None or (time.time() - self._last_use_sec > self.max_idle_sec):
            self.reconnect()
        
        self._last_use_sec = time.time()  
    
    def lock(self, block=True):
        """
        Lock connection from being used else where
        """
        self._locked = True
        return self._lock.acquire(block)

    def release(self):
        """
        Release the connection lock
        """
        if self._locked is True:
            self._locked = False
            self._lock.release()
    def is_locked(self):
        """
        Returns the status of this connection
        """
        return self._locked        

class MySQLPool:
    
    def __init__(self, conn_class = Connection, 
                 max_conns = None,
                 **conn_kwargs):
        #self.name = ""
        self.pid = os.getpid()
        self.conn_class = conn_class
        self.conn_kwargs = conn_kwargs
        self.max_conns = max_conns or 2 ** 31
        self._created_conns = 0
        self._available_conns = []
        self._in_use_conns = set()
        
    
    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.conn_class, self.max_conn, **self.conn_kwargs)
    
    def get_connection(self):
        """get a connection from pool"""
        self._checkpid()
        try:
            connection = self._available_conns.pop()
        except IndexError:
            connection = self.new_connection()
            self._in_use_conns.append(connection)
        return connection
    def new_connection(self):
         if self._created_conns >= self.max_conns:
            raise Exception("Too many connections")
         self._created_conns += 1
         return self.connection_class(**self.conn_kwargs)
    
    def release(self, connection):
        self._checkpid()
        if connection in self._in_use_conns:
            self._in_use_conns.remove(connection)
            self._available_conns(connection)
        
    def disconnet(self):
        #all_conns = chain(self._available_conns, self._in_use_conns)
        for conn in self._available_conns:
            conn.disconnect()
        for conn in self._in_use_conns:
            conn.disconnect()
    


    


            
class MySQLSyncPool:    
    
    def __init__(self, max_conns = 50, timeout = 20, 
                 conn_class = None, queue_class, **conkwargs):
        if conn_class  is None:
            conn_class = Connection
        if queue_class is None:
            queue_class = LifoQueue 
        
        self._lock = Condition()
        self.conn_class = conn_class
        self.conn_kwargs = conn_kwargs
        self.max_conns = max_conns 
        self.timeout = timeout
        is_valid = isinstance(max_conns, int) and max_conns > 0
        if not is_valid:
            raise ValueError('``max_connections`` must be a positive integer')
    
        self.pid = os.getpid()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(max_conns)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._conns = []
        
    def _checkpid(self):
        """
        Check the current process id.  If it has changed, disconnect and
        re-instantiate this connection pool instance.
        """
        # Get the current process id.
        pid = os.getpid()

        # If it hasn't changed since we were instantiated, then we're fine, so
        # just exit, remaining connected.
        if self.pid == pid:
            return

        # If it has changed, then disconnect and re-instantiate.
        self.disconnect()
        self.reinstance()
        
   
    
    def new_connection(self):
        self._lock.acquire()
        try:
            conn = self.conn_class(**self.conn_kwargs)
            self._conns.append(conn)
        except Exception:
            pass
        finally:
            self._lock.release()
        return conn
    
    def get_connection(self):
        self._checkpid()
        conn = None
        try:
            conne = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            raise Exception("No connection available.")
        if conn is None:
            # thread unsafe 
            conn = self.new_connection()
        return conn   
    
    def release(self, connection):
        """Releases the connection back to the pool."""
        self._checkpid()

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            pass
    
    def disconnect(self):
        """Disconnects all connections in the pool."""
        for conn in self._conns:
            try:
                conn.disconnect()
            except Exception:
                        pass  
        self._conns = []
        
            
    def reinstance(self):
 
        """
        Reinstatiate this instance within a new process with a new connection
        pool set.
        """
        
        self.__init__(max_conns=self.max_conns,
                      timeout=self.timeout,
                      conn_class=self.conn_class,
                      queue_class=self.queue_class, **self.conn_kwargs)
    
    class MySQLCherryPool:
        
        def __init_(self,conn_class, max_conns, 
                    max_shared, timetout,conn_kwargs, threadlocal):
            
            self.threadlocal = threadlocal
            
            
        
        
        def get_connection(self, shareable = True):
            pass
        
        def new_connection(self):
            pass
            
            
            
            
            
            
            
            
            
            
            
            
            
            
        