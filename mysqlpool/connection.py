#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author : Thomas Huang
Date : 2013-05-01
Description:
    MySQL connection 
"""
from itertools import chain

import os
import MySQLdb
import MySQLdb.constants
import MySQLdb.converters
import MySQLdb.cursors
import time

__version__ = "0.1"
__author__ = "Thomas Huang"
__date__ = "2013-05-01"

class Connection:
    """MySQL Connection
    """
    def __init__(self, max_conns,
                 closeable = True, **kwargs):
        """
        @Args:
            kwargs.keys ->[host, db, user, passwd, use_unicode, charset,...]
        @Return:
        """
        self.init(**kwargs)
        
        self._max_conns = max_conns
        self._closeable = closeable
        
        
    def init(self, **kwargs):
        self._con = None
        self._close = True
        self._conn_kwargs = {
            'charset': 'utf8',
            'use_unicode': True,
        } 
        self._conn_kwargs.update(kwargs)
        self.max_idle_sec = 25200
        self._last_use_sec = time.time()
        self._lock = Semaphore()
        self._locked = False
    
    def _store(self):
        self.reconnect()
        self._transaction = False
        self._closed = False
        self._used = 0
    
    def __del__(self):
        self.close()
        
    def cursor(self, cursor_type = None):
        self._ensure_connected()
        if cursortype == None:
            return self._con.cursor()
        
        return self._con.cursor(cursor_type)
        
    def reconnect(self):
        self.disconnet()
        self._con = MySQLdb.connect(**self._db_settings)
        
    def close(self):
        if self.closeable:
            self._close()
        elif self._transaction():
            self._reset()
        
    def _close(self):
         if getattr(self, "_con", None) is not None:
            self._con.close()
            self._con = None
         self._transaction = False
         self._closed = True
    
    def _reset(self, force = False):
        
        if force or self._transaction :
            try:
                self.rollback()
            except Exception:
                pass
        
    def commit(self):
        self._con.commit()
        
    def rollback(self):
        self._con.rollback()
        
    def begin(self):
        pass
        
    def _ensure_connected(self):
        if self._con is None or (time.time() - self._last_use_sec > self.max_idle_sec):
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

