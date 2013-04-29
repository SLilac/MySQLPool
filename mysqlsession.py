
#-*-coding:utf-8 -*-

from connnection import Connection
    
class MySQLSeesion:
    
    
    def __init__(self, connection):
        
        self._sql_stmt_list = []
        self.conn = connection
    def add(self, query, *params):
        self._sql_stmt_list.append((querry, params))
    def add_all(self, stmt_trans):
        self._sql_stmt_list.extend(stmt_trans)
           
    
    def commit(self):
        
        cur = conn.cursor()
        try:
            for stmt, params in self._sql_stmt_list:
                cur.execute(stmt, params)    
        except MySQLdb.Error,e:
            print e[0],e[1]
            self.conn.rollback()
        conn.commit()
        cur.close()

        
def sessionmaker(connection):
    return MySQLSeesion(connection)