#!/usr/bin/python

import psycopg2, threading

class pqDB:
        PQ_SEM = None;
        con = None
        DBHOST='160.91.1.75'
        DB='NAC'
        def __init__(self, database, host):
                if database is not None and len(database) > 0:
                        self.DB=database
                if len(host) > 0:
                        self.DBHOST=host
                self.con = psycopg2.connect("dbname=" + self.DB + " user=harrold host=" + self.DBHOST + " password=pwd")
                self.PQ_SEM = threading.BoundedSemaphore(1)
        #
        def close_db(self):
                print "close_db()"
                self.con.close()
                self.PQ_SEM = None
        #
        def lock_DB(self):
                if self.PQ_SEM is None:
                        print "DB lock error\n"
                        exit(1)
                self.PQ_SEM.acquire() # decrements the counter
        #
        def unlock_DB(self):
                if self.PQ_SEM is None:
                        print "DB lock error\n"
                        exit(1)
                self.PQ_SEM.release() # increments the counter
        #
        def init_DB(self, max_connections):
                print "Initialized DB with ", max_connections
                self.PQ_SEM = threading.BoundedSemaphore(max_connections)
        #
        def rollback(self):
                self.lock_DB()
                self.con.rollback()
                self.unlock_DB()
        #
        def get_list(self, buffer):
                """returns a dictionary of data derived from search string, stored in the input parameter [buffer]"""
                #print "get_list(%s)"%buffer
                the_list = {}
                if buffer.lstrip().upper().startswith("SELECT"):
                        self.lock_DB()
                        try:
                                cur = self.con.cursor()
                                cur.execute(buffer)
                                the_list = cur.fetchall()
                        except Exception, e:
                                print "get_list() error occurred:", e[0]
                                print buffer
                        self.unlock_DB()
                return the_list
        #
        def get_string(self, buffer):
                """returns a single string given input search query defined in [buffer]"""
                the_string=""
                buffer = buffer.strip()
                if len(buffer) > 0 and buffer.lstrip().upper().startswith("SELECT"):
                        self.lock_DB()
                        try:
                                cur = self.con.cursor()
                                cur.execute(buffer)
                                the_string = cur.fetchone()[0]
                        except Exception, e:
                                print "get_string() error occurred:", e[0]
                                print buffer
                        self.unlock_DB()
                # end if
                return the_string
        #
        def execute_command(self, buffer):
                """Executes sqlstatement defined in the input parameter [buffer]"""
                myBuf=buffer.strip()
                if len(myBuf)==0:
                        print "execute_command() empty search string buffer"
                        return 0
                myRet=1
                myBuf="%s; commit;"%buffer
                self.lock_DB()
                try:
                        cur = self.con.cursor()
                        cur.execute(myBuf)
                except Exception, e:
                        print "execute_command() error occurred:", e[0]
                        print buffer
                        self.unlock_DB()
                        exit(1)
                        myRet=0
                self.unlock_DB()
                return myRet
        #
        def get_count(self, buffer):
                """returns a single string given input search query defined in [buffer]"""
                myRet=self.get_string(buffer);
                if myRet is None or myRet=="":
                        return -1;
                return int(myRet)
