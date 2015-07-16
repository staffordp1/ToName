#!/usr/bin/python


# uses postgres NACmgr's database
# connects globally and with an import statement

# load this library from yum update (python-psycopg2)
import psycopg2
con=psycopg2.connect("dbname=myDB user=U33 password=abc123")


def commit():
        con.commit()

def close_db():
        con.commit()
        con.close()

def rollback():
        con.rollback()

def get_list(buffer):
        """returns a dictionary of data derived from search string, stored in the input parameter [buffer]"""
        the_list = {}
        if buffer.lstrip().upper().startswith("SELECT"):
                try:
                        cur = con.cursor()
                        cur.execute(buffer)
                        the_list = cur.fetchall()
                except Exception, e:
                        print "get_list() error occurred:", e[0]
                        print buffer
                        pass
        return the_list

def get_count(buffer):
        """returns a single string given input search query defined in [buffer]"""
        myRet=0
        try:
                myRet=get_string(buffer);
        except Exception, e:
                return 0
        return int(myRet)

def get_string(buffer):
        """returns a single string given input search query defined in [buffer]"""
        the_string=""
        if len(buffer) > 0 and buffer.lstrip().upper().startswith("SELECT"):
                try:
                        buffer = buffer.strip()
                        cur = con.cursor()
                        cur.execute(buffer)
                        the_string = cur.fetchone()[0]
                except Exception, e:
                        print "get_string() error occurred:", e[0]
                        print buffer
                        pass
                #end try
        # end if
        return the_string

def get_zones():
        """returns a list of zones defined in the OpenDNSSEC database"""
        zones = get_list("select name, input, output from zones")
        myZones = {}
        for x in zones:
                key = x[0]
                myZones [ key ] = [x[1], x[2]]  # storing list of two files [ unsigned, signed ]
        return myZones

def execute_command(buffer):
        """Executes sqlstatement defined in the input parameter [buffer]"""
        buffer=buffer.strip()
        cur = con.cursor()
        try:
                cur.execute(buffer)
        except Exception, e:
                print "execute_command() error occurred:", e[0]
                print buffer
                return 0
        return 1
