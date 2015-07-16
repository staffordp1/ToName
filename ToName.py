#!/usr/bin/python
#
# Written by Paige Stafford, November 2012
#

"""
A fast, lightweight IPv4/IPv6 tool to digest log files to replace
IP addresses with IP Names using DNS.

-----------------------------------------------------------------
Execute with -h option for usage information.
-----------------------------------------------------------------

"""

__version__ = '1.0'

import cx_db
import os, sys
import socket
import re
import ipaddress

STORED_DATA={}  # data used, as requested tuples of [address, name] from NAC.dns_ips
                # If NOT using data stored on disk, this is strictly a DNS cache
                # all data is stored in a zero delimited format (e.g. '192.168.001.001')

def usage():
        print '''
Usage:  ./toName.py  [-h -r, -b, -reset] [-f file]
        -h              print this output
        -r              Uses stored data (written to disk), and adds new data as encountered
        -b              Write both name and address in this format:  address (name).  If not resolvable, name is empty.
        -reset          zero out the data stored on disk *before* proceeding
        -f file         The file is read instead of stdin.

        Default is to read from stdin and to use DNS only.  Data stored on disk is not checked or updated.
        Default output is to replace the IP address, if it resolves, with the resolved IP name.
        Negative DNS responses are only stored in the current execution memory, and not stored to disk
'''
        exit(1);


def get_options():
        UpdateStored=0;
        PrintBoth=0;
        ReSet=0;
        file_name='';
        sys_len = len(sys.argv)
        if sys_len == 1:
                return UpdateStored, PrintBoth, ReSet, file_name;
        #
        read_file=0;
        for num in range(1, sys_len):
                if(read_file == 1):
                        read_file=0
                        continue
                word=sys.argv[num];
                if word == "-h" or word == "--help":
                        usage();
                if word == "-r":
                        UpdateStored=1;
                elif word == "-b":
                        PrintBoth=1;
                elif word == "-reset":
                        ReSet=1;
                elif word == "-f":
                        if num == sys_len-1:
                                print "File name missing for flag '-f'"
                                usage();
                        read_file=1;
                        num=num+1;
                        file_name=sys.argv[num];
                        try: #check_file accessability
                                os.access(file_name, os.R_OK)
                        except OSError, e:
                                print "Unable to read file '%s'\n"% filename, e
                                exit (1)
                else:
                        usage();
        # return newly defined params
        return UpdateStored, PrintBoth, ReSet, file_name;

def fmt_ipv6_for_db(ip_addr):
        try:
                ip = ipaddress.IPv6Address(ip_addr);
        except ipaddress.AddressValueError:
                #unable to format for v6
                return ip_addr;
        return ip.exploded

def fmt_ipv4_for_db(ip_addr):
        octets=ip_addr.split(".");
        ipv4_addr=[]
        for i in octets:
                myNum = "%s" % int(i)
                ipv4_addr.append(myNum.zfill(3))
        newIPv4=".".join(ipv4_addr)
        return newIPv4

def check_DNS(ip_addr): #checking DNS for ip address
        try:
                [name, aliaslist, addresslist] = socket.gethostbyaddr(ip_addr);
                return name
        except socket.herror:
                return ""

def update_STORED_DATA(ip, name):
        buffer="insert into dns_ips (ip_addr, ip_name) values ('%s', '%s')"% (ip, name);
        if(cx_db.exececute_command(buffer)) == 0:
                print >> sys.stderr, "Failed to add record to the Stored DNS DATA (NAC.dns_ips)"
#
def check_data(type, ip_addr, UpdateStored):
        name=""
        ip=""
        if(len(ip_addr)==0):
                return name
        if(type=="4"):
                ip=fmt_ipv4_for_db(ip_addr);
        elif(type=="6"):
                ip=fmt_ipv6_for_db(ip_addr);
                if len(ip)==0: #Invalid IPv6 Address
                        return name;
        # check cache first
        if ip in STORED_DATA:
                #found in data
                return STORED_DATA[ip]
        #
        # it's not in STORED data
        # No success yet...or just wanting DNS.
        name = check_DNS(ip)
        # if not resolved, replace the name with a blank
        if len(name)==0:
                STORED_DATA[ip]="";
        elif UpdateStored == 1:
                update_STORED_DATA(ip, name);
                STORED_DATA[ip]=name;
        return name

# outside:2001:470:0:d6::2/45739
# using ext-dns:2620:0:2b30:304::32/53
# (2001:470:0:d6::2/45739)
# observed separators: ':', '=', '/', '(' with ')'

def testv6(word):
        v6List=[]
        testList=word.split(':');
        if (len(testList)==1) or ( (len(testList)<8) and (len(word.split('::'))==1) ):
                return None
        hWord=word.replace('=', ' ');
        hWord=hWord.replace('/', ' ');
        hWord=hWord.replace('(', ' ');
        hWord=hWord.replace(')', ' ');
        wordList=hWord.split(' ');
        # remove unwanted short strings
        for index, item in enumerate(wordList):
                if len(item) < 10:
                        wordList.pop(index)
        # for each remaining word
        for item in wordList:
                # separate these by the ':' and analyze each one
                subList=item.split(':');
                # remove beginning invalid formatted strings...
                for index, hWord in enumerate(subList):
                        # if anyone one of the string match, stop
                        matches=re.findall("(?m)[0-9a-fA-F]{1,4}", hWord);
                        if len(matches) != 1 or matches[0]!=hWord:
                                subList.pop(index)
                        else:
                                # the first time it's a good string, break
                                break;
                #end of for loop
                ipv6_addr="";
                for index, hWord in enumerate(subList):
                        if len(hWord)==0:
                                break;
                myAddr= ':'.join(subList)
                v6List.append(myAddr);
        # if not returned at this point, return failure (None) result
        return v6List
def process_line(line, PrintBoth, UpdateStored):
        words = line.split();
        for word in words:
                v4matches = re.findall("(?m)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", word);
                v6matches = testv6(word);
                if ( len(v4matches)==0) and (v6matches==None or len(v6matches)==0):   #No matches...
                        print word,
                        sys.stdout.flush()
                elif len(v4matches) > 0:
                        #IPv4 matches...
                        for addr in v4matches:  #for each ip address retrieved from word
                                name = check_data("4", addr, UpdateStored)
                                if len(name) > 0:
                                        if(PrintBoth):
                                                newWord="%s (%s)"% (addr, name)
                                                word = word.replace(addr, newWord);
                                        else:
                                                word = word.replace(addr, name);
                        #for addr in v6matches:
                        print word,
                        sys.stdout.flush()
                #
                elif v6matches!=None and len(v6matches) > 0:
                        #IPv6 matches...
                        for addr in v6matches:  #for each ip address retrieved from word
                                name = check_data("6", addr, UpdateStored)
                                if len(name) > 0:
                                        if(PrintBoth):
                                                newWord="%s (%s)"% (addr, name)
                                                word = word.replace(addr, newWord);
                                        else:
                                                word = word.replace(addr, name);
                        #for addr in v6matches:
                        print word,
                        sys.stdout.flush()
        print
        sys.stdout.flush()

if __name__ == "__main__":
        (UpdateStored, PrintBoth, ReSet, file_name)=get_options();
        if (UpdateStored == 1):
                myList=cx_db.get_list("select ip_addr, ip_name from dns_ips");
                for ip_addr, name in myList:
                        STORED_DATA[ip_addr]=name;
        if ReSet == 1:
                cx_db.exececute_command("truncate table dns_ips");
        if len(file_name) > 0:
                try:
                        with open(file_name) as myFile:
                                for line in myFile:
                                        process_line(line.strip(), PrintBoth, UpdateStored);
                except IOError as e:
                        print "Failed to open file", file_name
                        print e
        else:
                OK=1;
                while(OK != 0):
                        try:
                                line=raw_input();
                                process_line(line.strip(), PrintBoth, UpdateStored);
                        except (EOFError, KeyboardInterrupt):
                                OK=0;
        #
#       print "Stored Data:"
#       print STORED_DATA
        if (UpdateStored == 1):
                cx_db.exececute_command("commit")
        cx_db.close_db()
