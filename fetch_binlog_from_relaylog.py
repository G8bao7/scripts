#!/usr/bin/env python
# -*- coding:utf-8 -*- 
'''
Created on 2016年12月19日

@author: guoqi
'''
import os, sys, signal
import argparse
import logging

(script_path, script_file_name) = os.path.split(os.path.abspath(sys.argv[0]))
(script_name, script_format) = script_file_name.split(".")
BIN_MYSQLBINLOG = None
def sigint_handler(signum, frame):
    logging.warning("receive a signal %s" % str(signum))
    exit(1)

def run_os_cmd(cmd):
    logging.debug("run_os_cmd> %s" % (cmd))
    res = os.popen(cmd).read().rstrip("\n")
    return res

def check_env():
    global BIN_MYSQLBINLOG
    # mysqlbinlog
    cmd_mysqlbinlog = "which mysqlbinlog 2>/dev/null"
    res = run_os_cmd(cmd_mysqlbinlog)
    logging.info(res)
    if not cmp("", res):
        logging.error("mysqlbinlog not exists")
        return False
    else:
	BIN_MYSQLBINLOG = res   
        
    return True

def get_relaylog_files(relay_log_index_file):
    if not os.path.exists(relay_log_index_file):
        logging.error("%s not exists" % (relay_log_index_file))
        exit(1)
    logging.warn("get_relaylog_files:%s" % (relay_log_index_file))
    
    cmd_relay_logs = "cat %s" % (relay_log_index_file)
    relay_log_files = run_os_cmd(cmd_relay_logs).split("\n")
    return relay_log_files
    
def is_begin_master_binlog(relay_log_file, master_binlog_file):
    logging.debug("is_begin_master_binlog:%s,%s" % (relay_log_file, master_binlog_file))
    cmd_check = "mysqlbinlog --stop-position=300 %s 2>/dev/null |egrep 'server\s+id\s+[0-9]+\s+end_log_pos 0\s.*\sRotate to %s\s+pos: 4'" % (relay_log_file, master_binlog_file)
    #161201 19:31:42 server id 1  end_log_pos 0     Rotate to mysql-bin.000617  pos: 4
    res = run_os_cmd(cmd_check)
    logging.debug("is_begin_master_binlog:%s" % (res))
    if len(res) <= 0:
        return False
    return True

def get_relaylog_position(relay_log_file, master_binlog_pos):
    logging.debug("get_relaylog_position:%s,%s" % (relay_log_file, master_binlog_pos))
    cmd_binlog = "mysqlbinlog %s 2>/dev/null |egrep -B 1 'server\s+id\s+[0-9]+\s+end_log_pos\s+%s' 2>/dev/null| head -n 1 |awk '{print $3}'" % (relay_log_file, master_binlog_pos)
    res = run_os_cmd(cmd_binlog)
    logging.debug("get_relaylog_position:%s" % (res))
    # at 1312
    #161219 11:48:07 server id 1  end_log_pos 1180     Xid = 821622795
    if len(res) <= 0:
        return None
    pos = int(res)
    return pos

def get_fetch_cmds(relay_log_files, start_binlog_file, start_binlog_pos, stop_binlog_file=None, stop_binlog_pos=4):
    cmds = []
    f_start_relay_log = None
    f_stop_relay_log = None
    for relay_log_file in relay_log_files:
	logging.info("check relay_log :%s" % (relay_log_file))
        if f_start_relay_log is not None:
	    is_stop = False
	    if stop_binlog_file is None:
		cmd_fetch = "%s %s" % (BIN_MYSQLBINLOG, relay_log_file)
	    else:
		if is_begin_master_binlog(relay_log_file, stop_binlog_file):
		    relay_pos = get_relaylog_position(relay_log_file, stop_binlog_pos)
		    if relay_pos is None:
			logging.error("%s not exists in %s" % (stop_binlog_pos, relay_log_file))
			exit(1)
		    is_stop = True
		    cmd_fetch = "%s --stop-position=%s %s" % (BIN_MYSQLBINLOG, relay_pos, relay_log_file)
		else:
		    cmd_fetch = "%s %s" % (BIN_MYSQLBINLOG, relay_log_file)
	    cmds.append(cmd_fetch)
	    if is_stop:
		break
        elif is_begin_master_binlog(relay_log_file, start_binlog_file):
	    f_start_relay_log = relay_log_file
	    relay_pos = get_relaylog_position(relay_log_file, start_binlog_pos)
	    if relay_pos is None:
		logging.error("%s not exists in %s" % (start_binlog_pos, relay_log_file))
		exit(1)
	    cmd_fetch = "%s --start-position=%s %s" % (BIN_MYSQLBINLOG, relay_pos, relay_log_file)
	    cmds.append(cmd_fetch)

    return cmds

def sub_manual(args):
    relaylog_files = get_relaylog_files(args.relay_log_index)
    return relaylog_files

def sub_auto(args):
    import MySQLdb
    
    def create_connect(user, passwd, socket, db="mysql"):
        conn = None
        try:
	    conn = MySQLdb.connect(user=user,passwd=passwd,unix_socket=socket,db=db,connect_timeout=2,charset='utf8')
            cur = conn.cursor()
            cur.close()
        except MySQLdb.Error, e:
            conn = None
            logging.error(e)
        finally:
            pass
        return conn
        
    def do_query(conn, sqlstr):
        logging.debug(sqlstr)
        try:
            cur = conn.cursor()
            cur.execute(sqlstr)
            lines = cur.fetchall()
            cur.close()
            return lines
        except MySQLdb.Error,e:
            logging.error(e)
        finally:
            pass
     
        return None
    
    conn = create_connect(args.user, args.passwd, args.socket)
    if conn is None:
	exit(1)
    lines = do_query(conn, "SHOW VARIABLES LIKE 'relay_log_index';")
    relay_log_index = lines[0][1]
    conn.close()
    
    #
    relaylog_files = get_relaylog_files(relay_log_index)
    return relaylog_files

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")

    #
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    
    #
    descs = []
    descs.append("")
    
    parser = argparse.ArgumentParser(description="\n".join(descs))
    subparsers = parser.add_subparsers()

    parents_parser = argparse.ArgumentParser(add_help=False)
    parents_parser.add_argument("-D","--debug", dest="debug", action='store_true', help= "")
    default_export_file = "%s/fetch_binlog.sql" % (script_path)
    parents_parser.add_argument("-E","--export-file", dest="export_file", default=default_export_file, help= "default '%s'" % (default_export_file))
    parents_parser.add_argument("--start-binlog-file", dest="start_binlog_file", required=True, help= "")
    parents_parser.add_argument("--start-binlog-pos", dest="start_binlog_pos", default=4, type=int, help= "default '4'")
    parents_parser.add_argument("--stop-binlog-file", dest="stop_binlog_file", help= "")
    parents_parser.add_argument("--stop-binlog-pos", dest="stop_binlog_pos", default=4, type=int, help= "default '4'")
    
    # manual
    manual_parser = subparsers.add_parser('manual', parents=[parents_parser], help='set relay_log_index')
    manual_parser.add_argument("-r","--relay-log-index", dest="relay_log_index", required=True, help= "")
    manual_parser.set_defaults(func=sub_manual)
    
    # auto
    auto_parser = subparsers.add_parser('auto', parents=[parents_parser], help='get relay_log_index by connect mysql')
    auto_parser.add_argument("-u","--slave-user", dest="user", required=True, help = "")
    auto_parser.add_argument("-p","--slave-passwd", dest="passwd", required=True, help = "")
    auto_parser.add_argument("-S","--slave-socket", dest="socket", default=None, help = "")
    auto_parser.set_defaults(func=sub_auto)
    
    args = parser.parse_args()
    
    if args.debug:
        log_level = logging.DEBUG
        log_format = '%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s'
        log_datefmt = '%Y-%m-%d %H:%M:%S'
    else:        
        log_level = logging.INFO
        log_format = '%(asctime)s [%(levelname)s] %(message)s'
        log_datefmt = '%d %H:%M:%S'

    logging.basicConfig(level=log_level,  format=log_format, datefmt=log_datefmt)
    
    if not check_env():
        exit(1)
    relay_log_files = args.func(args)
    cmd_fetchs = get_fetch_cmds(relay_log_files, args.start_binlog_file, args.start_binlog_pos, args.stop_binlog_file, args.stop_binlog_pos)

    export_file = args.export_file
    cmd_exports = []
    cmd_exports.append("echo '' > %s" % (export_file))
    for cmd_fetch in cmd_fetchs:
	logging.debug(cmd_fetch)
	cmd_export = "%s >> %s" % (cmd_fetch, export_file)
	cmd_exports.append(cmd_export)

    fetch_shell = "%s/fetch_cmd.sh" % (script_path)
    logging.warning("export shell:%s" % (fetch_shell))
    with open(fetch_shell,'w') as fh:
	for cmd_export in cmd_exports:
	    logging.debug(cmd_export)
	    fh.write("%s\n" % (cmd_export))
    logging.info("\n%s" % (run_os_cmd("cat %s" % (fetch_shell))))

    logging.warning("export file:%s" % (export_file))
    for cmd_export in cmd_exports:
        os.system(cmd_export)

    logging.warning("Finish")
    exit(0)
