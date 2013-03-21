__author__ = 'rostislav'
#/usr/bin/python
# -*- coding: utf8 -*-

import cx_Oracle
import argparse

class Oracle(object):

    def connect(self, username, password, tns_alias):
        """ connect to the db """

        try:
            self.db = cx_Oracle.connect(username, password, tns_alias)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                print('Error: incorrect username / password')
            else:
                print('Database connection error: %s'.format(e))
            raise

        # If db connection succeeded create the cursor
        # we-re going to use.
        self.cursor = db.Cursor()

    def disconnect(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist we don't really care.
        """

        try:
            self.cursor.close()
            self.db.close()
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=None, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """

        try:
            self.cursor.execute(sql, bindvars)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                print('Table already exists')
            elif error.code == 1031:
                print("Insufficient privileges")
            elif error.code == 904:
                print("Invalid identifier")
        print(error.code)
        print(error.message)
        print(error.context)

        # Raise the exception.
        raise

    # Only commit if it-s necessary.
        if commit:
            self.db.commit()

def __conn_init(request):
    # change needed - get as Param1 name of ORACLE_SID
    try: conn = cx_Oracle.connect("/@FLIRTDOH", mode=cx_Oracle.SYSDBA)
    except: print('CONNECT ERR')
    else:
        cursor = conn.cursor()
    try:
        cursor.execute(request)
    except cx_Oracle.DatabaseError as exc:
        error = exc.args
        print ('Oracle-Error-Code: ', error.code)
        print ('Oracle-Error-Message: ', error.message)
    else: result = cursor.fetchall()
    cursor.close()
    conn.close()
    conn_res = 1
    return result

def __keys():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tablespace', required=False, dest='tablespace', help='Укажите имя табличного пространства.')
    args = parser.parse_args()
    return args

# main block
if __name__ == "__main__":

    try:
        oracle = Oracle.connect('sys', 'sysdoh', 'flirtdoh')

        # No commit as you don-t need to commit DDL.
        Oracle.execute('select * from dual')

    # Ensure that we always disconnect from the database to avoid
    # ORA-00018: Maximum number of sessions exceeded.
    finally:
        Oracle.disconnect()


if __keys().tablespace == None:
    print('Connection with Primary DB...')
    ### add try...except check for succesfull connection
    request = "select * from dual"
    for i in __conn_init(request):
        if i[0] == 'Y':
            print ('Connection succesfull')

    # this query returns all founded TOOLS schemas with filtering out TOOLS_G type schemas
    # strange Python behaviour - "escape '\' " generates error, but '\\' works ok...
    request = "select t2.* from (select username from dba_users where username like '%TOOLS%' order by username) t2 where t2.username not like '%\_TOOLS\_G\_%' escape '\\'"
    for i in __conn_init(request):
        print('Founded TOOLS schemas: %s' % i[0])

    print ('')
    # in the loop select each schema of necessary type, registered in table SCHEMAS
    request = "select t1.type_ref, t1.name from " + i[0] + ".schemas t1 where t1.type_ref in ('FLIRT_AP', 'FLIRT_M', 'FLIRT_MON', 'FLIRT_TOOLS', 'OPAL', 'OPAL_DATA', 'REPORT_GEN') order by t1.name"
    for i in __conn_init(request):
        print('Founded ' + i[0] + ' schema ' + i[1])

        # check, if schema exists in db
        sql_text_t1 = "select 1 from dba_users t1 where t1.username = '" + i[1] + "'"
        for j in __conn_init(sql_text_t1):
            if j[0] != 1:
                print ('Schema ' + [i1] + ' (type ' + i[0] + ') does not exists in DB - misconfiguration in table SCHENAS')
                break

            # check, if VER table exists in schema
            sql_text_t1 = "select 1 from dba_tables t1 where t1.owner = " + i[1] + " and t1.table_name = 'VER'"
            for j in __conn_init(sql_text_t1):
                # select maximum installed pathlevel from each VER table in each schema
                request2 = "select t2.ts, t2.patchlevel from (select t1.ts, t1.patchlevel from " + i[0] + ".ver t1 where t1.ok = 'Y' order by t1.patchlevel desc) t2 where rownum = 1"
                for j in __conn_init(request2):
                    print('Founded PATCHLEVEL: {0} {1}'.format(j[0], j[1]))

else:
    request = ("""select tablespace_name, round(total_bytes/1024/1024) as fMb
                  from dba_free_space_coalesced where tablespace_name='%s'""") % (__keys().tablespace)
    for i in __conn_init(request):
        print(i[1])

# Алексей Ларионов: Скрипт при запуске выводит имена табличных простанств в формате json (сделано для zabbix).
# Если необходимо получить информацию по размеру табличного пространства следует использовать ключ -t и после
# него указать имя табличного пространства.
