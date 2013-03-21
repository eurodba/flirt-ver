__author__ = 'rostislav'
#/usr/bin/python
# -*- coding: utf8 -*-

import cx_Oracle
import argparse

DEBUG_FLAG = 'TRUE'

def __conn_init(request):
    # change needed - get as Param1 name of ORACLE_SID
    try: conn = cx_Oracle.connect("/@FLIRTDOH", mode=cx_Oracle.SYSDBA)
    except: print('CONNECT ERR')
    else:
        cursor = conn.cursor()
    try:
        if DEBUG_FLAG == 'TRUE':
            print (request)
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
    request = "select t1.type_ref, t1.name from " + i[0] + ".schemas t1 where t1.type_ref in ('FLIRT_AP', 'FLIRT_M', 'FLIRT_MON', 'FLIRT_TOOLS', '1OPAL', '1OPAL_DATA', 'REPORT_GEN') order by t1.name"
    for i in __conn_init(request):
        print('Founded ' + i[0] + ' schema ' + i[1])

        # check, if schema exists in db
        sql_text_t1 = "select 1 from dba_users t1 where t1.username = '" + i[1] + "'"
        for j in __conn_init(sql_text_t1):
            if j[0] != 1:
                print ('Schema ' + [i1] + ' (type ' + i[0] + ') does not exists in DB - misconfiguration in table SCHENAS')
                break

            # check, if VER table exists in schema
            sql_text_t1 = "select 1 from dba_tables t1 where t1.owner = '" + i[1] + "' and t1.table_name = 'VER'"
            for j in __conn_init(sql_text_t1):
                # select maximum installed pathlevel from each VER table in each schema
                request2 = "select t2.ts, t2.patchlevel from (select t1.ts, t1.patchlevel from " + i[1] + ".VER t1 where t1.ok = 'Y' order by t1.patchlevel desc) t2 where rownum = 1"
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
