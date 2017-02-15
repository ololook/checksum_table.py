#encoding: utf-8
__author__ ='zhangyuanxiang'
from multiprocessing import Pool
import time
import sys
import MySQLdb
import MySQLdb.cursors
from optparse import OptionParser
import cx_Oracle
import hashlib
import os
from sys import argv
reload(sys )
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
error=[]
presult=[]

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",description="""Table CheckSUM""")

    parser.add_option("-H", "--f_dsn",
                      dest="f",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                      )

    parser.add_option("-L", "--t_dsn",
                      dest="t",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                     )
    parser.add_option("-S", "--f_sid",
                      dest="f_sid",
                      default="orcl",
                      metavar="orcl"
                     )
    parser.add_option("-d", "--t_sid",
                      dest="t_sid",
                      default="orcl",
                      metavar="orcl"
                     )
    parser.add_option("-m", "--type",
                      dest="m",
                      default="m2m",
                      metavar="o2o o2m m2m m2o"
                     )
    (options, args) = parser.parse_args()

    return options

def source_client():
      options = get_cli_options()
      h=options.f
      f_h=h.strip().split(':')[0]
      f_p=h.strip().split(':')[1]
      f_t=options.m
      f_s=options.f_sid
      
      if f_t.lower()=='m2o' or f_t.lower()=='m2m' :
        try:
           con=MySQLdb.connect(host =f_h,user ='username',passwd ='passwd',port =int(f_p),charset='utf8',cursorclass = MySQLdb.cursors.SSCursor)
        except Exception , e:
          print "source_client ",e
      elif  f_t.lower()=='o2m' or f_t.lower()=='o2o' :
        try:
           dsn_tns =cx_Oracle.makedsn(f_h,f_p,f_s) 
           con = cx_Oracle.connect('username','passwd',dsn_tns) 
        except cx_Oracle.DatabaseError as e:
           print "source_client ",e
      else:
         print "input parameter error"
      return con
 
def source_table():
       options = get_cli_options()
       h=options.f
       f_d=h.strip().split(':')[2]
       f_t=h.strip().split(':')[3]
       f_tt=options.m
       cur = source_client().cursor()
       sql_1="select * from %s.%s where 0=1" %(f_d,f_t)
       row=""
       try:
          cur.execute(sql_1)
       except Exception , e:
          print "source_table ",e
       if f_tt.lower()=='m2o' or f_tt.lower()=='m2m' :
          for i in range(0, len(cur.description)):
              if i==0:
                  row =row+cur.description[i][0]
              else:
                  row +=','+cur.description[i][0]
                  i=i+1
          cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"%s.%s  " %(f_d,f_t)
       elif f_tt.lower()=='o2m' or f_tt.lower()=='o2o' :
            for i in range(0, len(cur.description)):
              if i==0:
                  row =row+cur.description[i][0]+"||'||'||"
              elif i>0 and i<len(cur.description)-1:
                  row +=cur.description[i][0]+"||'||'||"
                  i=i+1 
              else:
                  row +=cur.description[i][0]
            cols="select "+row+" "+"from "+" %s.%s " %(f_d,f_t)
       return cols


def destin_client():
      options = get_cli_options()
      h=options.t
      t_h=h.strip().split(':')[0]
      t_p=h.strip().split(':')[1]
      t_tt=options.m
      t_s=options.t_sid
      if t_tt.lower()=='o2m' or t_tt.lower()=='m2m' :
        try:
           con=MySQLdb.connect(host =t_h,user ='username',passwd ='passwd',port =int(t_p),charset='utf8',cursorclass = MySQLdb.cursors.SSCursor)
        except Exception , e:
          print "source_client ",e
      elif  t_tt.lower()=='o2o' or t_tt.lower()=='m2o' :
        try:
           dsn_tns =cx_Oracle.makedsn(t_h,t_p,t_s)
           con = cx_Oracle.connect('username','passwd',dsn_tns)
        except cx_Oracle.DatabaseError as e:
           print "source_client ",e
      else:
         print "input parameter error"
      return con
def destin_table():
       options = get_cli_options()
       h=options.t
       t_d=h.strip().split(':')[2]
       t_t=h.strip().split(':')[3]
       t_tt=options.m
       cur = destin_client().cursor()
       sql_1="select * from %s.%s where 0=1" %(t_d,t_t)
       row=""
       try:
          cur.execute(sql_1)
       except Exception , e:
          print "source_table ",e
       if t_tt.lower()=='m2m' or t_tt.lower()=='o2m' :
          for i in range(0, len(cur.description)):
              if i==0:
                  row =row+cur.description[i][0]
              else:
                  row +=','+cur.description[i][0]
                  i=i+1
              cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"%s.%s  " %(t_d,t_t)
       elif t_tt.lower()=='o2o' or t_tt.lower()=='m2o' :
            for i in range(0, len(cur.description)):
              if i==0:
                  row =row+cur.description[i][0]+"||'||'||"
              elif i>0 and i<len(cur.description)-1:
                  row +=cur.description[i][0]+"||'||'||"
                  i=i+1
              else:
                  row +=cur.description[i][0]
            cols="select "+row+" "+"from "+" %s.%s " %(t_d,t_t)
       return cols

def compare_row(source_1,destin_1):
    
   for s in source_1:
       if s not in destin_1 and s not in error:
          error.append(s)
       else:
          continue
   for l in destin_1:
       if l not in source_1:
          presult.append(l)
   for x in error:
       if x in destin_1 or x in presult:
          error.remove(x)   
       else:
          pass
     
def compare_table():
    batch=500
    lag=0
    source_cur=source_client().cursor()
    destin_cur=destin_client().cursor()
    source_sql=source_table()
    destin_sql=destin_table()
    source_cur.execute(source_sql)
    destin_cur.execute(destin_sql)
    source_result = source_cur.fetchmany(batch) 
    while source_result: 
          destin_result = destin_cur.fetchmany(batch)
          source_md5=hashlib.md5(str(source_result)).hexdigest()
          destin_md5=hashlib.md5(str(destin_result)).hexdigest()
          if source_md5 !=destin_md5:
             compare_row(source_result,destin_result) 
          else:
             pass
          source_result=[]
          destin_result=[]
          source_result = source_cur.fetchmany(batch)
    for i in error:
        var_col=[]
        for var in list(i)[0].split("||"):
           var_col.append(var)
        print'||'.join(var_col)
if __name__ == '__main__':
    compare_table()