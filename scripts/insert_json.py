# -*- coding: utf-8 -*-
import argparse
from config import config
#import csv
from datetime import datetime
import json
import psycopg2
import sys


def get_db_connection(filename='database.ini'):
    stderr('get_db_connection')
    # read database configuration
    params = config(filename)
    # connect to the PostgreSQL database
    conn = psycopg2.connect(**params)
    return conn


def get_last(conn):
    stderr('get_last')
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT last_num FROM last_result_num;")
        return cur.fetchone()[0]
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'get_last:  {error}')
        cur.execute(f"""
    CREATE TABLE last_result_num (
    last_num int
)
""")
        conn.commit()
        save_num(conn,0)
        return 0

def save_num(conn,num):
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM last_result_num;")
        conn.commit();
        cur.execute(f"INSERT INTO last_result_num VALUES ({num});")
        conn.commit()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'save_num: {error}')
        return False

def reset_num(conn):
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS last_result_num;")
        conn.commit()
        get_last(conn)
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'reset_num: {error}')
        return False


def create_table(conn,table,headers):
    stderr(f'create table {table}')
    try:
        cur = conn.cursor()
        cols = " text,".join(headers)
        cur.execute(f"DROP TABLE IF EXISTS {table};")
        cur.execute(f"""
    CREATE TABLE {table} (
    {cols} text
)
""")
        conn.commit()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'create_table: {error}')
        return False

def drop_table(conn,table):
    stderr(f'drop table {table}')
    try:
        cur = conn.cursor()
        cols = " text,".join(headers)
        cur.execute(f"DROP TABLE IF EXISTS {table};")
        conn.commit()
        return f'drop {table}: SUCCEED'
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'drop_table: {error}')
        return f'drop {table} ERROR: {error}'


def insert_content(conn,table,cols,rows):
    stderr('insert content')
    stderr(f'len: {len(rows)}')
    try:
        cur = conn.cursor()
        for row in rows:
            values = []
            for col in cols:
                values.append(row[col]['value'])
            all_values = "'" + "','".join(values) + "'"
            #stderr(f"INSERT INTO {table} VALUES ({all_values});")
            cur.execute(f"INSERT INTO {table} VALUES ({all_values});")
        conn.commit()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'insert_content: {error}')
        return False

# INSERT INTO products VALUES (1, 'Cheese', 9.99);

def upload_json(inputfile):
    stderr(f'upload: {inputfile}')
    args = arguments()
    configfile = args['config']
    res = json.load(open(inputfile))
    vars = res['head']['vars']
    conn = None
    try:
        conn = get_db_connection(configfile)
        teller = get_last(conn)
        table = f'result_{teller}'
        result = create_table(conn,table,vars)
        if result:
            result = insert_content(conn,table,vars,res['results']['bindings'])
        if result:
            save_num(conn,teller + 1)
            return f'{teller}'
        else:
            return 'FAILED'
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(f'upload_json ERROR: {error}')
        return 'FAILED'
    finally:
        if conn is not None:
            conn.close()
        #return 'CONNECTION CLOSED'

    
def stderr(text,nl='\n'):
    sys.stderr.write(f"{text}{nl}")


def arguments():
    ap = argparse.ArgumentParser(description="Insert json file into postgres db")
    ap.add_argument('-i', '--inputfile',
                    help="inputfile",
                    default= "result1.json")
    ap.add_argument('-c', '--config',
                    help="db config",
                    default= "database.ini")
    args = vars(ap.parse_args())
    return args


if __name__ == '__main__':

    stderr(datetime.today().strftime("start: %H:%M:%S"))
    args = arguments()
    inputfile = args['inputfile']
    configfile = args['config']
 
    res = json.load(open(inputfile))

    stderr(res.keys())
    vars = res['head']['vars']
    stderr(' TEXT,\n'.join(vars) + ' TEXT')

    conn = None
    try:
        # create a new cursor
        conn = get_db_connection(configfile)
        res = get_last(conn)
        stderr(res.__class__)
        stderr(res)
        exit(1)

        table = 'result1'
        create_table(conn,table,vars)
        insert_content(conn,table,vars,res['results']['bindings'])
    except (Exception, psycopg2.DatabaseError) as error:
        stderr(error)
    finally:
        if conn is not None:
            conn.close()

