import csv
import json
import psycopg2
from config import config

def create_table(cur,headers):
    print(headers)
    cols = " text,".join(headers.split(";"))
    cur.execute("DROP TABLE users;")
    cur.execute(f"""
    CREATE TABLE users(
    {cols} text
)
""")


if __name__ == '__main__':

    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        with open('user_accounts.csv', 'r') as f:
#            reader = csv.reader(f)
#            next(reader) # Skip the header row.
             create_table(cur, next(f))
#             next(f) # Skip the header row.
             cur.copy_from(f, 'users', sep=';')
#            for row in reader:
#                print(row)
#                cur.execute(
#                "INSERT INTO users VALUES (%s, %s, %s, %s)",
#                row
#            )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

