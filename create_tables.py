import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# the fun to drop db tables by use drop_table_queries
def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("drop : " + query)
        cur.execute(query)
        conn.commit()

# the fun to create db tables by use create_table_queries
def create_tables(cur, conn):
    for query in create_table_queries:
        print("creat : " + query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("open the connection with DB")
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    

    print('Drop tables')
    drop_tables(cur, conn)
    
    print('Create tables')
    create_tables(cur, conn)

    conn.close()
    
    print('closed the connection with DB')


if __name__ == "__main__":
    main()