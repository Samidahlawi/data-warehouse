import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


# load data to staging tables by copy_table_queries
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# insert data to tables by insert_table_queries
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('connect to DB')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # STAGING TABLES
    print('start Loading data ')
    load_staging_tables(cur, conn)
    
    print('start inserting data')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()