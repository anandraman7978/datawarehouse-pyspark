import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    '''
    Loads the artists and songs tables from the s3 data
            Parameters:
                    cur   -- Connection String
                    conn  -- instance of connection
                    
            Returns:
                    None
    '''
    for query in copy_table_queries: 
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Loads the data from staging tables to main dim and fact tables
            Parameter
                    cur   -- Connection String
                    conn  -- instance of connection
                    
            Returns:
                    None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main Function connects to the database sparkifydb. calls function to load data into staging and main tables.

            Parameters:
                    None
            Returns:
                    None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()