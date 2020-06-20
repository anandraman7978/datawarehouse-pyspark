import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops all the staging and main tables
            Parameters:
                    cur   -- Connection String
                    conn  -- instance of connection
                    
            Returns:
                    None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    creates all the staging and main tables
            Parameters:
                    cur   -- Connection String
                    conn  -- instance of connection
                    
            Returns:
                    None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main Function connects to the database sparkifydb. calls function to create staging and main tables.

            Parameters:
                    None
            Returns:
                    None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()