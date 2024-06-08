import logging
import mysql.connector

mysqldb_host = '10.1.1.31'
mysqldb_user = 'catchx'
mysqldb_password = 'catchx'


logger = logging.getLogger("mysqldb")


def get_db(dbname: str):
    """
    Get an existing database or create if it doesn't exist

    :param dbname str: name of the database to search or create

    :return `PooledMySQLConnection | MySQLConnectionAbstract`
    """

    try:
        mydb = mysql.connector.connect(
            host=mysqldb_host,
            user=mysqldb_user,
            passwd=mysqldb_password,
        )

        cursor = mydb.cursor()
        cursor.execute("SHOW DATABASES")
        # logger.info([x for x in cursor])
        existing_dbs = [x[0] for x in cursor]
        
        if dbname not in existing_dbs:
            logger.info("DB not found, creating: %s", dbname)
            cursor.execute(f"CREATE DATABASE {dbname}")

        return mysql.connector.connect(
            host=mysqldb_host,
            user=mysqldb_user,
            passwd=mysqldb_password,
            database=dbname
        )

    except Exception as error:
        logger.warning(error)


def query(dbname: str, query: str):
    """
    Run any SQL query against the database

    :param dbname str: Database name to connect/execute

    :param query str: SQL query to run on `dbname`

    :return result str|list: query results
    """

    connection = get_db(dbname=dbname)

    if connection is None: return # if failed to connect, no need to bother the rest

    results = None

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()

    except Exception as error:
        logger.warning(error)

    finally:
        cursor.close()
        logger.info("Mysql results for %s: %s", query, results)
        return results


def insert(dbname: str, tablename: str, records: list) -> bool:
    """
    Insert `recrods` into the `tablename` in `dbname` DB

    :param dbname str: mysql database name
    :param tablename str: table name in the database
    :param records list: records to append to `tablename`

    :return bool: Success or failure
    """

    print(records)

    return True