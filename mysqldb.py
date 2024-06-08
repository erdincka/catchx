import logging
import mysql.connector

mysqldb_host = '10.1.1.31'
mysqldb_user = 'catchx'
mysqldb_password = 'catchx'


logger = logging.getLogger("mysqldb")


def getdatabase(dbname: str):
    """
    Get the connection to an existing database or create if it doesn't exist and return the connection

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


def ensuretable(dbname: str, tablename: str):
    """
    Create the `tablename` in `dbname` DB

    :param dbname str: mysql database name
    :param tablename str: table name in the database

    :return bool: Success or failure
    """

    # create table if not exist

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {tablename} (
            _id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            address TEXT,
            account_number VARCHAR(255),
            score FLOAT,
            transactions_sent JSON,
            transactions_received JSON
        );
        """
    
    if dbquery(dbname=dbname, query=create_table_sql) is None:
        return False

    return True


def droptable(dbname: str, tablename: str):
    """
    Drop the `tablename` in `dbname` DB

    :param dbname str: mysql database name
    :param tablename str: table name in the database

    :return bool: Success or failure
    """

    drop_table_sql = f"DROP TABLE {tablename}"
    if dbquery(dbname=dbname, query=drop_table_sql) is None:
        return False

    return True


def dbquery(dbname: str, query: str):
    """
    Run SQL query against the database

    :param dbname str: Database name to connect/execute

    :param query str: SQL query to run on `dbname`

    :return result str|list: query results
    """

    connection = getdatabase(dbname=dbname)

    if connection is None: return # if failed to connect, no need to bother the rest

    results = None

    cursor = connection.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()

    except Exception as error:
        logger.warning("Query '%s' failed: %s", query, error)

    finally:
        cursor.close()
        logger.info("Query results for '%s': %s", query, results)
        return results


def insert(dbname: str, tablename: str, records: list) -> bool:
    """
    Insert `records` into the `tablename` in `dbname` DB

    :param dbname str: mysql database name
    :param tablename str: table name in the database
    :param records list: records to append to `tablename`

    :return bool: Success or failure
    """

    if not ensuretable(dbname=dbname, tablename=tablename): return False

    # Insert data into the table
    insert_query = f"""
    INSERT INTO {tablename} (_id, name, address, account_number, score, transactions_sent, transactions_received)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    connection = getdatabase(dbname=dbname)

    if connection is None: return # if failed to connect, no need to bother the rest

    results = None

    cursor = connection.cursor()

    try:
        print([tuple(row) for row in records])
        # cursor.executemany(insert_query, [tuple(row) for row in records])
        # connection.commit()
        logger.info("Insert %d records", cursor.rowcount)

    except Exception as error:
        logger.warning("Query '%s' failed: %s", insert_query, error)

    finally:
        cursor.close()
        logger.info("Query results for '%s': %s", insert_query, results)


