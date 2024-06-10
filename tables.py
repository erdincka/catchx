import socket
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

from nicegui import app 

from common import *

logger = logging.getLogger("tables")

# suppress ojai connection logging
logging.getLogger("mapr.ojai.storage.OJAIConnection").setLevel(logging.NOTSET)

def get_connection():
    """
    Returns an OJAIConnection object for configured cluster
    """

    connection_str = f"{app.storage.general['cluster']}:5678?auth=basic;user={app.storage.general['MAPR_USER']};password={app.storage.general['MAPR_PASS']};" \
            "ssl=true;" \
            "sslCA=/opt/mapr/conf/ssl_truststore.pem;" \
            f"sslTargetNameOverride={socket.getfqdn(app.storage.general['cluster'])}"
    
    return ConnectionFactory.get_connection(connection_str=connection_str)


def upsert_document(table_path: str, json_dict: dict):
    """
    Update or insert a document into the OJAI store (table)

    :param table_path str: full table path under the selected cluster
    :param json_dict dict: JSON serializable object to insert/update

    :return bool: result of operation

    """

    try:
        connection = get_connection()

        store = connection.get_or_create_store(table_path)

        new_document = connection.new_document(dictionary=json_dict)

        store.insert_or_replace(new_document)

        logger.debug("upsert for %s", json_dict["_id"])

    except Exception as error:
        logger.warning(error)
        return False

    # finally:        
    #     if connection: connection.close()

    return True


def upsert_documents(table_path: str, docs: list):
    """
    Update or insert a document into the OJAI store (table)

    :param table_path str: full table path under the selected cluster
    :param json_dict dict: JSON serializable object to insert/update

    :return bool: result of operation

    """

    try:
        connection = get_connection()

        store = connection.get_or_create_store(table_path)

        logger.info("Upserting %d documents from list", len(docs))
        
        store.insert_or_replace(doc_stream=docs)

    except Exception as error:
        logger.warning(error)
        return False

    # finally:        
    #     if connection: connection.close()

    return True


def find_document_by_id(table: str, docid: str):

    doc = None

    try:
        connection = get_connection()

        # Get a store and assign it as a DocumentStore object
        store = connection.get_store(table)

        # fetch the OJAI Document by its 'id' field
        doc = store.find_by_id(docid)

    except Exception as error:
        logger.warning(error)

    finally:
        # # close the OJAI connection
        # connection.close()
        return doc


def search_documents(table: str, selectClause: list, whereClause: dict):

    doc = None

    try:
        connection = get_connection()

        # Get a store and assign it as a DocumentStore object
        table = connection.get_store(table)

        # Create an OJAI query
        query = {"$select": selectClause,
                "$where": whereClause }

        logger.info("Query: %s", query)

        # options for find request
        options = {
            'ojai.mapr.query.result-as-document': True
            }

        # fetch OJAI Documents by query
        query_result = table.find(query, options=options)

        # Print OJAI Documents from document stream
        for doc in query_result:
            yield doc.as_dictionary()

    except Exception as error:
        logger.warning(error)

    finally:        
        # close the OJAI connection
        # connection.close()
        return doc


def get_documents(table_path: str, limit: int = FETCH_RECORD_NUM):
    """
    Read `limit` records from the table to peek data

    :param table str: full path for the JSON table

    :param limit int: Number of records to return, default is 10

    :returns list[doc]: list of documents as JSON objects

    """

    try:
        connection = get_connection()

        table_path = connection.get_store(table_path)

        # Create a query to get the last n records based on the timestamp field
        if limit is not None:
            query = connection.new_query() \
                .select('*') \
                .limit(limit) \
                .build()
        else:
            query = connection.new_query() \
                .select('*') \
                .build()

        # Run the query and return the results as list
        return [doc for doc in table_path.find(query)]

    except Exception as error:
        logger.warning("Failed to get document: %s", error)
        return []


# SSE-TODO: binary table create/read/write functions
# using Spark or any other means
# we may use REST API but I couldn't find rich REST functionality (read/write) for binary tables

def binary_table_upsert(tablepath: str, row: dict):
    """
    Create or return table, then push the row into the table

    :param tablepath str: full path to the table

    :param row dict: object to insert into the table

    :returns bool: result of op
    """

    not_implemented()


def binary_table_get_all(tablepath: str):
    """
    Returns all records from the binary table as ???

    :param tablepath str: full path to the table

    :returns ??: record as dict
    """

    not_implemented()

