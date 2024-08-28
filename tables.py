import socket
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from nicegui import app

from common import *
import timeit

logger = logging.getLogger("tables")

# suppress ojai connection logging
logging.getLogger("mapr.ojai.storage.OJAIConnection").setLevel(logging.NOTSET)

ojaiconnection = None

def get_connection():
    """
    Returns an OJAIConnection object for configured cluster
    """

    tick = timeit.default_timer()

    # Use singleton
    global ojaiconnection
    if ojaiconnection is not None: return ojaiconnection

    connection_str = f"{app.storage.user['MAPR_HOST']}:5678?auth=basic;user={app.storage.user['MAPR_USER']};password={app.storage.user['MAPR_PASS']};" \
            "ssl=true;" \
            "sslCA=/opt/mapr/conf/ssl_truststore.pem;" \
            f"sslTargetNameOverride={socket.getfqdn(app.storage.user['MAPR_HOST'])}"

    ojaiconnection = ConnectionFactory.get_connection(connection_str=connection_str)
    logger.info("Got new maprdb connection using OJAI in %f sec", timeit.default_timer() - tick)

    return ojaiconnection


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

        # logger.debug("upsert new doc: %s", new_document)

        store.insert_or_replace(new_document)

        logger.info("doc upserted %s", json_dict["_id"])

    except Exception as error:
        logger.warning(error)
        return False

    # finally:
    #     if connection: connection.close()

    return True


async def upsert_documents(table_path: str, docs: list):
    """
    Update or insert a document into the OJAI store (table)

    :param table_path str: full table path under the selected cluster
    :param docs list[dict]: list of JSON serializable objects to insert/update

    :return bool: result of operation

    """

    try:
        connection = get_connection()

        store = connection.get_or_create_store(table_path)

        logger.info("Upserting %d documents from list", len(docs))

        tick = timeit.default_timer()

        store.insert_or_replace(doc_stream=docs)

        logger.debug("doc insert took %fs", timeit.default_timer() - tick)

    except Exception as error:
        logger.warning(error)
        return False

    finally:
        logger.info("%d documents processed", len(docs))
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


async def get_documents(table_path: str, limit: int = FETCH_RECORD_NUM):
    """
    Read `limit` records from the table to peek data

    :param table str: full path for the JSON table

    :param limit int: Number of records to return, default is 10, if None, returns all documents

    :returns list[doc]: list of documents as JSON objects

    """

    try:
        # logger.debug("Requesting docs from %s", table_path)

        # tick = timeit.default_timer()
        connection = get_connection()
        # logger.debug("Got ojai connection in %f sec", timeit.default_timer() - tick)

        # tick = timeit.default_timer()
        table = connection.get_store(table_path)
        # logger.debug("Got store in %f sec", timeit.default_timer() - tick)

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

        tick = timeit.default_timer()
        # Run the query and return the results as list
        logger.debug("Returned %d docs in %s, took %f sec", len([doc for doc in table.find(query)]), table_path, timeit.default_timer() - tick)
        # tick = timeit.default_timer()
        results = table.find(query)
        # logger.debug("TEST time: %f", timeit.default_timer() - tick)
        return [doc for doc in results]

    except Exception as error:
        logger.warning("Failed to get document: %s", error)
        return []


# SSE-TODO: binary table create/read/write functions
# using Spark or any other means
# we may use REST API but I couldn't find rich REST functionality (read/write) for binary tables

def binary_table_upsert(table_path: str, row: dict):
    """
    Create or return table, then push the row into the table

    :param table_path str: full path to the table

    :param row dict: object to insert into the table

    :returns bool: result of op
    """

    not_implemented()


def binary_table_get_all(table_path: str):
    """
    Returns all records from the binary table as ???

    :param table_path str: full path to the table

    :returns ??: record as dict
    """

    not_implemented()

async def delta_table_upsert(table_path: str, records: pd.DataFrame):
    """
    Write list of dicts into Delta Lake table
    """

    try:
        table_uri = f"{MOUNT_PATH}/{get_cluster_name()}{table_path}"

        df = pd.DataFrame().from_records(records)

        if not os.path.exists(table_uri):
            write_deltalake(table_or_uri=table_uri, data=df, mode="append", schema_mode="merge")
            logger.debug("Created new Delta table in %s", table_uri)
        else:
            dt = DeltaTable(table_uri=table_uri)

            merge_result = (
                dt.merge(
                    source=df,
                    predicate="s._id = t._id",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

            logger.debug(merge_result)

    except Exception as error:
        logger.error("Failed to write: %s", table_path)
        logger.error(error)
        return False

    return True


async def delta_table_get(table_path, query: str = None):
    """
    Returns all records from the binary table as DataFrame
    """

    fullpath = f"{MOUNT_PATH}/{get_cluster_name()}{table_path}"

    if not os.path.exists(fullpath):
        logger.warning("%s not created yet", fullpath)
        return pd.DataFrame()

    try:
        if query is None or query == "":
            return DeltaTable(fullpath).to_pandas()
        else:
            fraud = "fraud" # for query string
            return DeltaTable(fullpath).to_pandas().query(query)

    except Exception as error:
        logger.error("Failed to read: %s", fullpath)
        logger.error(error)
        return pd.DataFrame()
