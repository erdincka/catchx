import logging
import os
from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

logger = logging.getLogger()

def get_cert_domain():
    with open("/opt/mapr/conf/ssl_truststore.pem", 'r') as f:
        for line in f:
            if "subject=C =" in line:
                return line.split(" CN = *.")[1]

    return ""
        
def get_connection(host: str):
    ### FIX: bruteforce cert validation
    domain = get_cert_domain()

    # Create a connection to data access server
    connection_str = f"{host}:5678?auth=basic;user={os.environ['MAPR_USER']};password={os.environ['MAPR_PASS']};" \
            "ssl=true;" \
            "sslCA=/opt/mapr/conf/ssl_truststore.pem;" \
            f"sslTargetNameOverride=client.{domain}"
    
    return ConnectionFactory.get_connection(connection_str=connection_str)

def upsert_document(host: str, table: str, json_dict: dict):
    try:
        connection = get_connection(host=host)

        store = connection.get_or_create_store(table)

        # Create new document from json_document
        new_document = connection.new_document(dictionary=json_dict)

        store.insert_or_replace(new_document)

    except Exception as error:
        logger.warning(error)
        return False

    finally:        
        # close the OJAI connection
        connection.close()

    return True

def find_document_by_id(host: str, table: str, docid: str):

    doc = None

    try:
        connection = get_connection(host)

        # Get a store and assign it as a DocumentStore object
        store = connection.get_store(table)

        # fetch the OJAI Document by its '_id' field
        doc = store.find_by_id(docid)

    except Exception as error:
        logger.warning(error)

    finally:        
        # close the OJAI connection
        connection.close()
        return doc


def search_documents(host: str, table: str, whereClause: dict):

    doc = None

    try:
        connection = get_connection(host)

        # Get a store and assign it as a DocumentStore object
        store = connection.get_store(table)

        # Create an OJAI query
        query = {"$select": ["_id",
                            "sender",
                            "receiver"],
                "$where": whereClause }

        # options for find request
        options = {
            'ojai.mapr.query.result-as-document': True
            }

        # fetch OJAI Documents by query
        query_result = store.find(query, options=options)

        # Print OJAI Documents from document stream
        for doc in query_result:
            yield doc.as_dictionary()

    except Exception as error:
        logger.warning(error)

    finally:        
        # close the OJAI connection
        connection.close()
        return doc
