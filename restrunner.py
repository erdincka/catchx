import json
import logging
import os
import pathlib
import importlib_resources
import requests

from nicegui import app

from helpers import DEMO

logger = logging.getLogger()

AUTH_CREDENTIALS = (
    os.environ["MAPR_USER"],
    os.environ["MAPR_PASS"],
)


# Admin API GET call
def get(host: str, path: str, *args, **kwargs):
    """
    GET request to Admin REST API
    args and kwargs for requests.get.
    """

    REST_URL = f"https://{host}:8443{path}"

    try:
        response = requests.get(url=REST_URL, auth=AUTH_CREDENTIALS, verify=False, *args, **kwargs)
        logger.debug("GET RESPONSE: %s", response.text)
        response.raise_for_status()
        return response

    except Exception as error:
        # possibly not configured or incorrect configuration, just ignore it
        logger.debug("GET ERROR %s", error)
        return None


# Admin API POST call
def post(host: str, path: str, *args, **kwargs):
    """
    POST request to Admin REST API
    args and kwargs for requests.post.
    """

    REST_URL = f"https://{host}:8443{path}"
    logger.debug("POST URL: %s", REST_URL)
    try:
        response = requests.post(url=REST_URL, auth=AUTH_CREDENTIALS, verify=False, *args, **kwargs)
        logger.debug("POSTRESPONSE: %s", response.text)
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("POST ERROR %s", error)
        return None


# Save file to destination folder
def putfile(host: str, file: str, destfolder: str, *args, **kwargs):
    """
    PUT request to Admin REST API at /files.
    args and kwargs for requests.put.
    """

    basedir = importlib_resources.files("app")

    filepath = basedir.joinpath(file)
    filename = pathlib.Path(filepath).name
    destination = f"{DEMO['endpoints']['volume']}/{destfolder}/{filename}"

    REST_URL = f"https://{host}:8443/files{destination}"

    try:
        with open(filepath, "rb") as f:
            response = requests.put(
                url=REST_URL,
                auth=AUTH_CREDENTIALS,
                verify=False,
                data=f,
                timeout=5,
                *args,
                **kwargs
            )
            response.raise_for_status()
            return response

    except Exception as error:
        logger.warning("PUTFILE ERROR %s", error)
        return None


# Get file from destination
def getfile(host: str, filepath: str, *args, **kwargs):
    """
    GET request to Admin REST API at /files.
    args and kwargs for requests.get.
    """

    REST_URL = f"https://{host}:8443/files{filepath}"

    try:
        response = requests.get(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            timeout=5,
            *args,
            **kwargs
        )
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("GETFILE ERROR %s", error)
        return None


# JSON DB (OJAI) call
def dagget(host: str, path: str, *args, **kwargs):
    """
    GET request to Data Access Gateway API
    args and kwargs for request.get.
    """

    REST_URL = f"https://{host}:8243{path}"
    logger.debug("DAG GET %s", REST_URL)
    try:
        response = requests.get(url=REST_URL, auth=AUTH_CREDENTIALS, verify=False, *args, **kwargs)
        logger.debug("DAG GET RESPONSE: %s", response)
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("DAG GET ERROR %s", error)
        return None


# JSON DB (OJAI) call
def dagput(host: str, path: str, data=None, *args, **kwargs):
    """
    PUT request to Data Access Gateway API
    args and kwargs for requests.put.
    """

    REST_URL = f"https://{host}:8243{path}"

    logger.debug("DAG PUT: %s", REST_URL)
    try:
        response = requests.put(url=REST_URL, auth=AUTH_CREDENTIALS, verify=False, data=data, *args, **kwargs)
        logger.debug("DAG PUT RESPONSE: %s", response.text if response.text != "" else "OK")
        response.raise_for_status()
        return response

    except requests.exceptions.HTTPError as httperror:
        if httperror.response.status_code != 409:
            logger.warning("DAG PUT HTTP ERROR %s", httperror.response.text)

    except Exception as error:
        logger.warning("DAG PUT ERROR %s", error)
        return None


def dagpost(host: str, path: str, json_obj=None, *args, **kwargs):
    """
    POST request to Data Access Gateway API
    args and kwargs for requests.post.
    """

    REST_URL = f"https://{host}:8243{path}"

    logger.debug("DAGPOSTDATA: %s", type(json_obj))

    try:
        response = requests.post(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            json=json_obj,
            *args,
            **kwargs,
            # headers={"Content-Type: application/json"},
        )
        logger.debug("DAG POST RESPONSE: %s", response)
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("DAG POST ERROR %s", error)
        return None


# Kafka REST calls
def kafkaget(host: str, path: str, *args, **kwargs):
    """
    GET request to Kafka REST API
    args and kwargs for requests.get.
    """

    REST_URL = f"https://{host}:8082{path}"

    logger.debug("KAFKA GET %s", REST_URL)
    try:
        response = requests.get(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            headers={
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept": "application/vnd.kafka.json.v2+json",
            },
            *args,
            **kwargs,
        )
        logger.debug("KAFKA GET RESPONSE: %s", response.text)
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("KAFKA GET ERROR %s", error)
        return None


def kafkaput(host: str, path: str, data=None, *args, **kwargs):
    """
    PUT request to Kafka REST API
    args and kwargs for requests.put.
    """

    REST_URL = f"https://{host}:8082{path}"

    logger.debug("KAFKA PUT: %s", REST_URL)
    try:
        response = requests.put(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            data=json.dumps({"records": [{"value": data}]}),
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            *args,
            **kwargs,
        )
        logger.debug("KAFKA PUT RESPONSE: %s", response)
        response.raise_for_status()
        return response

    except Exception as error:
        logger.warning("KAFKA PUT ERROR %s", error)
        return None


def kafkapost(host: str, path: str, data=None, *args, **kwargs):
    """
    POST request to Kafka REST API
    args and kwargs for requests.post.
    """

    REST_URL = f"https://{host}:8082{path}"

    logger.debug("KAFKA POST DATA: %s", data)

    try:
        response = requests.post(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            data=json.dumps(data),
            headers={ "Content-Type": "application/vnd.kafka.json.v2+json" },
            *args,
            **kwargs,
        )
        logger.debug("KAFKA POST RESPONSE: %s", response.text)
        response.raise_for_status()
        return response

    except requests.exceptions.HTTPError as error:
        # 409 Existing Record
        if error.response.status_code != 409:
            raise error
    except Exception as error:
        # print(error.text)
        logger.warning("KAFKA POST ERROR %s", error)
        return None


def kafkadelete(host: str, path: str, *args, **kwargs):
    """
    DELETE request to Kafka REST API
    args and kwargs for requests.delete.
    """

    REST_URL = f"https://{host}:8082{path}"

    try:
        response = requests.delete(
            url=REST_URL,
            auth=AUTH_CREDENTIALS,
            verify=False,
            headers={"Content-Type": "application/vnd.kafka.v2+json"},
            *args,
            **kwargs,
        )
        logger.debug("KAFKA DELETE REQUEST: %s", response.request)
        logger.debug("KAFKA DELETE RESPONSE FOR %s: %s", REST_URL, response.text)
        response.raise_for_status()
        return response

    except requests.exceptions.HTTPError as httperror:
        if httperror.response.status_code != 404:
            logger.warning("KAFKA DELETE HTTP ERROR %s", httperror.response.text)

    except Exception as error:
        # print(f"KAFKA DELETE ERROR {error}")
        logger.warning("KAFKA DELETE ERROR %s", error)
        return None

