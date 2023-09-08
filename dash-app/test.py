import logging
import pandas as pd
import requests
from ewb_restapi_client import EWBRestapiClient

logging.basicConfig(level='DEBUG')
logger = logging.getLogger('Restapi')

restapi = requests.Session()

url = "http://localhost:82/queries/getCorpusMetadataFields/?corpus_collection=scopus"

resp = requests.get(
                url=url,
                timeout=None
            )