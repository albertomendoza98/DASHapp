"""
This  module provides 2 classes to handle Inferencer API responses and requests.

The InferencerResponse class handles Inferencer API response and errors, while the EWBInferencerClient class handles requests to the Inferencer API.

Author: Lorena Calvo-Bartolomé
Date: 21/05/2023
"""

import logging
import os

import requests


class RestAPIResponse(object):
    """
    A class to handle Rest API response and errors.
    """

    def __init__(self,
                 resp: requests.Response,
                 logger: logging.Logger) -> None:

        # Get status code
        self.status_code = resp.status_code

        # Get JSON object of the result
        self.results = resp.json()

        if self.status_code == 200:
            logger.info(f"-- -- RestAPI request acknowledged")
        else:
            logger.info(
                f"-- -- RestAPI request generated an error: {self.results['error']}")
        return


class EWBRestapiClient(object):
    """
    A class to handle EWB Rest API requests.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        Parameters
        ----------
        logger : logging.Logger
            The logger object to log messages and errors.
        """

        # Get the RestAPI URL from the environment variables
        self.restapi_url = os.environ.get('RESTAPI_URL')

        # Initialize requests session and logger
        self.restapi = requests.Session()

        if logger:
            self.logger = logger
        else:
            import logging
            logging.basicConfig(level='DEBUG')
            self.logger = logging.getLogger('Restapi')
        
        return

    def _do_request(self,
                    type: str,
                    url: str,
                    timeout: int = 10,
                    **params) -> RestAPIResponse:
        """Sends a request to the Rest API and returns an object of the RestAPIResponse class.

        Parameters
        ----------
        type : str
            The type of the request.
        url : str
            The URL of the Rest API.
        timeout : int, optional
            The timeout of the request in seconds, by default 10.
        **params: dict
            The parameters of the request.

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """

        # Send request
        if type == "get":
            resp = requests.get(
                url=url,
                timeout=timeout,
                **params
            )
        elif type == "post":
            resp = requests.post(
                url=url,
                timeout=timeout,
                **params
            )
        else:
            self.logger.error(f"-- -- Invalid type {type}")
            return

        # Parse Restapi response
        api_resp = RestAPIResponse(resp, self.logger)

        return api_resp

    def open_access(self,
                    selected_category: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        text_to_infer : str
            The text to infer.
        model_for_inference : str
            The model to use for inference.

        Returns
        -------
        InferencerResponse: InferencerResponse
            An object of the InferencerResponse class.
        """

        open_access = '1' if selected_category == 'Open Access' else '0'
        self.logger.info(f"-- -- Open Access label is: {selected_category}")

        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'open_access': open_access
        }

        url_ = '{}/queries/getOpenAccess'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def corpus_metadata(self) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        text_to_infer : str
            The text to infer.
        model_for_inference : str
            The model to use for inference.

        Returns
        -------
        InferencerResponse: InferencerResponse
            An object of the InferencerResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus'
        }

        url_ = '{}/queries/getCorpusMetadataFields'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp