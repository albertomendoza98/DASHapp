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
        selected_category : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
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
    
    def continent(self, 
                  continent: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        continent : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'continent': continent
        }

        url_ = '{}/queries/getDocsByContinent'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def city(self, 
                  city: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        city : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'city': city
        }

        url_ = '{}/queries/getDocsByCity'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp

    def cited_count(self, 
                  label: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        label : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        if label == '< 5':
            lower_limit = '0'
            upper_limit = '5'
        elif label == '5 - 9':
            lower_limit = '5'
            upper_limit = '10'
        elif label == '10 - 24':
            lower_limit = '10'
            upper_limit = '25'
        elif label == '25 >':
            lower_limit = '25'
            upper_limit = '*'  

        params_ = {
            'corpus_collection': 'scopus',
            'lower_limit': lower_limit,
            'upper_limit': upper_limit
        }

        url_ = '{}/queries/getDocsByCitedCount'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp

    def fund(self, 
                  fund: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        fund : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'fund_sponsor': fund
        }

        url_ = '{}/queries/getDocsByFundSponsor'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def institution(self, 
                  institution: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        institution : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'institution': institution
        }

        url_ = '{}/queries/getDocsByInstitution'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def topic_label(self, 
                  topic_label: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        topic_label : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'model_collection': 'mallet-50',
            'topic_label': topic_label
        }

        url_ = '{}/queries/getDocsByTopicLabel'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def year(self, 
                  year: str) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        year : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'corpus_collection': 'scopus',
            'year': year
        }

        url_ = '{}/queries/getDocsByYear'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp
    
    def topic_map(self) -> RestAPIResponse:
        """Execute query to filter by open access.

        Parameters
        ----------
        year : str

        Returns
        -------
        RestAPIResponse: RestAPIResponse
            An object of the RestAPIResponse class.
        """
        
        headers_ = {'Accept': 'application/json'}

        params_ = {
            'model_collection': 'mallet-50',
        }

        url_ = '{}/queries/getTopicMap'.format(self.restapi_url)
        self.logger.info(f"-- -- The restapi url is: {url_}")

        # Send request to RestAPI
        api_resp = self._do_request(
            type="get", url=url_, timeout=120, headers=headers_, params=params_)

        return api_resp