"""
This Module interacts with Gerrit and retrieves Data from Gerrit
"""

import os
import json
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from urllib.parse import urlunsplit, urlencode
from typing import Tuple, Union
try:
    from requests import __version__, Session, adapters, exceptions, urllib3, status_codes
    logging.debug(f'Available request module of version {__version__}')
except ImportError:
    logging.error('Please install requests module. Use pip install requests.')

class GerritApi:
    """
    *Class name :* GerritHandler

    *Description :* Class to retrieve data from Gerrit
    """
    GET_ALL_REPO_URI = "/projects/?d"
    GET_ALL_CHANGES_URI = "/changes/?q=repo:{repo_name}"
    GET_ALL_ACTIVE_USERS_URI = "/accounts/?q=is:active"
    GET_COMMITS_BY_AGE = "/changes/?q=-age:"
    GET_COMMITS_USING_AFTER = "/changes/?q=after:"

    def __init__(self, gerrit_server: str, username: str=None, password: str=None):
        """
        *Method description :* Initializing values for Gerrit operations from OVF

        :param username: Username to login to Gerrit
        :type username: String
        :param password: Password to login to Gerrit
        :type password: String
        :param url: Gerrit URL to get commit details
        :type url: String
        """
        self.gerrit_username = username
        self.gerrit_password = password
        self.gerrit_url = f"https://{gerrit_server}"
        logging.debug(f"GerritDetails:: {self.gerrit_url}, {self.gerrit_username}, {self.gerrit_password}")
        if username and password:
            self.rest_engine = RestEngine(auth=(self.gerrit_username, self.gerrit_password))
        else:
            self.rest_engine = RestEngine()

    def get_all_projects(self) -> dict:
        """
        Method to get all repositories

        :returns: :class:`repo_details`: All repo details
        :rtype: :class:`repo_details`: Dict
        """
        all_repo_details = {}
        get_all_repo_url = f"{self.gerrit_url}{GerritApi.GET_ALL_REPO_URI}"
        all_repo_resp = self.decode_response(self.rest_engine.rest_request(get_all_repo_url))
        for key, value in all_repo_resp.items():
            all_repo_details[key] = {"id": value.get("id"), "description": value.get("description"),
                                 "state": value.get("state")}
        logging.info(f"List of All repositories : {all_repo_details} {len(all_repo_details)}")
        return all_repo_details

    def get_all_active_projects(self) -> list:
        """
        Method to get all active repositories

        :returns: :class:`active_repo_list`: List of active repositories
        :rtype: :class:`active_repo_list`: List
        """
        active_repo_list = []
        all_repo_details = self.get_all_projects()
        for key, value in all_repo_details.items():
            if value["state"] == "ACTIVE":
                active_repo_list.append(key)
        logging.info(f"List of active repositories : {active_repo_list} {len(active_repo_list)}")
        return active_repo_list

    def get_active_user_accounts(self) -> list:
        """
        *Method description :* Method to get active user accounts in server

        :returns: :class:`all_users_details`: List of commit changes as dict
        :rtype: :class:`all_users_details`: list
        """
        all_users_details = []
        all_users_list, mocker_response = [], []
        all_users_url = f"{self.gerrit_url}{GerritApi.GET_ALL_ACTIVE_USERS_URI}&S=0"
        response = self.decode_response(self.rest_engine.rest_request(all_users_url))
        all_users_list.extend(response)
        mocker_response = self.no_limit_mocker(response, mocker_response,
                                        url_to_be_used=f"{self.gerrit_url}{GerritApi.GET_ALL_ACTIVE_USERS_URI}")
        if all_users_list:
            all_users_list.extend(mocker_response)
        logging.info(f"Number Of Active User Accounts in Gerrit: {len(all_users_list)}")
        for each_user in all_users_list:
            user_id = each_user.get("_account_id")
            user_details_url = f"{self.gerrit_url}/accounts/{user_id}/detail"
            detailed_response = self.decode_response(self.rest_engine.rest_request(user_details_url))
            all_users_details.append(detailed_response)
        logging.info(f"Active User Account Details in Gerrit: {all_users_details}")
        return all_users_details

    def get_commit_details_in_given_period(self, start=None, duration="24Hours", stop=datetime.utcnow()):
        all_commits_list, mocker_response = [], []
        if not start:
            start = self.get_start_time(duration, stop)
        commits_url = f"{self.gerrit_url}{GerritApi.GET_COMMITS_USING_AFTER}\"{start}\"&S=0"
        print(commits_url)
        response = self.decode_response(self.rest_engine.rest_request(commits_url))
        all_commits_list.extend(response)
        mocker_response = self.no_limit_mocker(response, mocker_response,
                                url_to_be_used=f"{self.gerrit_url}{GerritApi.GET_COMMITS_USING_AFTER}\"{start}\"")
        if mocker_response:
            all_commits_list.extend(mocker_response)
        for each_commit in all_commits_list:
            owner_account_url = f"{self.gerrit_url}/accounts/{each_commit.get('owner').get('_account_id')}/detail"
            each_commit["owner"] = self.decode_response(self.rest_engine.rest_request(owner_account_url)).get("name")
            if each_commit.get("submitter"):
                submitter_id = each_commit.get('submitter').get('_account_id')
                submit_account_url = f"{self.gerrit_url}/accounts/{submitter_id}/detail"
                each_commit["submitter"] = self.decode_response(self.rest_engine.rest_request(
                                                                                    submit_account_url)).get("name")
        print(f"Total commits from {start} is: {len(all_commits_list)}")
        return all_commits_list

    @staticmethod
    def get_start_time(duration, stop):
        if "minutes" in str(duration).lower():
            min_delta = int(str(duration).lower().strip("minutes"))
            start = stop - timedelta(minutes=min_delta)
        if "hours" in str(duration).lower():
            hour_delta = int(str(duration).lower().strip("hours"))
            start = stop - timedelta(hours=hour_delta)
        elif "days" in str(duration).lower():
            day_delta = int(str(duration).lower().strip("days"))
            start = stop - timedelta(days=day_delta)
        elif "months" in str(duration).lower():
            month_delta = int(str(duration).lower().strip("months"))
            start = stop - timedelta(months=month_delta)
        return start

    @staticmethod
    def decode_response(response: str) -> dict:
        """
        *Method description :* Method to decode rest response with Gerrit Magic Prefix

        :param response: Raw REST Response Content
        :type response: String
        :raises: :class:`ValueError`: Invaid Response Json Content
        :returns: :class:`resp_dict`: Dictionary of the given Response content
        :rtype: :class:`resp_dict`: Dictionary
        """
        output = response[1]
        # prefix that comes with the json responses.
        gerrit_magic_json_prefix = ")]}'\n"
        if str(response[0]) == '200' and isinstance(response[1], str):
            if response[1].startswith(gerrit_magic_json_prefix):
                output = response[1][len(gerrit_magic_json_prefix):]
                try:
                    output = json.loads(output)
                except ValueError:
                    logging.error(f"Invalid Json in response {output}")
        else:
            logging.error(f'Rest Call Failed with the status code {response[0]} and response {response[1]}')
        return output

    def no_limit_mocker(self, response: str, mocker_response: list, url_to_be_used: str,
                                                                                def_limit: int =0) -> list:
        """
        *Method description :* Method to mock no_limit option in Gerrit Server

        :param response: Previous GET Call Response
        :type response: String
        :param mocker_response: Mocker response list on which no_limit responses are accumulated
        :type mocker_response: list
        :param url_to_be_used: URL to be used for REST Call in no_limits mocker block
        :type url_to_be_used: String
        :param def_limit: Number Of Commits Limit for GET call
        :type def_limit: Integer
        :returns: :class:`mocker_response`: Get REST Response in List
        :rtype: :class:`mocker_response`: List
        """
        if "_more_" in str(response):
            def_limit = def_limit + 500
            start_limit = def_limit - 500 + 1
            logging.info(f"Fetching {start_limit} - {def_limit} Records. Please Wait...")
            new_url = f"{url_to_be_used}&S={str(def_limit)}&n=500"
            int_response = self.decode_response(self.rest_engine.rest_request(new_url))
            mocker_response.extend(int_response)
            self.no_limit_mocker(int_response, mocker_response, url_to_be_used, def_limit)
        else:
            def_limit = def_limit + 500
            new_url = f"{url_to_be_used}&S={str(def_limit)}&n=500"
            int_response = self.decode_response(self.rest_engine.rest_request(new_url))
            mocker_response.extend(int_response)
        return mocker_response

class RestEngine:
    """
    Class to perform rest operations like PUT, PATCH, POST, GET
    DELETE, HEAD, OPTIONS.
    """
    def __init__(self, **session_args: str):
        """
        *Method description :* Initialization method.

        1. Initialize a http session with the session parameters passed by user
        2. Default authentication is set to (username, password) as (admin, admin).
           And a header with json content type is added.
        3. These session level parameters are overwritten when the same are provided
           at the method level.

        :param session_args: Rest arguments that can be set at the session level.
                             Supported: 'headers', 'cookies', 'auth', 'proxies', 'hooks',
                             'params', 'verify', 'cert', 'stream', 'trust_env', 'max_redirects'
        :type session_args: dict
        """
        self.http_session = Session()
        self.http_session.auth = session_args.get('auth')
        self.http_session.headers.update(session_args.get('headers', {}))
        #as verify is set to False,requests in this session will accept any TLS certificate
        #will ignore SSL certificate verification
        self.http_session.verify = session_args.get('verify', False)
        #Retries to establish a http secure connection.
        https_adapter = adapters.HTTPAdapter(max_retries=3)
        self.http_session.mount('https://', https_adapter)
        #To set other session parameters supported by requests
        self.http_session.params = session_args.get('params')
        self.http_session.proxies = session_args.get('proxies')
        self.http_session.cert = session_args.get('cert')
        self.http_session.hooks = session_args.get('hooks')
        self.http_session.stream = session_args.get('stream')
        self.http_session.max_redirects = session_args.get('max_redirects')
        self.http_session.cookies.update(session_args.get('cookies', {}))
        self.http_session.trust_env = session_args.get('trust_env')

    @staticmethod
    def build_api_url(netloc: str, scheme: str ="https", path: str ="", query: Union[str, dict]="",
                      fragments: str ="") -> str:
        """Generates complete url from the inputs provided by the user.
        URL format : scheme://netloc/path?query#fragments

        #query str: page=12
        eg : https://docs.python.com/tutorial/index.html?page=12#datatypes

        #query dict: {page:12, type:tuple)
        eg : https://docs.python.com/tutorial/index.html?page=12&type=tuple#datatypes

        :param netloc: Network location part. Domain name should be given as input.
            (eg): example.com, 168.0.0.1:8080, jenkins.com:8443
        :type netloc: str
        :param scheme: URL scheme specifier. Can be either http or https, defaults to "https"
        :type scheme: str, optional
        :param path: Hierarchical path. Additional path to be added to the netloc, defaults to ""
        :type path: str, optional
        :param query: query string needed to be added. It will be added after the "?" symbol.
            Can be given directly as string or dict with multiple key value pairs. if multiple key
            value pairs are given then query string will be concatenated with "&" symbol, defaults to ""
        :type query: str or dict, optional
        :param fragments: Additional piece of information to be added to the url. This will be added
            after the "#" symbol, defaults to ""
        :type fragments: str, optional
        :return: complete api url
        :rtype: str
        """
        query_str = urlencode(query) if isinstance(query, dict) else query
        api_url = urlunsplit((scheme, netloc, path, query_str, fragments))
        logging.debug(f"Api url formed --> {api_url}")
        return api_url

    def rest_request(self, uri: str, operation: str ='GET', **func_args: str) -> Tuple[int, str, dict]:
        """
        *Method description :* Common rest request method be called for performing the rest operations.

        :param uri: rest uri
        :type uri: str
        :param operation: rest operation, could be GET, POST, PATCH, DELETE, PUT, HEAD, OPTIONS.
        :type operation: str
        :param func_args: Rest arguments such as 'auth', 'cookies', 'data', 'files',
                          'headers', 'hooks', 'json', 'params', 'timeout', 'allow_redirects', 'proxies',
                          'hooks', 'stream', 'verify', 'cert' that can be set at the method request level.
                          Overrides the session arguments.
        :type func_args: dict
        :returns: :class:`response_code`: Response code of the rest request call performed
                  :class:`response`: Response received from the rest request call
                  :class:'response_headers`: Headers in response
        :rtype: :class:`response_code`: int
                :class:`response`: dict/str
                :class:`response_headers`: dict
        """
        response_code, response, response_headers = None, None, None
        #suppress Insecure certificate warning
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        try:
            rest_response = self.http_session.request(operation.upper(), uri, **func_args)
            logging.debug(f'Request uri : {rest_response.request.url}')
            logging.debug(f'Request method : {rest_response.request.method}')
            logging.debug(f'Request headers : {rest_response.request.headers}')
            logging.debug(f'Request data : {rest_response.request.body}')
            response_code, response, response_headers = rest_response.status_code, rest_response.content, rest_response.headers
            #Uncomment the below line if status code has to raise an exception/error
            #rest_response.raise_for_status()
            if response:
                try:
                    response = rest_response.json()
                except JSONDecodeError:
                    #default utf-8 encoding is done.
                    response = rest_response.text
        except exceptions.InvalidURL:
            logging.error(f'The uri {uri} passed for this {operation.upper()} method is invalid')
        except exceptions.HTTPError:
            logging.error(f'The {operation.upper()} method failed with the status code {response_code}' \
                           f' and status message would be any of {status_codes._codes[response_code]}.')
        except exceptions.SSLError:
            logging.error('SSL Certificate verification failed.')
        except exceptions.ConnectionError:
            logging.error(f'Failed to establish a connection with {uri}')
        except exceptions.InvalidHeader:
            logging.error(f'Invalid header exception. Request headers added : {rest_response.request.headers}')
        except exceptions.TooManyRedirects:
            logging.error('The URL redirects has crossed the maximum limit of 30.')
        except exceptions.Timeout:
            logging.error(f'{operation.upper()} request timed out. Can be either Connection or Read timeout.')
        except exceptions.RequestException:
            logging.error('Exception occurred while handling request. Please check if the input passed are correct.')
        except TypeError:
            logging.error('Please re-check if the input arguments passed are valid.')
        logging.debug(f'Rest Response : {response}')
        logging.debug(f'Rest Response status code : {response_code}')
        logging.debug(f'Rest Response headers : {response_headers}')
        if response_code:
            logging.debug(f'Possible status message for {response_code} : {status_codes._codes[response_code]}')
        return response_code, response, response_headers

class Common:
    """
    Class to perform rest operations like PUT, PATCH, POST, GET
    DELETE, HEAD, OPTIONS.
    """

    @staticmethod
    def convert_json_to_dict(json_file: str) -> Union[dict, None]:
        """Converts the input json file into dictionary

        :param json_file: Name of the json file to be converted
        :type json_file: str
        :return: Converted dictionary
        :rtype: dict or None
        """
        try:
            assert os.path.exists(json_file)
            with open(json_file, 'r') as file_obj:
                data_dict = json.load(file_obj)
            return data_dict
        except AssertionError:
            logging.error(f'Json file {json_file} doesnot exists')
        except json.decoder.JSONDecodeError as decode_err:
            logging.error(f'unable to parse {json_file}. Kindly validate the json file. Error occured: {decode_err}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--servername", type=str, help="Gerrit Server Name/IP")
    parser.add_argument("-u", "--user", type=str, help="Gerrit Login Username", default=None)
    parser.add_argument("-p", "--password", type=str, help="Gerrit Login Password", default=None)
    parser.add_argument("-d", "--duration", type=str, help="Duration for which gerrit changes to be fetched\n\
        Supported are Minutes, Hours, Days, Months. Examples: 120Minutes, 48Hours, 2Days, 1Month \n\
        Default : 24Hours", default="24Hours")
    args = parser.parse_args()
    if args.servername and args.duration:
        obj = GerritApi(f"{args.servername}")
        commits_list = obj.get_commit_details_in_given_period(duration=args.duration)
        print(f"Gerrit commits for given {args.duration} is: {len(commits_list)}\n")
        print("Gerrit Commits Details are saved in new_commits.csv file")
        cl_df = pd.DataFrame(commits_list)
        cl_df.to_csv('new_commits.csv')
    else:
        print("Please pass Gerrit server name with -s and duration with -d argument !!!")
