import requests
import logging
import time


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class MstrAuth(requests.auth.AuthBase):
    def __init__(self, server_url, username, password):
        self.server_url = server_url
        self.username = username
        self.password = password
        self.auth_token = None
        self.cookies = None
        self.cookies_string = None
        self.token_time_limit = None

    def __call__(self, request):
        if (not self.auth_token):
            self.refresh_token()
        request.headers["X-MSTR-AuthToken"] = self.auth_token
        if self.cookies_string:
            request.headers["Cookie"] = self.cookies_string
        return request

    def is_token_expired(self):
        if not self.token_time_limit:
            logger.info("is_token_expired: Auth token was never retrieved")
            return True

        if get_epoch_time_now() > self.token_time_limit:
            logger.info("is_token_expired: Auth token expires in 10 minutes")
            return True

        return False

    def refresh_token(self):
        logger.info("Retrieving new auth token")
        self.auth_token, self.cookies = request_auth_token(self.server_url, self.username, self.password)
        self.cookies_string = build_cookies_string(self.cookies)
        self.refresh_token_time_limit()
        logger.info("Estimated validity for new token: {}".format(self.token_time_limit))

    def refresh_token_time_limit(self):
        self.token_time_limit = get_epoch_time_now() + 3000  # 50 minutes in future


def get_epoch_time_now():
    return int(time.time())


def build_cookies_string(cookies_dictionary):
    coockies = []
    for cookie_name in cookies_dictionary:
        cookie_value = cookies_dictionary.get(cookie_name, "")
        coockies.append("{}={}".format(cookie_name, cookie_value))
    return ";".join(coockies)


def request_auth_token(server_url, username, password):
    url = "{}/auth/login".format(server_url)
    data = {
        "username": username,
        "password": password,
        "loginMode": 1,
        "applicationType": 35
    }
    logger.info("Requesting auth token to {}".format(server_url))
    response = requests.post(url=url, data=data)
    status_code = response.status_code
    if status_code >= 400:
        error_message = "Error {} while requesting auth token to {}".format(status_code, server_url)
        logger.error(error_message)
        logger.error("dumping: {}".format(response.content))
        raise Exception(error_message)
    auth_token = response.headers.get("X-MSTR-AuthToken")
    cookies = dict(response.cookies)
    if auth_token or cookies:
        logger.info("Auth token obtained from {}".format(server_url))
    return auth_token, cookies
