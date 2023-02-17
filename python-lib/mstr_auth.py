import requests
import logging


logging.basicConfig(level=logging.INFO, format='dss-plugin-microstrategy %(levelname)s - %(message)s')
logger = logging.getLogger()


class MstrAuth(requests.auth.AuthBase):
    def __init__(self, server_url, username, password):
        auth_token, cookies = request_auth_token(server_url, username, password)
        self.auth_token = auth_token
        self.cookies = cookies
        self.cookies_string = None
        if cookies:
            self.cookies_string = build_cookies_string(cookies)

    def __call__(self, request):
        request.headers["X-MSTR-AuthToken"] = self.auth_token
        if self.cookies_string:
            request.headers["Cookie"] = self.cookies_string
        return request


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
        "loginMode": 1
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
