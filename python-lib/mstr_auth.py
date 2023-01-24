import requests


class MstrAuth(requests.auth.AuthBase):
    def __init__(self, auth_token, cookies):
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
