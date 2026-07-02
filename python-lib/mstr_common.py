def get_connection_from_config(config):
    auth_type = config.get("auth_type", None)
    if auth_type=="oauth":
        username = None
        password = None
        auth_token = config.get("microstrategy_sso", {}).get("auth_token")
    else:
        username = config["microstrategy_api"].get("username", None)
        password = config["microstrategy_api"].get("password", '')
        auth_token = None
    return username, password, auth_token
