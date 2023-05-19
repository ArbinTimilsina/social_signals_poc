import os
import urllib

def get_engine():
    client_config = eval(os.environ["config"])

    username = client_config["db_username"]
    # Special characters have to be URL-encoded
    password = urllib.parse.quote(client_config["db_password"]),
    host = client_config["db_host"]
    port = client_config["db_port"]
    name = client_config["db_name"]
    engine = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{name}"

    return engine
