import os

def get_engine():
    client_config = eval(os.environ["config"])

    username = client_config["db_username"]
    password = client_config["db_password"]
    host = client_config["db_host"]
    port = client_config["db_port"]
    name = client_config["db_name"]
    engine = f"mysql+pymysql://{username}:{password}@{host}:{port}/{name}"

    return engine
