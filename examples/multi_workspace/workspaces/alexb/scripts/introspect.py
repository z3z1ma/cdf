import cdf


def config(ws: cdf.Workspace) -> None:
    """A simple script to demonstrate the api of cdf scripts"""
    print(ws)
    print(ws.config_dict)
