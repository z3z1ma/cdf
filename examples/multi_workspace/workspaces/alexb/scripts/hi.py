import cdf


@cdf.with_config(sections=("my", "script"))
def entrypoint(ws: cdf.Workspace) -> None:
    """A simple script to demonstrate the api of cdf scripts"""
    print(f"Hello, world from {ws.namespace}!")
