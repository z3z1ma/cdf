import cdf


@cdf.with_config(sections=("my", "script"))
def entrypoint(workspace: cdf.Workspace):
    print("Hello from print_stuff.py!")
    print("The workspace is:")
    print(workspace)
