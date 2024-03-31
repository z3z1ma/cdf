import sys


def is_main() -> bool:
    """Check if the current module is being run as the main program."""
    frame = sys._getframe(1)
    name = frame.f_globals["__name__"]
    cdf_name = frame.f_globals.get("__cdf_name__")
    return name == "__main__" or name == cdf_name
