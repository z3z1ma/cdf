import pdb
import sys
import traceback

import cdf.core.context as context
from cdf.core.runtime.pipeline import pipeline_factory as pipeline


def execute() -> bool:
    """Check if the current module is being run as the main program."""
    frame = sys._getframe(1)
    name = frame.f_globals["__name__"]

    # TODO: there might be a better way to assert we are being executed by cdf via runpy
    cdf_name = frame.f_globals.get("__cdf_name__")
    proceed = name == "__main__" or name == cdf_name

    if proceed and context.debug_mode.get():

        def debug_hook(etype, value, tb) -> None:
            traceback.print_exception(etype, value, tb)
            pdb.post_mortem(tb)

        sys.excepthook = debug_hook

    return proceed


__all__ = ["pipeline", "execute"]
