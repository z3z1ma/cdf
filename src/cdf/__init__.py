import pdb
import sys
import traceback

import cdf.core.constants as c
import cdf.core.context as context
from cdf.core.runtime.pipeline import pipeline_factory as pipeline


def execute() -> bool:
    """Check if the current module is being run as the main program in cdf context.

    Also injects a hook in debug mode to allow dropping into user code via pdb.
    """
    frame = sys._getframe(1)
    name = frame.f_globals["__name__"]

    _main = frame.f_globals.get(c.CDF_MAIN)
    proceed = name == "__main__" or name == _main

    if proceed and context.debug_mode.get():

        def debug_hook(etype, value, tb) -> None:
            traceback.print_exception(etype, value, tb)
            pdb.post_mortem(tb)

        sys.excepthook = debug_hook

    return proceed


__all__ = ["pipeline", "execute"]
