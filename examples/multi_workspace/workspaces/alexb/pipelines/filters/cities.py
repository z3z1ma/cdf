"""
Filters for the cities dataset.

Many filters can be represented via cdf.builtin.filters, but sometimes you need
to write your own. This is a contrived example of how to do that. Its worth noting
that you _can_ apply this code directly to the pipeline code itself, but sometimes
this pattern is more useful / readable / composable. They are attached in the
pipeline spec.
"""


def not_alaska(item):
    """Filters out items where the state is Alaska"""
    return item["state"] != "AK"


def not_hawaii(item):
    """Filters out items where the state is hawaii"""
    return item["state"] != "HI"
