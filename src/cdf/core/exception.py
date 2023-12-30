"""All common exceptions for the cdf package."""


class CDFException(Exception):
    pass


class CDFConfigMissing(CDFException, KeyError):
    pass


class CDFConfigInvalid(CDFException, ValueError):
    pass
