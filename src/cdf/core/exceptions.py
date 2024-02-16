"""Exceptions for the CDF package."""


class CDFError(Exception):
    """Base class for all CDF exceptions."""


class CDFPipelineError(CDFError):
    """Base class for all CDF pipeline exceptions."""
