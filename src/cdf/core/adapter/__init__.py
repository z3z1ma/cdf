"""Adapters and factories for the core components of the CDF framework."""

import cdf.core.adapter.ingest as ingest
import cdf.core.adapter.state as state
import cdf.core.adapter.test as test
import cdf.core.adapter.transform as transform
from cdf.core.adapter.ingest import ingest_adapter_factory
from cdf.core.adapter.state import state_backend_factory
from cdf.core.adapter.test import test_adapter_factory
from cdf.core.adapter.transform import transform_adapter_factory

__all__ = [
    "ingest",
    "state",
    "test",
    "transform",
    "ingest_adapter_factory",
    "state_backend_factory",
    "test_adapter_factory",
    "transform_adapter_factory",
]
