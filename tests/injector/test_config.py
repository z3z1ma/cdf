import os
from unittest.mock import patch

import pytest

from cdf.injector import ConfigResolver


def test_apply_converters():
    with patch.dict("os.environ", {}):
        os.environ["CDF_TEST"] = "1"
        assert ConfigResolver.apply_converters("$CDF_TEST") == "1"
        assert ConfigResolver.apply_converters("@int ${CDF_TEST}") == 1
        os.environ["CDF_BOOL"] = "true"
        assert ConfigResolver.apply_converters("@bool ${CDF_BOOL}") is True
        os.environ["CDF_FLOAT"] = "3.14"
        assert ConfigResolver.apply_converters("@float ${CDF_FLOAT}") == 3.14
        os.environ["CDF_JSON"] = '{"key": "value"}'
        assert ConfigResolver.apply_converters("@json ${CDF_JSON}") == {"key": "value"}
        os.environ["CDF_PATH"] = "tests/v2/test_config.py"
        assert ConfigResolver.apply_converters("@path ${CDF_PATH}") == os.path.abspath(
            "tests/v2/test_config.py"
        )
        os.environ["CDF_DICT"] = "{'key': 'value'}"
        assert ConfigResolver.apply_converters("@dict ${CDF_DICT}") == {"key": "value"}
        os.environ["CDF_LIST"] = "['key', 'value']"
        assert ConfigResolver.apply_converters("@list ${CDF_LIST}") == ["key", "value"]
        os.environ["CDF_TUPLE"] = "('key', 'value')"
        assert ConfigResolver.apply_converters("@tuple ${CDF_TUPLE}") == (
            "key",
            "value",
        )
        os.environ["CDF_SET"] = "{'key', 'value'}"
        assert ConfigResolver.apply_converters("@set ${CDF_SET}") == {"key", "value"}

        with pytest.raises(ValueError):
            ConfigResolver.apply_converters("@unknown_converter idk")
        with pytest.raises(ValueError):
            ConfigResolver.apply_converters("@int something")

        assert ConfigResolver.apply_converters("no conversion") == "no conversion"


def test_config_resolver():
    os.environ["CDF_TEST"] = "1"
    resolver = ConfigResolver(
        {
            "main_api": {
                "user": "someone",
                "password": "secret",
                "database": "test",
            },
            "db_1": "@int ${CDF_TEST}",
            "db_2": "@resolve main_api",
        }
    )

    assert resolver["main_api"] == {
        "user": "someone",
        "password": "secret",
        "database": "test",
    }
    assert resolver["db_1"] == 1
    assert resolver["main_api"] == resolver["db_2"]
    resolver._loader.import_({"db_1": 2})
    assert resolver["db_1"] == 2

    @ConfigResolver.map_values(db_1="db_1", db_2="db_2")
    def foo(db_1: int, db_2: dict):
        return db_1, db_2

    foo_configured = resolver.resolve_defaults(foo)
    assert foo_configured() == (
        2,
        {"user": "someone", "password": "secret", "database": "test"},
    )

    @ConfigResolver.map_values(
        user="main_api.user", password="main_api.password", database="main_api.database"
    )
    def bar(user: str, password: str, database: str):
        return user, password, database

    bar_configured = resolver.resolve_defaults(bar)
    assert bar_configured() == ("someone", "secret", "test")
