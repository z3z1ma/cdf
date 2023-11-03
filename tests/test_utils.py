import sys

from cdf.core.utils import augmented_path, do, index_destinations


def test_append_syspath(mocker):
    """Test that augmented_path appends to sys.path."""
    mocker.patch.object(sys, "path", new=[])

    # Test case 1: Append a path to sys.path
    with augmented_path("./tests/fixtures"):
        assert "./tests/fixtures" in sys.path
    # Test case 2: Append multiple paths to sys.path
    with augmented_path("./tests/fixtures", "./tests/fixtures/empty"):
        assert "./tests/fixtures" in sys.path
        assert "./tests/fixtures/empty" in sys.path
    # Test case 3: Append a path to sys.path, ensure it is removed
    with augmented_path("./tests/fixtures"):
        pass
    assert "./tests/fixtures" not in sys.path


def test_do():
    assert do(lambda x: x + 1, [1, 2, 3]) == [2, 3, 4]


def test_index_destinations():
    destinations = index_destinations(
        {
            "SOME_VAR": "some_value",  # This should be ignored
            "ANOHER_VAR": "another_value",  # This should be ignored
            "CDF_WRONGSYNTAX_NO_DUNDER": "wrong_syntax_value",  # This should be ignored
            "CDF__TEST_DUCKDB__ENGINE": "duckdb",  # Native cred is a string (no key)
            "CDF__TEST_DUCKDB__CREDENTIALS": "duckdb:///native/connection/neat",  # Native cred is a string (no key)
            "CDF__PROD_DB__ENGINE": "bigquery",  # Complex cred is a dict (has keys)
            "CDF__PROD_DB__CREDENTIALS__PROJECT_ID": "prod",  # Complex cred is a dict (has keys)
            "CDF__PROD_DB__CREDENTIALS__DATASET_ID": "public",
            "CDF__PROD_DB__CREDENTIALS__CREDENTIALS_FILE": "/path/to/credentials.json",
        }
    )
    assert "default" in destinations
    destinations.pop("default")
    assert len(destinations) == 2
    assert destinations["test_duckdb"] == ("duckdb", "duckdb:///native/connection/neat")
    assert destinations["prod_db"] == (
        "bigquery",
        {
            "project_id": "prod",
            "dataset_id": "public",
            "credentials_file": "/path/to/credentials.json",
        },
    )
