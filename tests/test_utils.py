import sys

from cdf.core.utils import augmented_path, do


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
