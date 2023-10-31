import pytest

import cdf.core.types as ct


def test_option():
    opt = ct.Option(1)
    assert opt.unwrap() == 1

    opt = ct.Option(None)
    with pytest.raises(ValueError):
        opt.unwrap()

    assert ct.Option(1) == ct.Option(1)
    assert ct.Option(1) != ct.Option(2)

    assert ct.Option(1).map(lambda x: x + 1) == ct.Option(2)
    assert ct.Option[int](None).map(lambda x: x + 1) == ct.Option(None)  # type: ignore


def test_result():
    result = ct.Result(1, None)
    assert result.unwrap() == 1

    result = ct.Result(None, Exception("test"))
    with pytest.raises(Exception):
        result.unwrap()

    result = ct.Result("Nice", None)
    res, err = result
    assert res == "Nice"
    assert err is None

    result = ct.Result(None, Exception("test"))
    res, err = result
    assert res is None
    assert err is not None

    result = ct.Result(1, None)
    assert result.map(lambda x: x + 1) == ct.Result(2, None)  # type: ignore
    with pytest.raises(Exception):
        result.map(lambda x: x / 0).unwrap()  # type: ignore

    result = ct.Result(None, None)
    with pytest.raises(ValueError):
        result.expect()
