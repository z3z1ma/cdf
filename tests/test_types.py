import pytest

import cdf.core.monads as monads


def test_option():
    opt = monads.Option(1)
    assert opt.unwrap() == 1

    opt = monads.Option(None)
    with pytest.raises(ValueError):
        opt.unwrap()

    assert monads.Option(1) == monads.Option(1)
    assert monads.Option(1) != monads.Option(2)

    assert monads.Option(1).map(lambda x: x + 1) == monads.Option(2)
    assert monads.Option[int](None).map(lambda x: x + 1) == monads.Option(None)  # type: ignore


def test_result():
    result = monads.Result(1, None)
    assert result.unwrap() == 1

    result = monads.Result(None, Exception("test"))
    with pytest.raises(Exception):
        result.unwrap()

    result = monads.Result("Nice", None)
    res, err = result
    assert res == "Nice"
    assert err is None

    result = monads.Result(None, Exception("test"))
    res, err = result
    assert res is None
    assert err is not None

    result = monads.Result(1, None)
    assert result.map(lambda x: x + 1) == monads.Result(2, None)  # type: ignore
    with pytest.raises(Exception):
        result.map(lambda x: x / 0).unwrap()  # type: ignore

    result = monads.Result(None, None)
    with pytest.raises(ValueError):
        result.expect()
