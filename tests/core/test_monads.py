import threading
import time
import typing as t
from collections import defaultdict

import requests

from cdf.core.monads import State, promise, state, to_state

threadtime = defaultdict(list)

T = t.TypeVar("T")


@promise
def fetch(url: str) -> requests.Response:
    tid = threading.get_ident()
    threadtime[tid].append(time.perf_counter())
    resp = requests.get(url)
    return resp


@promise
def track(v: T) -> T:
    tid = threading.get_ident()
    threadtime[tid].append(time.perf_counter())
    return v


@promise
def num_abilities(resp: requests.Response) -> int:
    data = resp.json()
    i = len(data["abilities"])
    tid = threading.get_ident()
    threadtime[tid].append(time.perf_counter())
    return i


def test_fetch():
    futs = []
    for i in range(5):
        print(f"Starting iteration {i}")
        futs.append(
            track("https://pokeapi.co/api/v2/pokemon/ditto")
            >> fetch
            >> track
            >> num_abilities
            >> track
        )

    for fut in futs:
        print(fut.unwrap())


def test_state():
    state_x = to_state(1)  # 1 is the value for the computations NOT the state

    @state
    def add_one(x: int) -> int:
        return x + 1

    def print_state(x: int):
        def _print(state: int):
            print(state)
            return x, state

        return _print

    add_one(State(print_state(1)))

    def process_int(x: int) -> State[list[int], int]:
        """Process an integer, tracking unique values"""

        def process(state: list[int]) -> t.Tuple[int, list[int]]:
            nonlocal x
            x += 1
            if x in state:
                return x, state
            state.append(x)
            return x, state

        return State(process)

    state_y = state_x >> process_int >> add_one >> process_int >> add_one
    x, y = state_y.run_state([])  # type: ignore
    assert x == 5
    assert y == [2, 4]
