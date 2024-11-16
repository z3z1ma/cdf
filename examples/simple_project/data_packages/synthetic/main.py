"""Hello, world!"""

from cdf.core.container import inject_deps


@inject_deps
def main_pipeline(test1: int, test2: int) -> None:
    print("Hello from synthetic!", test1, test2)


if __name__ == "__main__":
    main_pipeline()
