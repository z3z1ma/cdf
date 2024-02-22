"""
# invalid, syntax should be SQL-based
PIPLINE:
    name data_pipeline,
    description 'Load data from source',
    tags [pii, main],
    cron '0 0 * * *',
    owner 'jdoe'
;
"""


def foo(n: int) -> int:
    return n + 1


x = 1
y = foo(x)
