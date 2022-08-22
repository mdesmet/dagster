from dagster import job, op


@op
def return_one():
    return 1


@op
def add_two(i: int):
    return i + 2


@op
def multi_three(i: int):
    return i * 3


@job
def my_job():
    multi_three(add_two(return_one()))