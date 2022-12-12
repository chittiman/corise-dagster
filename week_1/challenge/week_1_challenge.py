import csv
from datetime import datetime
from heapq import nlargest
from random import randint
from typing import Iterator, List ,Union

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    out={
        "stocks": Out(is_required=False),
        "empty_stocks": Out(is_required=False),
    },
)
def get_s3_data(context)-> Union[List[Stock],None]:
    s3_key = context.op_config["s3_key"]
    stock_iterator = csv_helper(s3_key)
    stocks = [stock for stock in stock_iterator]
    if len(stocks) > 0 :
        yield Output(stocks,"stocks")
    else:
        yield Output(None,"empty_stocks")


@op(
    ins={"stocks": In()},
    out={"aggregation": Out()}
)
def process_data(context,stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key = lambda stock: stock.high, reverse=True)
    highest_stock = sorted_stocks[0]
    stock_date, stock_high = highest_stock.date, highest_stock.high
    aggregation = Aggregation(date=stock_date, high=stock_high)
    return aggregation

@op(
    ins={"aggregation": In()}
)
def put_redis_data(context,aggregation: Aggregation):
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks) -> Nothing:
    context.log.info("No stocks returned")


@job
def week_1_challenge():
    stocks, empty_stocks = get_s3_data()
    empty_stock_notify(empty_stocks)
    put_redis_data(process_data(stocks))

