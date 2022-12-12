from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise",
)
def get_s3_data(context)-> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stock_iterator = context.resources.s3.get_data(s3_key)
    stocks = [Stock.from_list(stock) for stock in stock_iterator]
    return stocks


@asset(group_name="corise")
def process_data(get_s3_data) -> Aggregation:
    stocks = get_s3_data
    sorted_stocks = sorted(stocks, key = lambda stock: stock.high, reverse=True)
    highest_stock = sorted_stocks[0]
    stock_date, stock_high = highest_stock.date, highest_stock.high
    return Aggregation(date=stock_date, high=stock_high)


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    group_name="corise"
)
def put_redis_data(context,process_data):
    aggregation = process_data
    context.resources.redis.put_data(name=str(aggregation.date),value=str(aggregation.high))


@asset(
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    group_name="corise"
)
def put_s3_data(context,process_data):
    aggregation = process_data
    context.resources.s3.put_data(key_name = "aggregation",data=aggregation)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource,"redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
)


