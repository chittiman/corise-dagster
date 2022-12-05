from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    #tags={"kind": "s3"}
)
def get_s3_data(context)-> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stock_iterator = context.resources.s3.get_data(s3_key)
    stocks = [Stock.from_list(stock) for stock in stock_iterator]
    return stocks

@op
def process_data(context,stocks: List[Stock]):
    sorted_stocks = sorted(stocks, key = lambda stock: stock.high, reverse=True)
    highest_stock = sorted_stocks[0]
    stock_date, stock_high = highest_stock.date, highest_stock.high
    return Aggregation(date=stock_date, high=stock_high)


@op(required_resource_keys={"redis"})
def put_redis_data(context,aggregation: Aggregation):
    context.resources.redis.put_data(name=str(aggregation.date),value=str(aggregation.high))

@op(required_resource_keys={"s3"})
def put_s3_data(context,aggregation: Aggregation):
    context.resources.s3.put_data(key_name = "aggregation",data=aggregation)


@graph
def week_2_pipeline():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, 
                    "redis":ResourceDefinition.mock_resource() 
                    }
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, 
                    "redis":redis_resource 
                    }
)
