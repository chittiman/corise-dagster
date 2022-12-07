from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    String
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
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
def week_3_pipeline():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

def set_s3_key_in_docker_config(key,config=docker):
    config["ops"]["get_s3_data"]["config"]["s3_key"] = key
    return config

partitions = [str(x) for x in range(1,11)]

@static_partitioned_config(partition_keys=partitions)
def docker_config(partition_key:str,config = docker):
    s3_key = f"prefix/stock_{partition_key}.csv"
    config = set_s3_key_in_docker_config(s3_key)
    return config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource,"redis":ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource,"redis":redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10,delay=1)
)


week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")

#0 * * * * -> Begining of every hour

@schedule(job=week_3_pipeline_docker, cron_schedule="0 * * * *")
def week_3_schedule_docker():
    for partition_key in partitions:
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=partition_key, run_key=partition_key)

@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    #bucket = context.resources.s3.bucket
    #endpoint_url = context.resources.s3.endpoint_url
    bucket = docker["resources"]["s3"]["config"]["bucket"]
    endpoint_url = docker["resources"]["s3"]["config"]["endpoint_url"]
    new_files = get_s3_keys(bucket=bucket,prefix="prefix",endpoint_url=endpoint_url)
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        config = set_s3_key_in_docker_config(new_file)
        yield RunRequest(
            run_key=new_file,
            run_config=config
            )

