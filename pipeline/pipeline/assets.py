import ijson, gzip

from pymongo import MongoClient
from dagster import (
    Output,
    DynamicOut,
    DynamicOutput,
    op,
    job,
    resource,
    schedule,
    RunRequest
)


@resource()
def mongodb(context):
    # uri = context.resource_config["uri"]
    # client = MongoClient(uri)
    # return client.veryfi
    return "test_db"

@op(out=DynamicOut())
def extract_items() -> Output:
    i = 0
    with gzip.open("pipeline/dataset.json.gz", "r") as raw_data:
        for item in ijson.items(raw_data, "item"):
            if i >= 100:
                break
            i += 1
            yield DynamicOutput(item, mapping_key=item["code"])

@op(required_resource_keys={"mongodb"})
def dump_piece(context, piece):
    print(context.resources.mongodb)
    def transform(item: dict):
        oid = item.pop("_id", None)
        code = item.pop("code")
        item["_id"] = code
        if oid:
            item["oid"] = oid
        return item
    print(transform(piece))
    # context.resources.mongodb.veryfi.upsert_one(transform(piece))


@job(resource_defs={"mongodb": mongodb})
def load_items():
    pieces = extract_items()
    pieces.map(dump_piece)


@schedule(job=load_items, cron_schedule="0 0 * * 0")
def scheduled_job(context):
    """
    Runs every Sunday at midnight
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=None,
        run_config={
            "ops": {"configurable_op": {"config": {"scheduled_date": scheduled_date}}}
        },
        tags={"date": scheduled_date},
    )