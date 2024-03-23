import ijson, gzip, os
from decimal import Decimal
from bson.decimal128 import Decimal128

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
def mongodb():
    """
    Wouldn't want the fallback uri in prod
    """
    uri = os.getenv("MONGODB_URL", "mongodb://root:pass@mongodb:27017/")
    client = MongoClient(uri)
    return client.veryfi

@op(out=DynamicOut())
def extract_items() -> Output:
    """
    extracting all the data
    ijson to stream in the data from the file without
    needing to load it all in memory at once

    file should come from an api endpoint rather than be stored locally
    gzipped to reduce size
    """
    i = 0
    with gzip.open("pipeline/dataset.json.gz", "r") as raw_data:
        for item in ijson.items(raw_data, "item"):
            if i >= 100:
                break
            i += 1
            yield DynamicOutput(item, mapping_key=item["code"])

@op(required_resource_keys={"mongodb"})
def dump_piece(context, piece: dict):
    """
    Upsert the data into mongodb
    runs faster with more dagster workers
    """
    def transform(item: dict):
        """
        replace _id with code as primary key
        """
        oid = item.pop("_id", None)
        code = item.pop("code")
        if oid:
            item["oid"] = oid
        item = convert_decimal(item)
        return code, item

    def convert_decimal(dict_item):
        """
        Recursively convert decimals for bson format
        """
        if dict_item is None: return None

        for k, v in list(dict_item.items()):
            if isinstance(v, dict):
                convert_decimal(v)
            elif isinstance(v, list):
                for l in v:
                    convert_decimal(l)
            elif isinstance(v, Decimal):
                dict_item[k] = Decimal128(str(v))

        return dict_item
    item = transform(piece)
    context.resources.mongodb.veryfi.replace_one({"_id": item[0]}, item[1], upsert=True)


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