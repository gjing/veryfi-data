import ijson, gzip, os
from decimal import Decimal
from bson.decimal128 import Decimal128

from pymongo import MongoClient, UpdateOne
from dagster import (
    Output,
    DynamicOut,
    DynamicOutput,
    op,
    job,
    resource,
    schedule,
    RunRequest,
    Field
)


@resource()
def mongodb():
    """
    Wouldn't want the fallback uri in prod
    """
    uri = os.getenv("MONGODB_URL", "mongodb://root:pass@mongodb:27017/")
    client = MongoClient(uri)
    return client.veryfi

@op(
    out=DynamicOut(),
    config_schema={
        'chunk_size': Field(int, default_value=10),
    },
)
def extract_items(context) -> Output:
    """
    extracting all the data
    ijson to stream in the data from the file without
        needing to load it all in memory at once
    pandas or regular json could be used to chunk one line at a time
        and do more work in parallel than an ijson stream but the
        provided file would not work without extra modification
    Dagster caches to disk, but all the ops need to be
        frontloaded so chunking the data would be better if speed
        is a priority

    file should come from an api endpoint rather than be stored locally
    datafile gzipped to reduce size
    """
    i = 0
    chunk_size = context.op_config['chunk_size']
    with gzip.open("pipeline/dataset.json.gz", "r") as raw_data:
        chunk = []
        for item in ijson.items(raw_data, "item"):
            # break after 100 for testing, remove for prod
            if i >= 100:
                break
            i += 1
            chunk.append(item)
            if len(chunk) == chunk_size:
                yield DynamicOutput(chunk, mapping_key=item["code"])
                chunk = []
        if chunk:
            yield DynamicOutput(chunk, mapping_key="tail")

@op(required_resource_keys={"mongodb"})
def dump_piece(context, piece: list):
    """
    Upsert the data into mongodb
    runs faster with more dagster workers
    bulk writes are more efficient, can change the size in the
    complex transformations (e.g. if I had used a schema with Postgres)
        may want to have transform as a dedicated op instead
    Possible race conditions with db writes

    needs error checking, logging, and unit testing
    """
    def transform(item: dict) -> tuple:
        """
        Shift old _id into oid
        return tuple of code (new id) and rest of item
        """
        oid = item.pop("_id", None)
        code = item.pop("code")
        if oid:
            item["oid"] = oid
        item = convert_decimal(item)
        return {"_id": code}, item

    def convert_decimal(dict_item) -> dict:
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
    updates = []
    for item in piece:
        pid, t_item = transform(item)
        updates.append(UpdateOne(pid, {"$set": t_item}, upsert=True))
    context.resources.mongodb.veryfi.bulk_write(updates)


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
