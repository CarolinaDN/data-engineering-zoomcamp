import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/vivid-grammar-424416-a0-30034d58d88c.json"

bucket_name = 'mage-zoomcamp-carolina'
project_id = 'vivid-grammar-424416'

table_name = 'green_taxi'
root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    #partition on date

    table = pa.Table.from_pandas(data)

    # authorize env variable automatically
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["lpep_pickup_date"],
        filesystem=gcs
    )