"""Collection adds the subset request to dataschema.json before the download."""

import io
import json
from unittest.mock import MagicMock

from data_access_service.batch.subsetting.tasks.parquet_collector import (
    _add_subset_to_dataschema,
    subset_dataschema_entries,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import SubsetRequest


def _request() -> SubsetRequest:
    return SubsetRequest(
        uuid="test-uuid",
        keys=["data.parquet"],
        start_date="2020-01-01",
        end_date="2020-01-31",
        recipient="user@example.com",
        output_format="csv",
        multi_polygon='{"type":"MultiPolygon","coordinates":[]}',
        bboxes=[BoundingBox(10, 20, 30, 40)],
        suggested_citation="Cite this dataset.",
    )


def test_entries_carry_the_request_fields():
    entries = subset_dataschema_entries(_request())
    assert entries["suggested_citation"] == "Cite this dataset."
    assert entries["start_date"] == "2020-01-01"
    assert entries["end_date"] == "2020-01-31"
    assert entries["bboxes"] == [
        {"min_lon": 10, "min_lat": 20, "max_lon": 30, "max_lat": 40}
    ]
    assert entries["multi_polygon"] == '{"type":"MultiPolygon","coordinates":[]}'


def test_add_subset_keeps_column_metadata_and_writes_the_request():
    aws = MagicMock()
    existing = {"TEMP": {"type": "float", "long_name": "Temperature"}}
    aws.s3.get_object.return_value = {
        "Body": io.BytesIO(json.dumps(existing).encode("utf-8"))
    }

    _add_subset_to_dataschema(aws, "bucket", "job", _request())

    file_obj, bucket, key = aws.upload_fileobj_to_s3.call_args.args
    assert (bucket, key) == ("bucket", "job/temp/dataschema.json")
    written = json.loads(file_obj.getvalue())
    assert written["TEMP"] == existing["TEMP"]
    assert written["suggested_citation"] == "Cite this dataset."
    assert written["start_date"] == "2020-01-01"
    assert written["end_date"] == "2020-01-31"
    assert written["bboxes"] == [
        {"min_lon": 10, "min_lat": 20, "max_lon": 30, "max_lat": 40}
    ]
    assert written["multi_polygon"] == '{"type":"MultiPolygon","coordinates":[]}'


def test_add_subset_starts_from_empty_when_schema_is_missing():
    aws = MagicMock()
    no_such_key = type("NoSuchKey", (Exception,), {})
    aws.s3.exceptions.NoSuchKey = no_such_key
    aws.s3.get_object.side_effect = no_such_key()

    _add_subset_to_dataschema(aws, "bucket", "job", _request())

    written = json.loads(aws.upload_fileobj_to_s3.call_args.args[0].getvalue())
    assert set(written) == {
        "suggested_citation",
        "start_date",
        "end_date",
        "bboxes",
        "multi_polygon",
    }
