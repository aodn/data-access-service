import pytest

from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.batch.subsetting.helpers.request_helper import (
    get_subset_request,
)
from data_access_service.models.estimate_size_request import EstimateSizeRequest


def _batch_parameters(raw_key: str | None) -> dict:
    """The batch job parameters for the same portal request."""
    parameters = {
        Parameters.UUID.value: "b2548767-514f-4a31-b65e-36bb894382d5",
        Parameters.START_DATE.value: "01-2020",
        Parameters.END_DATE.value: "02-2020",
        Parameters.RECIPIENT.value: "someone@example.com",
        Parameters.MULTI_POLYGON.value: None,
        Parameters.OUTPUT_FORMAT.value: "csv",
    }
    if raw_key is not None:
        parameters[Parameters.KEY.value] = raw_key
    return parameters


class TestGetKeys:
    """`key` is one comma-separated string, the same wire form the batch
    download job takes."""

    def test_single_key(self):
        body = EstimateSizeRequest(key="animal_metadata.parquet")
        assert body.get_keys() == ["animal_metadata.parquet"]

    def test_comma_separated_keys(self):
        body = EstimateSizeRequest(key="a.parquet,b.parquet")
        assert body.get_keys() == ["a.parquet", "b.parquet"]

    def test_surrounding_spaces_are_stripped(self):
        body = EstimateSizeRequest(key=" a.parquet , b.parquet ")
        assert body.get_keys() == ["a.parquet", "b.parquet"]

    def test_absent_key_means_all_keys(self):
        # ["*"] is what resolve_keys expands to the whole dataset.
        assert EstimateSizeRequest().get_keys() == ["*"]

    def test_star_is_passed_through(self):
        assert EstimateSizeRequest(key="*").get_keys() == ["*"]

    def test_list_is_rejected(self):
        # The old body sent keys as a list; it must fail loudly rather than
        # fall back to "all keys".
        with pytest.raises(ValueError):
            EstimateSizeRequest(key=["a.parquet"])


class TestSameWireFormatAsBatchDownload:
    """Both paths carry the same portal request, so they must read it the same."""

    @pytest.mark.parametrize(
        "raw",
        ["a.parquet", "a.parquet,b.parquet", " a.parquet , b.parquet ", "*", None],
    )
    def test_key_parsing_matches_the_batch_job(self, raw):
        # Through the real batch entry point, so the two cannot drift apart.
        assert (
            EstimateSizeRequest(key=raw).get_keys()
            == get_subset_request(_batch_parameters(raw)).keys
        )

    def test_field_names_match_the_batch_parameter_names(self):
        shared = {
            Parameters.KEY.value,
            Parameters.START_DATE.value,
            Parameters.END_DATE.value,
            Parameters.MULTI_POLYGON.value,
            Parameters.OUTPUT_FORMAT.value,
        }
        fields = set(EstimateSizeRequest.model_fields)
        assert shared <= fields
        # Anything left is estimation-only, not a renamed batch parameter.
        assert fields - shared == {"columns"}


class TestOtherFields:
    def test_unknown_fields_are_ignored(self):
        # The portal posts uuid in the body too, though it is a path param.
        body = EstimateSizeRequest(
            **{"uuid": "b2548767", "key": "a.parquet", "output_format": "csv"}
        )
        assert body.get_keys() == ["a.parquet"]
        assert body.output_format == "csv"

    def test_defaults(self):
        body = EstimateSizeRequest()
        assert body.start_date == "non-specified"
        assert body.end_date == "non-specified"
        assert body.output_format == "netcdf"
        assert body.columns is None
        assert body.multi_polygon is None
