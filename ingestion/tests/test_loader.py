import pytest
import json
from src import loader


@pytest.mark.parametrize(
    "data, id_field, expected_count",
    [
        ([{"id": 1, "val": "a"}, {"id": 2, "val": "b"}], "id", 2),
        ([{"code": 10}], "id", 1),  # Test hashing when id_field is missing
        ([], "id", 0),
        (None, "id", 0),
    ],
)
def test_load_raw(mocker, data, id_field, expected_count):
    """
    Tests the load_raw function with various inputs.
    """
    # 1. Setup
    mock_ch_client = mocker.MagicMock()
    table_name = "test_table"

    # 2. Execution
    loader.load_raw(data, mock_ch_client, table_name, id_field=id_field)

    # 3. Assertions
    if expected_count > 0:
        mock_ch_client.insert.assert_called_once()
        call_args = mock_ch_client.insert.call_args

        # Check table name and column names
        assert call_args.args[0] == f"raw.{table_name}"
        assert call_args.kwargs["column_names"] == ["id", "raw_data"]

        # Check that the number of rows inserted is correct
        inserted_rows = call_args.args[1]
        assert len(inserted_rows) == expected_count

        # Check content of the first row
        first_row_id = inserted_rows[0][0]
        first_row_data = json.loads(inserted_rows[0][1])

        if id_field in data[0]:
            assert first_row_id == data[0][id_field]

        assert first_row_data == data[0]

    else:
        # Assert that insert is NOT called for empty data
        mock_ch_client.insert.assert_not_called()


def test_load_raw_generates_hash_for_missing_id(mocker):
    """
    Tests that a hash is generated for the ID if the id_field is not present.
    """
    mock_ch_client = mocker.MagicMock()
    data = [{"name": "test_item"}]  # No 'id' field

    loader.load_raw(data, mock_ch_client, "test_table", id_field="id")

    mock_ch_client.insert.assert_called_once()
    inserted_rows = mock_ch_client.insert.call_args.args[1]

    # The generated ID should be a large integer hash, not the original data
    assert isinstance(inserted_rows[0][0], int)
    assert inserted_rows[0][0] != data[0]
