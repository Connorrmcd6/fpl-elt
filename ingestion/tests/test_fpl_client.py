import pytest
import requests
import json
from src.fpl_client import FPLClient


@pytest.fixture
def mock_bootstrap_data():
    """Provides a mock of the bootstrap-static API response from a file."""
    mock_file_path = "tests/mock_responses/bootstrap_response.json"
    with open(mock_file_path, "r") as f:
        return json.load(f)


@pytest.fixture
def mock_fixtures_data():
    """Provides a mock of the fixtures API response from a file."""
    mock_file_path = "tests/mock_responses/fixture_response.json"
    with open(mock_file_path, "r") as f:
        return json.load(f)


def test_get_bootstrap_data_caching(mocker, mock_bootstrap_data):
    """
    Tests that bootstrap data is fetched once and cached on subsequent calls.
    """
    # Setup mock for requests.Session.get
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = mock_bootstrap_data
    mock_get = mocker.patch("requests.Session.get", return_value=mock_response)

    client = FPLClient()

    # First call - should trigger API call
    data1 = client._get_bootstrap_data()
    # Second call - should use cache
    data2 = client._get_bootstrap_data()

    # Assertions
    mock_get.assert_called_once_with(f"{client.BASE_URL}bootstrap-static/")
    mock_response.raise_for_status.assert_called_once()
    assert data1 == mock_bootstrap_data
    assert data2 == data1  # Should be the same object from cache


def test_get_bootstrap_components(mocker, mock_bootstrap_data):
    """
    Tests that the correct components are extracted from the bootstrap data.
    """
    mocker.patch("requests.Session.get").return_value.json.return_value = (
        mock_bootstrap_data
    )
    client = FPLClient()

    keys = ["chips", "events", "teams", "elements", "element_types", "element_stats"]
    components = client.get_bootstrap_components(keys)

    assert list(components.keys()) == keys
    assert components["teams"] == mock_bootstrap_data["teams"]
    assert components["events"] == mock_bootstrap_data["events"]
    assert components["chips"] == mock_bootstrap_data["chips"]
    assert components["elements"] == mock_bootstrap_data["elements"]
    assert components["element_types"] == mock_bootstrap_data["element_types"]
    assert components["element_stats"] == mock_bootstrap_data["element_stats"]


def test_get_fixtures(mocker, mock_fixtures_data):
    """
    Tests that the fixtures endpoint is called correctly.
    """
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = mock_fixtures_data
    mock_get = mocker.patch("requests.Session.get", return_value=mock_response)

    client = FPLClient()
    fixtures = client.get_fixtures()

    mock_get.assert_called_once_with(f"{client.BASE_URL}fixtures/")
    mock_response.raise_for_status.assert_called_once()
    assert fixtures == mock_fixtures_data
