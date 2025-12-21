import requests
from typing import List, Dict, Any


class FPLClient:
    BASE_URL = "https://fantasy.premierleague.com/api/"

    def __init__(self):
        self.session = requests.Session()
        self._bootstrap_data = None

    def _get_bootstrap_data(self) -> Dict[str, Any]:
        """
        Fetches and caches the bootstrap-static data.
        The data is fetched only on the first call per client instance.
        """
        if self._bootstrap_data is None:
            response = self.session.get(f"{self.BASE_URL}bootstrap-static/")
            response.raise_for_status()
            self._bootstrap_data = response.json()
        return self._bootstrap_data

    def get_players(self) -> List[Dict[str, Any]]:
        """Returns the 'elements' (players) from the bootstrap data."""
        data = self._get_bootstrap_data()
        return data["elements"]

    def get_player_types(self) -> List[Dict[str, Any]]:
        """Returns the 'element_types' (positions) from the bootstrap data."""
        data = self._get_bootstrap_data()
        return data["element_types"]

    def get_chips(self) -> List[Dict[str, Any]]:
        """Returns the 'chips' from the bootstrap data."""
        data = self._get_bootstrap_data()
        return data["chips"]

    def get_gameweeks(self) -> List[Dict[str, Any]]:
        """Returns the 'events' (gameweeks) from the bootstrap data."""
        data = self._get_bootstrap_data()
        return data["events"]

    def get_teams(self) -> List[Dict[str, Any]]:
        """Returns the 'teams' from the bootstrap data."""
        data = self._get_bootstrap_data()
        return data["teams"]

    def get_bootstrap_components(self, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of specified components from the bootstrap data.
        Valid keys include: 'events', 'teams', 'elements', 'element_types', etc.
        """
        data = self._get_bootstrap_data()
        return {key: data[key] for key in keys if key in data}

    def get_fpl_data(self) -> Dict[str, Any]:
        """Returns the entire bootstrap-static response."""
        return self._get_bootstrap_data()

    def get_fixtures(self) -> List[Dict[str, Any]]:
        response = self.session.get(f"{self.BASE_URL}fixtures/")
        response.raise_for_status()
        return response.json()
