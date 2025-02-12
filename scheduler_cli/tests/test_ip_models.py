import pytest
from scheduler_cli.models import IpOrderAndForecastData, IpToLonAndLat


class TestIpOrderAndForecastData:

    @pytest.fixture(autouse=True)
    def setup(self):
        # "129.114.108.207": {"carbon_intensity": 277, "lat": 30.3912, "lon": -97.7218}
        self.test_ip_lat_long = IpToLonAndLat("129.114.108.207", "30.3912", "-97.7218")
        self.test_obj = IpOrderAndForecastData(self.test_ip_lat_long)

    def test_init(self):
        assert self.test_obj.ipCoordinate is not None

    def test_load_forecast(self):
        assert len(self.test_obj.forecast_list) == 0
        self.test_obj.create_and_populate_forecast("")
        assert len(self.test_obj.forecast_list) > 0

    def test_average_empty_and_non_empty(self):
        ci_avg = self.test_obj.average()
        assert ci_avg == 0.0
        self.test_obj.create_and_populate_forecast("")
        ci_avg = self.test_obj.average()
        assert ci_avg > 0.0
