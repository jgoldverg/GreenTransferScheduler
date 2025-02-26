import os.path

import click
import pandas as pd
import requests

from models import IpOrderAndForecastData, IpToLonAndLat


class ForecastCleaning:

    def __init__(self):
        self.forecast_path = "/workspace/config/associations_df.csv"
        self.load_in_forecast_file()

    def load_in_forecast_file(self):
        if os.path.exists(self.forecast_path):
            self.forecasts_df = pd.read_csv(self.forecast_path)




