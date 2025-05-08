from typing import List

import click
import geopandas as gpd
import pandas as pd
from models import IpToLonAndLat
from rich.console import Console


class HistoricalForecastService:

    def __init__(self, forecast_start_utc, forecast_days, path_geojson, df_ci_path):
        self.forecast_start = pd.to_datetime(forecast_start_utc, utc=True)
        self.forecast_end = self.forecast_start + pd.Timedelta(days=forecast_days)
        self.geo_df = gpd.read_file(path_geojson)
        self.console = Console()
        self.df_ci = pd.read_csv(df_ci_path, index_col=False)
        self.df_ci['datetime'] = pd.to_datetime(self.df_ci['datetime'], utc=True)

    def ips_to_forecasts(self, ip_list: List[IpToLonAndLat]) -> gpd.GeoDataFrame:
        points = []
        for ipPojo in ip_list:
            points.append({
                'ip': ipPojo.ip,
                'lon': ipPojo.lon,
                'lat': ipPojo.lat
            })
        gdf_points = gpd.GeoDataFrame(
            points,
            geometry=gpd.points_from_xy([p['lon'] for p in points], [p['lat'] for p in points]),
            crs="EPSG:4326"  # Standard lon/lat CRS
        )
        ip_zoneName = gpd.sjoin(
            gdf_points,
            self.geo_df,
            how='left',
            predicate='within'
        ).drop_duplicates('ip')

        ip_zoneInfo = ip_zoneName[['ip', 'lon', 'lat', 'geometry', 'zoneName']]

        forecast_window = self.df_ci[
            (self.df_ci['datetime'] >= self.forecast_start) & (self.df_ci['datetime'] <= self.forecast_end)]

        final_result = pd.merge(
            ip_zoneInfo,
            forecast_window[
                ['datetime', 'zone_id', 'ci', 'ci_lifecycle']],
            left_on='zoneName',
            right_on='zone_id',
            how='left'  # Keep all IPs even if no CI data exists
        )
        final_result['forecast_idx'] = final_result.groupby('ip').cumcount()
        return final_result
