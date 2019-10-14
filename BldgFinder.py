import pandas as pd, numpy as np
import requests
import geopandas
import folium
from shapely.geometry import Polygon, Point

class BldgFinder:
    
    city_to_country_mapper = {"berlin": "de", "london": "gb",
                              "amsterdam": "nl", "dublin": "ie",
                              "paris": "fr"}
    
    def __init__(self, city):
        try:
            self.country = self.city_to_country_mapper[city.lower()]
        except KeyError as e:
            print("City not available. Try one of: Berlin, London, Amsterdam, Dublin, or Paris.\n{}".format(e))
        
        self.bldg_data = self.get_bldg_data()
        self.bldg_data = self.make_data_geospatial(self.bldg_data)
        print("Retrieved {:,} bldgs in {}".format(self.bldg_data.shape[0], city.capitalize()))
        
        
    def get_bldg_data(self):
        try:
            comb = pd.read_csv("./data/de_gb_nl_ie_fr_bldgs.csv")
            comb = comb.loc[(comb.country == self.country)]
            return comb
        except KeyError as e:
            print("Error while importing building data for {}.\n{}".format(self.country, e))
            
            
    def make_data_geospatial(self, df):
        try:
            df.geo.update(df.geo.apply(eval))
            # Making a Geopandas from bldg data
            df.loc[:, "geometry"] = df.geo.apply(lambda x: Polygon(x['coordinates'][0]))
            gdf = geopandas.GeoDataFrame(df, geometry=df.geometry)
            ## Adding geo-inverted columns (for plotting with folium)
            gdf.loc[:, "geo_inv"] = gdf.geo.apply(lambda c: [ [(a[1],a[0]) for a in b] for b in c['coordinates']][0])
            ## Adding centroid column
            # required to look for closest polygon when address does not intersect
            gdf.loc[:, "centroid"] = gdf.geometry.centroid
            return gdf
        except Exception as e:
            print("Error while making bldg data geospatial.\n{}".format(e))
        