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
            self.city = city
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
            
        
    def _search_address(self, addss):
        base_url_nominatim = "https://nominatim.openstreetmap.org/search"
        try:
            params = {"q": addss,
                      "format":"json",
                      "polygon_geojson":1,
                      "addressdetails":1,
                      "countrycodes":'{}'.format(self.country)}
            r = requests.get(base_url_nominatim, params=params)
            return r.json()[0]
        except IndexError:
            print("Address not found.\nMake sure the address ({}) belongs to {}, {}".format(addss, self.city.capitalize(), self.country.upper()))
        except Exception as e:
            print("Error while searching for addresses: {}.\n{}".format(addss, e))
            return np.nan
    
    
    def _get_closest_bldg(self, obj):
        df = self.bldg_data
        try:
            gj = obj['geojson']
            if (gj['type'] == 'Point'):
                closest_bldg = df[df.geometry.intersects(Point(gj['coordinates']))]

                if not closest_bldg.empty:
                    print("pin inside bldg")
                    return closest_bldg
                else:
                    print("pin outside, used closest bldg")
                    closest_bldg = df.loc[df.geometry.distance(Point(gj['coordinates'])).sort_values(ascending=True)[:1].index, :]
                    return closest_bldg
            else:
                print("poly: centroid to centroid closest bldg")
                closest_bldg = df.loc[df.centroid.distance(Point(float(obj['lon']), float(obj['lat']))).sort_values(ascending=True)[:1].index, :]
                return closest_bldg
        except Exception as e:
            print("Error while matching address to building: {}.\n{}".format(e))
            return np.nan
    
    
    def _create_text_box(self, obj, dfdict):
        pophtml = """
            <h3> {title} </h3>
            <b>Baya ID:</b> {bid}<br>
            Country: {country}<br>
            """.format(title=', '.join(obj['display_name'].split(',')[:3]), 
                       bid=dfdict['id'], country=obj['address']['country'])
        for k,v in dfdict.items():
            if ('id' not in k) and ('country' not in k):
                pophtml = pophtml + "{}: {}<br>".format(k,v)
        return pophtml    
    

    def find(self, addss):
        obj = self._search_address(addss)
        closest_bldg = self._get_closest_bldg(obj)
        pophtml = self._create_text_box(obj, closest_bldg.iloc[:, :-3]\
                                     .drop('geo', axis=1).dropna(axis=1).to_dict(orient='rows')[0])
        try:
            # creates map
            bldg_poly = None
            gj = obj['geojson']
            address = obj['address']

            test_map = folium.Map(location=[float(obj['lat']), float(obj['lon'])], zoom_start=16)

            obj_point = folium.Marker(location = (float(obj['lat']), float(obj['lon'])), color='red')
            obj_point.add_to(test_map)


            if (gj['type'] == 'Point'):
                # delineates the surrounding/closest bldg polygon
                bldg_poly = folium.Polygon(locations = closest_bldg.geo_inv.values.tolist(),
                                           color="red", fill=True, fill_color='#FF0000',
                                           tooltip=folium.Tooltip(', '.join(obj['display_name'].split(',')[:3])),
                                           popup=folium.Popup(pophtml, max_width=300))
                bldg_poly.add_to(test_map)

            if (gj['type'] == 'Polygon' and not bldg_poly):
                obj_poly = folium.Polygon(locations = [[ (a[1],a[0]) for a in b] for b in gj['coordinates']], 
                                          color="red", fill=True, fill_color='#FF0000',
                                          tooltip=folium.Tooltip(', '.join(obj['display_name'].split(',')[:3])),
                                          popup=folium.Popup(pophtml, max_width=300))
                obj_poly.add_to(test_map)

            display(test_map)
            
        except Exception as e:
            print("Error while displaying bldg on map.\n{}".format(e))
    

        
        