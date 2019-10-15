import os
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd, geopandas as gpd, numpy as np
from sqlalchemy import create_engine
import folium

class Building:
    """ Building Class with utility to query all data related to a building, plus most common queries (active leases, active listings, 
    mean building rent, upcoming lease expirations, etc.) along with information about buildings & tenants nearby. """
    # TO-DO: check that, and respond appropriately, if the dfs are empty. # self.current_availabilities = result if not result.empty else None

    def __init__(self, ry_id):
        self.ry_id = ry_id
        # Connection to DB, fixed for now.Connecting to our main DB. TO-DO. Connect to follow-up test DB.
        self.db_engine = create_engine(os.environ["DATABASE_URL_silhouetted"])
        self.set_bldg_metadata()
        
    
    def set_bldg_metadata(self):
        try:
            bldg_basics_query = "SELECT address, address_city, neighborhood, zipcode, address_state, rsf, location, year_built, year_renovated, perc_known, perc_vacant, perc_occupied \
                                    FROM properties_ry \
                                    WHERE reonomy_id = '{}'".format(self.ry_id)
            basics = gpd.GeoDataFrame.from_postgis(bldg_basics_query, con=self.db_engine, geom_col='location')
            self.address = basics.loc[0, 'address']
            self.city = basics.loc[0, 'address_city']
            self.neighborhood = basics.loc[0, 'neighborhood']
            self.zipcode = basics.loc[0, 'zipcode']
            self.state = basics.loc[0, 'address_state']
            self.rsf = basics.loc[0, 'rsf']
            self.year_built = basics.loc[0, 'year_built']
            self.year_renovated = basics.loc[0, 'year_renovated']
            self.location = basics.loc[0, 'location'] if not pd.isnull(basics.loc[0, 'location']) else np.nan
            self.perc_vacant = basics.loc[0, 'perc_vacant'] if not pd.isnull(basics.loc[0, 'perc_vacant']) else 0
            self.perc_occupied = basics.loc[0, 'perc_occupied'] if not pd.isnull(basics.loc[0, 'perc_occupied']) else 0
            self.perc_known = basics.loc[0, 'perc_known'] if not pd.isnull(basics.loc[0, 'perc_known']) else 0
            print(self.address, self.city, self.zipcode, self.state, self.rsf, self.year_built, self.year_renovated, self.ry_id)
        except Exception as e:
            print("Error when setting bldg metadata: " + str(e))

    
    def get_ry_data(self):
        ry_data_query = "SELECT * FROM properties_ry WHERE reonomy_id = '{}'".format(self.ry_id)
        self.ry_data = gpd.GeoDataFrame.from_postgis(ry_data_query, con=self.db_engine, geom_col='location').transpose()
        self.ry_data.columns = ['characteristics']
        return self.ry_data
    
    def get_financials(self):
        if not hasattr(self, 'ry_data'):
            self.get_ry_data()
        return self.ry_data.iloc[34:51, :].append(self.ry_data.iloc[81:89, :])
    
    def get_contacts(self):
        if not hasattr(self, 'ry_data'):
            self.get_ry_data()
        return self.ry_data.iloc[49:81, :]
            

    
    def get_f42_bldg_data(self):
        f42_bldg_query = "SELECT * FROM properties_f42 AS f42 \
                          JOIN f42_to_ry AS mt ON mt.f42_id = f42.property_id\
                          WHERE ry_id = '{}'".format(self.ry_id)
        self.f42_bldg_data = pd.read_sql(f42_bldg_query, con=self.db_engine).transpose()
        return self.f42_bldg_data


    def get_f42_sales_data(self):
        f42_sales_query = "SELECT * FROM listings_f42 AS lis \
                          JOIN f42_to_ry AS mt ON mt.f42_id = lis.property_id \
                          WHERE ry_id = '{}' \
                          AND lis.type = 'Sale'".format(self.ry_id)
        self.f42_sales_data = pd.read_sql(f42_sales_query, con=self.db_engine).transpose()
        return self.f42_sales_data


    def get_all_leases(self):
        all_leases_query = "SELECT * FROM leases_ck AS lea \
                            JOIN ck_to_ry AS mt ON mt.ck_id = lea.property_id \
                            WHERE ry_id = '{}'".format(self.ry_id)
        self.all_leases = pd.read_sql(all_leases_query, con=self.db_engine)
        return self.all_leases
    

    def get_current_leases(self):
        # TO-DO: optimize if all leases are queried, don't make another DB call for the current ones
        # TO-DO: if all leases is an empty df, there are no leases for bldg, don't make call
        try:
            today = dt.datetime.today().strftime('%Y-%m-%d')
            current_leases_query = "SELECT * FROM leases_ck AS lea \
                                JOIN ck_to_ry AS mt ON mt.ck_id = lea.property_id \
                                WHERE ry_id = '{}' \
                                AND lea.expiration_date > '{}' \
                                AND lea.commencement_date <= '{}'".format(self.ry_id, today, today)
            self.current_leases = pd.read_sql(current_leases_query, con=self.db_engine)

            # self.current_leases.loc[:, "perc_of_bldg_size"] = self.current_leases["transaction_size"].multiply(100).divide(self.rsf))
            # self.current_leases.current_rent.mask(self.current_leases.current_rent < 3, np.nan, inplace=True)
            # self.current_leases.effective_rent.mask(self.current_leases.effective_rent < 3, np.nan, inplace=True)

            return self.current_leases.loc[:, ['tenant_name', 'transaction_size', 'submarket', 'floor_occupancies', 'suite', \
                                                'current_rent', 'effective_rent', 'expiration_date', 'commencement_date', 'space_type']]
        except AttributeError as attr_error:
            return "Error while getting current leases: " + str(attr_error) + ". Try calling get_ry_data() first."
    

    def get_all_vacancies(self):
        all_vacancies_query = "SELECT * FROM listings_f42 AS lis \
                                    JOIN f42_to_ry AS mt ON mt.f42_id = lis.property_id \
                                    WHERE ry_id = '{}' \
                                    AND lis.type = 'Lease'".format(self.ry_id)
        self.all_vacancies = pd.read_sql(all_vacancies_query, con=self.db_engine)
        return self.all_vacancies
    

    def get_current_vacancies(self):
        try:
            still_vacant_assumption_date = (dt.datetime.today() + relativedelta(months= -6)).strftime('%Y-%m-%d')
            current_vacancies_query = "SELECT * FROM listings_f42 AS lis \
                                            JOIN f42_to_ry AS mt on mt.f42_id = lis.property_id \
                                            WHERE ry_id = '{}' \
                                            AND lis.type = 'Lease' \
                                            AND lis.touched_at > '{}'".format(self.ry_id, still_vacant_assumption_date)
            self.current_vacancies = pd.read_sql(current_vacancies_query, con=self.db_engine)
            
            self.current_vacancies.loc[:, "perc_of_bldg_size"] = self.current_vacancies["size"].multiply(100).divide(self.rsf)
            return self.current_vacancies.loc[:, ['floor', 'floor_order', 'unit', 'unit_type', 'size', 'perc_of_bldg_size', 'rate_per_sqft_per_year', 'details', 'touched_at', 'lease_expiration']]
        except AttributeError as attr_error:
            return "Error while getting current leases: " + str(attr_error) + ". Try calling get_ry_data() first."


    ######################################################## Baya Layers ##################################################
    
    def get_upcoming_vacancies(self, months=12):
        if not hasattr(self, 'current_leases'):
            self.get_current_leases()
        timeframe = (dt.datetime.today() + relativedelta(months= months)).date()
        return self.current_leases.loc[self.current_leases.expiration_date <= timeframe, ['tenant_name', 'transaction_size', 'submarket', 'floor_occupancies', 'suite', 'perc_of_bldg_size', \
                                            'current_rent', 'effective_rent', 'expiration_date', 'commencement_date', 'space_type']]


    def get_mean_rent(self):
        if not hasattr(self, 'current_leases'):
            self.get_current_leases()
        if self.current_leases.empty:
            return 0
        else:
            return self.current_leases.loc[:,  "current_rent" ].mean()

    
    def get_estimated_revenue(self, _rent, occupied_from_unknown = 0.75):
        # occupied_from_unknown is the fraction of unknwon sqft that is leased at market rent from the unknown sq footage.
        if not hasattr(self, 'current_leases'):
            self.get_current_leases()
        # revenue from known leases
        if self.current_leases.empty:
            return 0

        # bldg_mean_rent = self.get_mean_rent()
        
        rkl = self.current_leases.transaction_size.mul(self.current_leases.current_rent.fillna(_rent)).sum()
        if self.perc_known >= 100:
            return rkl
  
        # revenue estimated from unknown square footage
        ## Using mean bldg rent for now. Can be improved (?) by using mean market rent
        rusf = 0
        rusf = self.rsf * (1 - self.perc_known/100)*occupied_from_unknown * _rent
        return rkl + rusf


    def get_knotel_revenue_increase(self, _rent, vacant_from_unknown=0.25):
        if self.perc_known == 0:
            return 0
        
        if self.perc_known >= 100:
            # total estimated vacant space, from availabilities (F42)
            tevs =  self.rsf * self.perc_vacant/100
        else:
            # total estimated vacant space, from availabilities (F42) and unknown
            tevs =  self.rsf * (self.perc_vacant/100 + (1 - self.perc_known/100) * vacant_from_unknown)       
        
        # Getting revenue from Knotel filling vacant space at Market Rent
        # market_rent = market.get_mean_rent()
        # revenue form current vacancies
        rcv = tevs * _rent
        return rcv
    
    
    def get_surrounding_bldgs(self, radius=0.5):
        """ For now the Area is defined as a circle centered at the bldg which method is called upon. Radius is a parameter. 
        TO-DO: allow for Area to be a geojson, either pre-loaded or passed as argument. Area can then be a market, submarket defined by user.
        TO-DO: currently restricted to Manhattan, relax this constraint moving on. Also restricting to Office Category from Reonomy denomination. """
        try:
            radius = radius*1600         # miles to meters conversion
            bldgs_in_area_query = "SELECT reonomy_id FROM properties_ry AS ry \
                                        WHERE ST_DWithin(ry.location, ST_SetSRID(ST_Point({}, {}), 4326), {}) \
                                        AND address_city = 'MN' \
                                        AND reonomy_id != '{}' \
                                        AND category = 'Office'".format(self.location.x, self.location.y, radius, self.ry_id)
            result = pd.read_sql(bldgs_in_area_query, con=self.db_engine)
            return [x[0] for x in result.values.tolist()]
            
        except AttributeError as attr_error:
            return "Error while getting bldgs in area: " + str(attr_error) + ". Try calling get_ry_data() first."
        
    
    def show_location(self):
        try:
            test_map = folium.Map(location=[self.location.y, self.location.x], zoom_start=16)

            obj_point = folium.Marker(location = (self.location.y, self.location.x), tooltip=self.address)
            obj_point.add_to(test_map)
            display(test_map)
        except Exception as e:
            print("Error while displaying bldg on map.\n{}".format(e))

    
    def show_surrounding_locations(self, surr_ids):
        try:
            test_map = folium.Map(location=[self.location.y, self.location.x], zoom_start=16)
            obj_point = folium.Marker(location = (self.location.y, self.location.x), tooltip=self.address)
            obj_point.add_to(test_map)
            for b in surr_ids:
                B = Building(b)
                folium.Circle(radius=20,
                                location=[B.location.y, B.location.x],
                                tooltip=B.address,
                                color='crimson',
                                fill=True
                            ).add_to(test_map)
            display(test_map)
        except Exception as e:
            print("Error while displaying bldg on map.\n{}".format(e))
     







    


        


    



