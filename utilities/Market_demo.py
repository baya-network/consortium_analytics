import os
import datetime as dt
import pandas as pd, geopandas as gpd, numpy as np
from sqlalchemy import create_engine

class Market:
    """ Market Class with utility functions to get interesting information for the set of bldgs that conform the market."""
    # TO-DO: 

    def __init__(self, bldgs_ids):
        self.bldgs_ids = bldgs_ids
        # Connection to DB, fixed for now. Connecting to our main DB. TO-DO. Connect to follow-up test DB.
        self.db_engine = create_engine(os.environ["DATABASE_URL_silhouetted"])
    
    def get_current_leases(self):
        today = dt.datetime.today().strftime('%Y-%m-%d')
        market_rent_query = "SELECT mt.ry_id, ryrsf.address as bldg_address, lea.tenant_name, lea.transaction_size as size, lea.current_rent, lea.effective_rent, lea.commencement_date, lea.expiration_date, perc_occupied, perc_vacant, perc_known \
                                FROM leases_ck AS lea \
                                JOIN ck_to_ry AS mt on mt.ck_id = lea.property_id \
                                JOIN (SELECT address, rsf, reonomy_id, perc_known, perc_occupied, perc_vacant FROM properties_ry) as ryrsf ON ryrsf.reonomy_id = mt.ry_id \
                                WHERE mt.ry_id IN {} \
                                AND lea.expiration_date > '{}' \
                                AND lea.commencement_date <= '{}'\
                                ".format(tuple([str(b) for b in self.bldgs_ids]), today, today)
        self.current_leases = pd.read_sql(market_rent_query, con=self.db_engine)
        self.current_leases.current_rent.mask(self.current_leases.current_rent < 3, np.nan, inplace=True)
        self.current_leases.effective_rent.mask(self.current_leases.effective_rent < 3, np.nan, inplace=True)
        return self.current_leases

    def get_mean_rent(self):
        if not hasattr(self, 'current_leases'):
            self.get_current_leases()
        self.market_rent = self.current_leases.current_rent.mean()
        return self.market_rent
        







    


        


    



