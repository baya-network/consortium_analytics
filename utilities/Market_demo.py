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
        market_rent_query = "SELECT * \
                                FROM leases_ck AS lea \
                                JOIN ck_to_ry AS mt on mt.ck_id = lea.property_id \
                                JOIN (SELECT address, rsf, reonomy_id FROM properties_ry) as ryrsf ON ryrsf.reonomy_id = mt.ry_id \
                                WHERE mt.ry_id IN {} \
                                AND lea.expiration_date > '{}' \
                                AND lea.commencement_date <= '{}'\
                                ".format(tuple([str(b) for b in self.bldgs_ids]), today, today)
        self.current_leases = pd.read_sql(market_rent_query, con=self.db_engine)
        # self.current_leases.current_rent.mask(self.current_leases.current_rent < 3, np.nan, inplace=True)
        # self.current_leases.effective_rent.mask(self.current_leases.effective_rent < 3, np.nan, inplace=True)
        ck_by = {
            "address": "Address",
            "id": "Lease ID",
            "suite": "Unit ID",
            "tenant_name": "Company Name",
            "floor_occupancies": "Floor",
            "transaction_size": "Size",
            "property_id": "Company ID",
            "execution_date": "Signing Date",
            "commencement_date": "Start Date",
            "expiration_date": "End Date",
            "starting_rent": "Starting Rate",
            "current_rent": "Current Rate",
            "avg_rent": "Average Rate",
            "asking_rent": "Asking Rate",
            "lease_escalations": "Rate Increase", 
            "break_option_dates": "Termination Dates",
            "break_option_type": "Termination Type",
            "renewal_options": "Extension Options",
            "sublease": "Subleased",
            "free_rent_type": "Concession Type",
            "work_value": "Concession Work Value"
        }
        self.current_leases = self.current_leases.loc[:, ck_by.keys()]
        self.current_leases.rename(ck_by, axis=1, inplace=True)
        return self.current_leases.loc[:, [u'Lease ID', "Address", u'Company ID', "Company Name", u'Floor', u'Size', u'Unit ID',
                                                u'Starting Rate', u'Current Rate',  u'Average Rate',
                                                u'Signing Date', u'Start Date', u'End Date', u'Subleased',
                                                u'Extension Options', u'Termination Type', u'Termination Dates',
                                                u'Asking Rate', u'Rate Increase',
                                                u'Concession Type', u'Concession Work Value'
                                                ]]

    def get_mean_rent(self):
        if not hasattr(self, 'current_leases'):
            self.get_current_leases()
        self.market_rent = self.current_leases.current_rent.mean()
        return self.market_rent
        







    


        


    



