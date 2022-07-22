from AECDatabase import AECDatabase
from dotenv import dotenv_values
import sys, os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

class AECHistorical(AECDatabase):
    def __init__(self):
        config = dotenv_values(".\.env")
        db_user = os.environ['DB_USER']
        db_pass = os.environ['DB_PASS']
        db_port = int(os.environ['DB_PORT'])
        db_name = os.environ['DB_NAME']
        db_host = os.environ['DB_HOST']
        self.setup_connection(db_user, db_pass, db_host, db_port, db_name)
        self.site_id = int(sys.argv[1])
        self.site_data = self.get_site_data()
        self.current_level = float(sys.argv[2])
        self.pumped_flow = float(sys.argv[3])
        self.suction_pressure = float(sys.argv[4])
        self.calculate_historical()
        self.insert_suction_pressure(self.suction_pressure)

    def calculate_historical(self):
        buffer_data = self.last_historical_buffer()

        # Check if this is the first time we are running this
        if(len(buffer_data) == 0):
            # If so insert the data and return
            self.insert_buffer(self.pumped_flow, self.current_level)
            return

        # If not, calculate the historical
        start_level = float(buffer_data[0]["Level"])
        sample_level = self.current_level
        sample_flow = self.pumped_flow

        hourly_pumped = sample_flow * 1800
        diff = sample_level - start_level
        net_gain = diff*self.site_data["SurfaceArea"]
        diff_litres = abs(net_gain*1000)

        # Calculate the historical
        outlet = 0
        if(diff_litres >= 0):
            outlet = hourly_pumped-abs(diff_litres)
        else: 
            outlet = hourly_pumped+diff_litres

        outlet = abs(outlet / 1800)
        
        # Don't let the output go below 0
        if(outlet < 0):
            outlet = 0
            
        # Insert the data
        self.insert_buffer(sample_flow, sample_level)
        self.insert_historical(outlet)
        return

AECHistorical()