"""
Adaptive Efficiency Control (AEC)

The module will provide businesses with the possibility for saving money by implementing a strategic planned 24 hour pumping regime and deliver this in the best manner.
"""
import csv, datetime, time, itertools, os, json, sys
from dotenv import load_dotenv, find_dotenv
import cvxpy as cp
import numpy as np
from prettytable import PrettyTable
import pandas as pd
from AECDatabase import AECDatabase
from AECUtilities import AECUtilities
from AECExceptions import LevelTooLowError, LevelTooHighError, TargetNotSatisfiedError, MaxVolumeExceededError
load_dotenv(find_dotenv())

class AEC(AECDatabase, AECUtilities):
    CONST_SPEED = "Speed"
    """Constant string"""
    CONST_WKDAY = "Weekday"
    """Constant string"""
    CONST_WKEND = "Weekend"
    """Constant string"""
    CONST_HOURS = "Length"
    """Constant string"""
    CONST_DOW = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    """Constant array for days of week"""
    DB_USER = os.environ['DB_USER']
    """Environment variable for DB_USER"""
    DB_PASS = os.environ['DB_PASS']
    """Environment variable for DB_PASS"""
    DB_HOST = os.environ['DB_HOST']
    """Environment variable for DB_HOST"""
    DB_PORT = int(os.environ['DB_PORT'])
    """Environment variable for DB_PORT"""
    DB_NAME = os.environ['DB_NAME']
    """Environment variable for DB_NAME"""

    def __init__(self, current_level, site_id, pump_combo, debug):
        self.setup_connection(self.DB_USER, self.DB_PASS, self.DB_HOST, self.DB_PORT, self.DB_NAME)
        self.month = datetime.datetime.today().strftime("%B")[:3]
        self.day = self.CONST_DOW[datetime.datetime.today().weekday()]
        self.hour = 0#datetime.datetime.today().hour
        self.minute = 0#datetime.datetime.today().minute
        self.weekday = self.is_weekday()
        self.site_id = site_id
        self.site_data = self.get_site_data()
        self.cost_data = self.get_cost_data(self.site_data["CostType"], self.month)
        self.pump_data = self.get_pump_data(pump_combo)
        self.suctionAdjustment = bool(self.site_data["SuctionAdjustment"])

        # If suction adjustment enabled then we will calculate the suction adjustment factor and apply to self.pump_data
        if self.suctionAdjustment:
            latest_suction_pressure = float(self.get_latest_suction_pressure()[0]["Pressure"])
            
            # Loop self.pump_data and apply suction adjustment factor
            for i in range(len(self.pump_data)):
                factor = latest_suction_pressure / float(self.pump_data[i]["SuctionPressure"])
                self.pump_data[i]["Flow"] = float(self.pump_data[i]["Flow"]) * factor

        self.pump_combo = pump_combo
        self.time_data = self.get_tariff_data(self.site_data["TariffType"])
        self.mode = self.get_mode()
        self.all_data = []
        self.best_cost = 1000000000000000000000000000000
        self.best_volume = 0
        self.target = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet'])).sum()*1800
        self.current_level = current_level
        self.min_level = self.site_data["MinLevel"]
        self.max_level = self.site_data["MaxLevel"]
        self.SURFACE_AREA = self.site_data["SurfaceArea"]
        self.DEBUG = debug
        print(json.dumps(self.get_regime()))
        self.dev_debug()

    def slice_historical_data(self):
        historical = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet']))
        if self.hour == 0:
            historical_sliced = historical
        elif self.hour == 8:
            historical_sliced = pd.concat([historical[16:], historical[:16]])
        elif self.hour == 14:
            historical_sliced = pd.concat([historical[28:], historical[:28]])
        elif self.hour == 16:
            historical_sliced = pd.concat([historical[32:], historical[:32]])
        elif self.hour == 19:
            historical_sliced = pd.concat([historical[38:], historical[:38]])
        return historical_sliced   

    def dev_debug(self):
        """
        Developer debug table prints out a table of useful items for diagnosing problems such as:
        Target, site identification, mode, day, month, site limits, cost and total volume.
        """
        if self.DEBUG:
            t = PrettyTable(['Description', 'Value', 'Data Type'])
            t.add_row(['Site ID', self.site_id, type(self.site_id)])
            t.add_row(['Target (litres)', self.target, type(self.target)])
            t.add_row(['Mode', self.mode, type(self.mode)])
            t.add_row(['Hour', self.hour, type(self.hour)])
            t.add_row(['Month', self.month, type(self.month)])
            t.add_row(['Day', self.day, type(self.day)])
            t.add_row(['Weekday', self.weekday, type(self.weekday)])
            t.add_row(['Start Level', self.current_level, type(self.current_level)])
            t.add_row(['Min Level', self.min_level, type(self.min_level)])
            t.add_row(['Max Level', self.max_level, type(self.max_level)])
            t.add_row(['Reservoir Surface Area', self.SURFACE_AREA, type(self.SURFACE_AREA)])
            t.add_row(['Target', self.target, type(self.target)])
            t.add_row(['Cost', self.best_cost, type(self.best_cost)])
            t.add_row(['Volume (litres)', self.best_volume, type(self.best_volume)])
            t.add_row(['Volume (m³)', self.best_volume/1000, type(self.best_volume/1000)])
            print(t)

    def store_data(self, tariff_cost, hours):
        """
        This function stores the data for all the possible combinations found for the site.

        Paramaters
        ----------
        tariff_cost
            Cost of the tariff.
        hours
            Length of tariff in hours.

        Returns
        ----------
        Array
            Possible cominbations for regime calculations
        """
        v = [] # throw away array v
        for data in self.pump_data:
            volume = self.tp_volume(data["Flow"], hours)
            cost = self.electricity_cost(data["Kw"], tariff_cost, hours)
            v.append({"speed": data[AEC.CONST_SPEED], "volume": volume, "cost": cost})
        self.all_data.append(v) # store data in all data array

    def get_tariff(self, tariff):
        """
        This method returns the cost per kilowatt hours for energy usage on current time (day, peak, evening or night).

        Parameters
        ----------
        Tariff
            Integer -> Current tariff: 1, 2, 3, or 4

        Returns
        ----------
        Float
            Current cost data based on current tariff.
        """
        if tariff == 1: return self.cost_data.day
        elif tariff == 2: return self.cost_data.peak
        elif tariff == 3: return self.cost_data.evening
        elif tariff == 4: return self.cost_data.night

    def get_tariff_cost(self, iterator):
        """
        This method returns the current tariff for each point when looping and collecting the data from the database.

        Parameters
        ----------
        Iterator
            Integer -> Current iterator in loop.

        Returns
        ----------
        Integer
            Current tariff data is returned with relevant information.
        """
        if self.weekday: 
            tariff = int(self.time_data[iterator][AEC.CONST_WKDAY])
        else:
            tariff = int(self.time_data[iterator][AEC.CONST_WKEND])
        return self.get_tariff(tariff)

    def prep_level_constraints(self):
        start_period = self.get_time_period()-1
        hours = (np.array([0]+[float(self.time_data[i][AEC.CONST_HOURS]) for i in range(start_period,len(self.time_data))]).cumsum() *60/30).astype(int)
        hours_diff = np.diff(hours)
        hist_df = self.slice_historical_data()
        out_flow_matrix=np.zeros((max(hours_diff),len(hours_diff)))
        for i,l in enumerate(hours_diff):
            out_flow_matrix[:l,i]=hist_df[hours[i]:hours[i+1]]
        return hours_diff,out_flow_matrix

    def get_time_period(self):
        """
        This method returns an integer based on what the current time period is. This is calculated based on current time.

        Returns
        ----------
        Integer
            Period based on the current time of day.
        """
        time_now = datetime.datetime.now().replace(hour=self.hour, minute=self.minute)
        if time_now < time_now.replace(hour=8, minute=0): return 1
        if time_now < time_now.replace(hour=14, minute=0): return 2
        if time_now < time_now.replace(hour=16, minute=0): return 3
        if time_now < time_now.replace(hour=19, minute=0): return 4
        if time_now < time_now.replace(hour=20, minute=30): return 5
        if time_now < time_now.replace(hour=22, minute=30): return 6
        return 7

    def period_start_time(self):
        """
        This method returns the start time of the time period, which is used for calculating the reamining time of that specific time period.

        Returns
        ----------
        DateTime
            Start time of specific time periods.
        """
        time_period = self.get_time_period()+1
        time_now = datetime.datetime.now().replace(hour=self.hour, minute=self.minute)
        if time_period == 1: return time_now.replace(hour=0, minute=0)
        if time_period == 2: return time_now.replace(hour=8, minute=0)
        if time_period == 3: return time_now.replace(hour=14, minute=0)
        if time_period == 4: return time_now.replace(hour=16, minute=0)
        if time_period == 5: return time_now.replace(hour=19, minute=0)
        if time_period == 6: return time_now.replace(hour=20, minute=30)
        if time_period == 7: return time_now.replace(hour=22, minute=30)

    def refine_period_time(self):
        """
        This method allows for calculation of the reamining time of a specific period gets that difference in seconds to adjust calculations.

        Returns
        ----------
        Float
            Total time reamining of period in seconds.
        """
        period_start_time = self.period_start_time()
        time_now = datetime.datetime.now().replace(hour=self.hour, minute=self.minute)
        diff = period_start_time-time_now
        return diff.total_seconds()/3600
    
    def data_collection(self, period_lengths):
        """
        This method processes the data from the database and allows us to generate combinations based on this data.

        Parameters
        ----------
        period_lengths
            Integer -> iterator for us to loop based on how many periods are left for day.

        Returns
        ----------
        List
            List of possible combinations, length of which depends on reamining time periods of the day.
        """
        hours = [float(i[AEC.CONST_HOURS]) for i in self.time_data]
        hours[self.get_time_period()-1] = float(self.refine_period_time())
        for i in range(period_lengths,len(self.time_data)):
            if self.weekday: 
                tariff = int(self.time_data[i][AEC.CONST_WKDAY])
            else:
                tariff = int(self.time_data[i][AEC.CONST_WKEND])
            tariff = self.tariff_to_text(tariff)
            tariff_cost = float(self.cost_data[tariff])
            v = [] # throw away array v
            for data in self.pump_data:
                volume = self.tp_volume(data["Flow"], hours[i])
                cost = self.electricity_cost(data["Energy"], tariff_cost, hours[i])
                v.append({"speed": data[AEC.CONST_SPEED], "volume": volume, "cost": cost, "hours": hours[i], "flow": data["Flow"]})
            self.all_data.append(v) # store data in all data array

    def tariff_to_text(self, tariff):
        """
        This method converts and returns the current tariff period, but as a string for array processing.

        Returns
        ----------
        String
            Converts tariff to a string for array processing.
        """
        if tariff == 1: return "Day"
        if tariff == 2: return "Peak"
        if tariff == 3: return "Evening"
        return "Night"

    def demand_adjustment(self):
        clamp = lambda n, minn, maxn: max(min(maxn, n), minn)
        averaged_demand = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet'])*1800)
        actual_demand = self.get_volume_delivered_12()["VolumeDelivered"]
        averaged_demand_total = averaged_demand[24:].sum()
        demand_factor = 0.94#actual_demand/averaged_demand_total
        return clamp(demand_factor, 0.9, 1.1)
        
    def manage_response(self, combo):
        """
        This method returns an array of combinations for the response based on what the pumpset has been required to do based on the constraints.

        Parameters
        ----------
        combo
            Array of pumping regime.

        Returns
        ----------
        Array
            Array of pumping regime Time Period 1 - 6.
        """
        name_start = len(self.time_data)-len(combo)
        empty_response = []
        if len(combo) < len(self.time_data):
            for i in range(len(self.time_data)-len(combo)):
                data = self.get_regime_data()[:len(self.time_data)-len(combo)]
                name = data[i]["PeriodName"]
                speed = data[i]["Speed"]
                volume = data[i]["Volume"]
                cost = data[i]["Cost"]
                time = data[i]["Time"]
                flow = data[i]["Flow"]
                est_level = data[i]["EstLevel"]
                empty_response.append({"Name": name, "Speed": speed, "Volume": volume, "Cost": cost, "Time": time, "Flow": flow, "EstLevel": est_level, "Combo": self.pump_combo})
        combo_response = []
        for i in range(len(combo)):
            name = "T%s" % (name_start+1)
            name_start += 1
            combo_response.append({"Name": name, "Speed": combo[i]["speed"], "Volume": combo[i]["volume"], "Cost": combo[i]["cost"], "Time": combo[i]["hours"], "Flow": combo[i]["flow"], "Combo": self.pump_combo})
        combo = empty_response+combo_response
        combo = self.estimate_reservoir_levels(combo) 

        #Insert on new day, otherwise update regime
        if(len(self.get_regime_data()) == 0):
            self.insert_regime(combo)
        else:
            self.update_regime(combo)
        return combo

    def estimate_reservoir_levels(self, combo):
        """
        This method will calculate the estimated reservoir levels after our inital calculations.

        Parameters
        ----------
        combo
            Array of speeds

        Returns
        ----------
        Array of levels
        """
        levels_ = []
        pumped_ = pd.Series([float(data["Flow"]) for data in combo for i in range(0, int(float(data["Time"])*2))])
        out_ = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet']))
        diff_ = pumped_-out_
        current_sample_period = sum([int(float(data["Length"]))*2 for data in self.time_data[:self.get_time_period()-1]])
        if(len(self.get_regime_data()) == 0):
            start_level = self.current_level
        else:
            start_level = float(self.get_regime_data()[0]["EstLevel"])

        FACTOR = 1/self.SURFACE_AREA

        for i in range(0, 48):
            if i == 0: 
                levels_.append(start_level)
            else: 
                if i == current_sample_period:
                    new_level = self.current_level
                else: 
                    new_level = start_level+diff_[i]*FACTOR
                start_level = new_level
                levels_.append(start_level)
        # print(len(levels_))

        period_data = [float(data["Length"]) for data in self.time_data]
        # Accumulate period_data
        accum_index = 0
        index_list = []
        for i in range(len(period_data)):
            index_list.append(accum_index)
            accum_index += period_data[i]*2

        index_list = [int(i) for i in index_list]
        
        # Loop combo and add "EstLevel" based on index of levels_
        for i in range(self.get_time_period()-1, len(combo)):
            combo[i]["EstLevel"] = levels_[index_list[i]]

        return combo

    def level_compensation(self):
        """
        This method will calculate difference between actual current level and estimated levels.

        Returns
        ----------
        Difference in litres
        """
        current_level = self.current_level
        estimate_level = float(self.get_regime_data()[self.get_time_period()-1]["EstLevel"])
        diff_m = estimate_level-current_level
        diff_m_cubed = diff_m*self.SURFACE_AREA
        diff_to_litres = diff_m_cubed*1000

        if self.hour == 8 and self.minute == 0:
            # 16 hours / 24 hours
            diff_to_litres = diff_to_litres*(960/1440)
        if self.hour == 14 and self.minute == 0:
            # 8 hours / 24 hours
            diff_to_litres = diff_to_litres*(840/1440)
        if self.hour == 16 and self.minute == 0:
            # 8 hours / 24 hours
            diff_to_litres = diff_to_litres*(480/1440)
        if self.hour == 19 and self.minute == 0:
            diff_to_litres = diff_to_litres*(300/1440)
        return diff_to_litres

    def initial_target_compensation(self):
        """
        This method will calculate difference between actual current level and setpoint.

        Returns
        ----------
        Difference in cubic metres
        """
        current_level = self.current_level
        setpoint_level = float(self.site_data["Setpoint"])
        diff_m = setpoint_level-current_level
        diff_m_cubed = diff_m*self.SURFACE_AREA
        diff_to_litres = diff_m_cubed*1000
        return diff_to_litres

    def demand_compensation(self):
        volume_used = self.get_volume_used()["ActualPumped"]
        expected_volume = sum([float(data["Volume"]) for data in self.get_regime_data()[:self.get_time_period()]])
        return (volume_used/expected_volume)*self.target

    def optimiser(self, cost_, volume_,v_min,flow_,min_level,max_level,initial_level,period_lengths,out_flow_, errors) :
        """
        This function optimises the regime possible combinations using convex optimisation. We assign and define the problem and uses `GLPK_MI` to solve our problem.

        Parameters
        ----------
        cost_
            Numpy Array
        volume_
            Numpy Array
        v_min
            Float -> minimum volume
        flow_
            Numpy Array
        min_level
            Float -> minimum level
        max_level
            Float -> maximum level
        initial_level
            Float -> initial level
        period_lengths
            Integer -> number of period lengths
        out_flow_
            Numpy Array
        errors
            Exceptions -> error handling

        Returns
        ----------
        selection
            Solved problem with the best possible combination for pumping regime.
        """
        FACTOR = 1/self.SURFACE_AREA
        input_flow_matrix=np.zeros((max(period_lengths),len(period_lengths)))
        for i,l in enumerate(period_lengths):
            input_flow_matrix[:l,i]=1
        selection = cp.Variable(shape=cost_.shape,boolean=True)
        assignment_constraint = cp.sum(selection,axis=1) == 1
        input_flow_= cp.sum(cp.multiply(flow_,selection),axis=1)
        input_flow_vector=cp.vec(cp.multiply(input_flow_matrix,np.ones((max(period_lengths), 1)) @ cp.reshape(input_flow_,(1,len(period_lengths)))))

        res_flow= (input_flow_vector-cp.vec(out_flow_))
        net_volume = res_flow * 1.8
        res_level=cp.cumsum(net_volume) * FACTOR + initial_level
        volume_= cp.sum(cp.multiply(volume_,selection))
        volume_constraint = volume_ >= v_min
        min_level_constraint = res_level >= min_level
        max_level_constraint = res_level <= max_level

        constraints = [assignment_constraint, max_level_constraint, min_level_constraint, volume_constraint]

        cost_ = cp.sum(cp.multiply(cost_,selection))
        assign_prob = cp.Problem(cp.Minimize(cost_),constraints)
        assign_prob.solve(solver=cp.CPLEX, verbose=False)

        # Display res_levels as a plot
        df = pd.DataFrame({
            "Input Flow": input_flow_vector.value,
            "Out Flow": cp.vec(out_flow_).value,
            "Res Level": res_level.value,
        })
        df = df[df["Out Flow"] != 0]

        return selection

    def recalulcation_required(self):
        """
        This method will termine if a recalculation is required at any point when triggered.
        """
        regime = self.get_regime_data()
        levels_ = []
        pumped_ = pd.Series([float(data["Flow"]) for data in regime for i in range(0, int(float(data["Time"])*2))])
        out_ = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet']))
        diff_ = pumped_-out_
        current_sample_period = sum([int(float(data["Length"]))*2 for data in self.time_data[:self.get_time_period()-1]])
        FACTOR = 1/self.SURFACE_AREA

        if(len(self.get_regime_data()) == 0):
            start_level = self.current_level
        else:
            start_level = float(self.get_regime_data()[0]["EstLevel"])

        for i in range(current_sample_period, 48):
            if i == 0: 
                levels_.append(start_level)
            else: 
                if i == current_sample_period:
                    new_level = self.current_level+diff_[i]*FACTOR
                else: 
                    new_level = start_level+diff_[i]*FACTOR
                start_level = new_level
                levels_.append(start_level)

        levels_ = pd.Series(levels_)
                
        levels_ = pd.Series(levels_).apply(lambda row: row < self.max_level and row > self.min_level)

        # If all returns True then calulcation not needed
        if levels_.all(): 
            exit()
        else: 
            return True

    def regime_management(self):
        """
        This method manages the regime. It adjusts based on errors thrown and allows for setting and editing the target as required in order to be adaptive to the current demands as per reservoirs.
        """
        error = None
        try:
            """
            Check for daily targets
            """
            # If target is 0 then it is a new day
            if len(self.get_target()) == 0:
                # Want to calculate target from historical average for past 4 weeks.
                self.target = pd.Series(self.get_historical()).apply(lambda x: float(x['Outlet'])).sum()*1800
                self.initial_target = self.target
            else:
                # Get last target from the database to use
                self.target = float(self.get_target()[0]["NewTarget"])
                self.initial_target = self.target

            """
            Compensate target based on inital level compated to level setpoint.
            """
            est_total = 0
            
            if self.hour == 0 and self.minute == 0:
                level_compensation = self.initial_target_compensation()
                demand_compensation = 1.0#self.demand_adjustment()
                self.target = self.target+level_compensation-est_total
                self.target = self.target * demand_compensation
                
            else:
                self.recalulcation_required()
                est_total = sum([float(data["Volume"]) for data in self.get_regime_data()[:self.get_time_period()-1]])
                level_compensation = self.level_compensation()
                demand_compensation = 1.0
                self.target = (self.target-est_total)+level_compensation

            if self.current_level < self.min_level: raise LevelTooLowError
            if self.current_level > self.max_level: raise LevelTooHighError
            if self.target >= self.max_volume(): raise MaxVolumeExceededError
        except LevelTooLowError:
            error = LevelTooLowError
            self.min_level = self.current_level
        except LevelTooHighError:
            error = LevelTooHighError
            # self.target = 0
        except MaxVolumeExceededError:
            error = MaxVolumeExceededError
            self.target = self.max_volume()
            
        # AEC Target Management
        self.insert_target(self.initial_target, demand_compensation, level_compensation, est_total, self.target+est_total)
        return error

    def get_regime(self):
        """
        This method returns the pumping regime. It calls upon regime_management and data_collection for processing possible regimes. 
        After data processing of costs, flows and volumes, we call upon the optimiser function to select the appropriate regime by minimzing the cost and ensuring constraints are met.
        
        Returns
        ----------
        `manage_response()`
        """
        regime_management = self.regime_management()
        self.data_collection(self.get_time_period()-1)
        hours,hist_df=self.prep_level_constraints()
        flow_list=[ [ candidate["flow"] for candidate in slot   ]    for slot in  self.all_data ]
        volume_list=[ [ candidate["volume"] for candidate in slot   ]    for slot in  self.all_data ]
        cost_list=[ [ candidate["cost"] for candidate in slot   ]    for slot in  self.all_data ]
        volume_=np.array(volume_list)
        cost_=np.array(cost_list)
        flow_=np.array(flow_list)
        sol=self.optimiser(cost_,volume_, self.target,flow_,self.min_level,self.max_level,self.current_level,hours,hist_df,regime_management)
        # sampler = 0.99
        # while sol.value is None:
        #     sol=self.optimiser(cost_,volume_, self.target*sampler,flow_,self.min_level,self.max_level,self.current_level,hours,hist_df,regime_management)
        #     sampler = sampler - 0.01
        #     if sampler <= 0.85:
        #         break

        assignments = [np.where(r>=0.99)[0][0] for r in sol.value]
        combo=[ self.all_data[i][assignments[i]] for i in range(len(assignments)) ]
        self.best_cost = np.sum(np.multiply(cost_,sol.value))
        self.best_volume = np.sum(np.multiply(volume_,sol.value)) 
        return self.manage_response(combo)