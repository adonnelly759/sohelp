import datetime, math

class AECUtilities:
    def get_mode(self):
        """
        This method returns the current mode setting for AEC, this is based on current hour.

        Returns
        ----------
        Integer
            Current time period mode [1, 2 or 3]
        """
        if self.hour < 16: return 1
        if self.hour < 19: return 2
        return 3

    def electricity_cost(self, kw, cost, hours):
        """
        Calculate the cost of electricity for pump speed and running time.
        Calculation: Kilowatts * Energy Cost * Hours

        Parameters
        ----------
        flow
            Float -> litres/second
        hours
            Float -> Hours

        Returns
        ----------
        float
            TP Period Volume Calculation
        """
        return float(kw)*cost*hours

    def tp_volume(self, flow, hours):
        """
        Calculate the volume for the time period.
        Calculation: Flow * Hours * 3600 (seconds in one hour)

        Parameters
        ----------
        flow
            Float -> litres/second
        hours
            Float -> Hours

        Returns
        ----------
        float
            Total volume for time period
        """
        return float(flow)*hours*3600

    def get_volume(self, combo): # Get Total Volume
        """
        Calculate the volume for a combination (array of time periods, generated and selected through optimisation function).
        Loop through the combo array and increment total with values from volume key of array.

        Parameters
        ----------
        combo
            Array of all time period comintions['volume']

        Returns
        ----------
        float
            Total volume for pumping regime
        """
        volume = 0 
        for c in combo: volume+=c["volume"] 
        return volume

    def get_cost(self, combo): # Get Total Cost
        """
        Calculate the cost for a combination (array of time periods, generated and selected through optimisation function).
        Loop through the combo array and increment total with values from cost key of array.

        Parameters
        ----------
        combo
            Array of all time period comintions['cost']

        Returns
        ----------
        float
            Total cost for pumping regime
        """
        cost = 0 
        for c in combo: cost+=c["cost"] 
        return cost

    def max_volume(self):
        """
        Calculate the max volume remaining for rest of the day.
        Retrieves the max flow available for pump set * Time Remaining * 3600

        Returns
        ----------
        Float
            Max Volume available for pumping
        """
        end_day = datetime.datetime.now().replace(hour=23, minute=59, second=59)
        time_now = datetime.datetime.now().replace(hour=self.hour, minute=self.minute)
        diff = end_day-time_now
        max_volume = diff.total_seconds()*float(self.pump_data[len(self.pump_data)-1]["Flow"])
        return max_volume

    def is_weekday(self):
        """
        This method returns whether or not it is currently a weekday, and is used for adjusting the tariff periods based on time of week.

        Returns
        ----------
        Boolean
            True if current day is a weekday, False otherwise.
        """
        return True if datetime.datetime.today().weekday() < 5 else False

    """
    This method will return a list of dates in the required format for querying the database and inserting new targets for week ahead.
    """
    def prev_week_dates(self):
        today = datetime.date.today()
        weekday = today.weekday()
        start_delta = datetime.timedelta(days=weekday, weeks=1)
        start_of_week = today - start_delta
        return [obj.strftime("%Y-%m-%d") for obj in [start_of_week+datetime.timedelta(i) for i in range(0,7)]]

    def calculate_outflow(self, start_level, finish_level, flow, seconds=1800):
        surface_area = math.pi*(12**2)*2
        pumped = flow*seconds
        level_difference = finish_level-start_level
        net_gain = level_difference*surface_area
        difference = net_gain*1000
        outflow = pumped-abs(difference) if difference >= 0 else pumped+abs(difference)
        return outflow/seconds

    def reverse_historical(self):
        historical_buffer = self.get_historical_buffer()[53:]
        buffer = []
        for i in range(0, len(historical_buffer)-1):
            
            start_level = float(historical_buffer[i]["Level"])
            finish_level = float(historical_buffer[i+1]["Level"])
            flow = float(historical_buffer[i+1]["Flow"])
            buffer.append(self.calculate_outflow(start_level, finish_level, flow))

        buffer = buffer[4:]
            
        for i in range(0, len(buffer)):
            self.update_historical(buffer[i]*0.9, i+46)