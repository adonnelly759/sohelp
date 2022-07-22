import mariadb, sys

class AECDatabase():
    """
    This class handles the parsing and quering of the databse.
    """
    def setup_connection(self, username, password, host, port, database):
        """
        This method sets up the database connection.

        Parameters
        ----------
        username
            String
        password
            String
        host
            IP Address as String
        port
            Integer
        database
            String
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def open_connection(self):
        """
        This method opens the database connection.
        """
        try:
            self.connection = mariadb.connect(user=self.username, password=self.password, host=self.host, port=self.port, database=self.database)
            self.cur = self.connection.cursor()
        except mariadb.Error:
            sys.exit(1)

    def get_site_data(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM site WHERE ID = ?", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_volume_used(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeUsed(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_volume_delivered_0000(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeDelivered0000(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_volume_delivered_12(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeDelivered12(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_volume_delivered_0800(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeDelivered0800(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_volume_delivered_1600(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeDelivered1600(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result
    
    def get_volume_delivered_1900(self):
        """
        This method returns the stored procedure getSiteData.
        """
        self.open_connection()
        self.cur.execute("CALL getVolumeDelivered1900(?);", (self.site_id,))
        fetch_site = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_site))
        self.close_connection()
        return result

    def get_regime_yesterday(self):
        """
        This method returns the stored procedure getHistorical.
        """
        self.open_connection()
        self.cur.execute("CALL getRegimeYesterday(?);", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result

    def get_typical_inlet_data(self):
        """
        This method returns the stored procedure getTypicalInletData.
        """
        self.open_connection()
        self.cur.execute("CALL getTypicalInletData(?);", (self.site_id,))
        fetch_typical_inlet = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_typical_inlet))
        self.close_connection()
        return result

    def get_typical_outlet_data(self):
        """
        This method returns the stored procedure getTypicalOutletData.
        """
        self.open_connection()
        self.cur.execute("CALL getTypicalOutletData(?);", (self.site_id,))
        fetch_typical_outlet = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_typical_outlet))
        self.close_connection()
        return result

    def get_cost_data(self, cost_id, month):
        """
        This method returns the stored procedure getCostData.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM cost WHERE CostID = ? and `Month` = ?;", (cost_id, month))
        fetch_cost = self.cur.fetchone()
        result = dict(zip([c[0] for c in self.cur.description], fetch_cost))
        self.close_connection()
        return result

    def get_pump_data(self, pump_combo):
        """
        This method returns the stored procedure getPumpData.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM pump WHERE SiteID = ? AND Combination = ?;", (self.site_id, pump_combo,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result

    def get_latest_suction_pressure(self):
        """
        This method returns the stored procedure getPumpData.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM suction_pressure WHERE SiteID = ? ORDER BY ID DESC LIMIT 1;", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result
    
    def get_tariff_data(self, tariff_id):
        """
        This method returns the stored procedure getTariffData.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM tariff WHERE TypeID = ?;", (tariff_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result
    
    def get_regime_data(self):
        """
        This method returns the stored procedure getRegime.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM regime WHERE SiteID = ? AND DATE(Created) = CURDATE();", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result

    def insert_buffer(self, pumped_flow, level):
        """
        This method returns the stored procedure insertRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        self.cur.execute("INSERT INTO historical_buffer (ID, SiteID, PumpedFlow, Level) VALUES (null, ?, ?, ?);", (self.site_id, pumped_flow, level,))
        self.connection.commit()
        self.close_connection()

    def insert_historical(self, outlet):
        """
        This method returns the stored procedure insertRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        self.cur.execute("INSERT INTO historical (ID, SiteID, Outlet) VALUES (null, ?, ?);", (self.site_id, outlet,))
        self.connection.commit()
        self.close_connection()

    def insert_level_estimate(self, t1, t2, t3, t4, t5, t6, end_day):
        """
        This method returns the stored procedure insertRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        self.cur.execute("CALL insertLevelEstimate(?, ?, ?, ?, ?, ?, ?, ?);", (self.site_id, t1, t2, t3, t4, t5, t6, end_day,))
        self.connection.commit()
        self.close_connection()

    def insert_diagnostics(self, json_data):
        """
        This method returns the stored procedure insertRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        self.cur.execute("CALL insertDiagnostics(?, ?);", (self.site_id, json_data,))
        self.connection.commit()
        self.close_connection()

    def last_historical_buffer(self):
        self.open_connection()
        self.cur.execute("SELECT * FROM historical_buffer WHERE SiteID = ? ORDER BY ID DESC LIMIT 1;", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result

    def insert_regime(self, combo):
        """
        This method returns the stored procedure insertRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        for data in combo:
            self.cur.execute("INSERT INTO regime (ID, SiteID, PeriodName, Speed, Flow, Time, Volume, Cost, EstLevel, Pump) VALUES (null, ?, ?, ?, ?, ?, ?, ?, ?, ?);", (self.site_id, data["Name"], data["Speed"], data["Flow"], data["Time"], data["Volume"], data["Cost"], data["EstLevel"], data["Combo"],))
            self.connection.commit()
        self.close_connection()

    def update_regime(self, combo):
        """
        This method returns the stored procedure updateRegime.

        Paramaters
        ----------
        combo
            Array of regimes
        """
        self.open_connection()
        for data in combo[self.get_time_period()-1:]:
            self.cur.execute("UPDATE regime SET `Speed`=?, `Cost`=?, `Volume`=?, `Time`=?, `Flow`=?, `EstLevel`=?, `Pump`=? WHERE SiteID = ? AND `PeriodName` = ? AND DATE(`Created`) = CURDATE();", (data["Speed"], data["Cost"], data["Volume"], data["Time"], data["Flow"], data["EstLevel"], data["Combo"], self.site_id, data["Name"],))
            self.connection.commit()
        self.close_connection()

    def get_historical(self):
        """
        This method returns the stored procedure getHistorical.
        """
        self.open_connection()
        self.cur.execute("SELECT TIME(`Created`) AS 'Time', AVG(`Outlet`) AS 'Outlet' FROM `historical` WHERE DATE(`Created`) BETWEEN NOW() - INTERVAL 4 WEEK AND NOW() - INTERVAL 1 WEEK AND WEEKDAY(`Created`) = WEEKDAY(NOW()) AND SiteID = ? GROUP BY HOUR(`Created`), MINUTE(`Created`);", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result  

    def get_historical_buffer(self):
        """
        This method returns the stored procedure getHistoricalForTarget.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM historical_buffer;")
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result
    

    def get_historical_for_target(self, date):
        """
        This method returns the stored procedure getHistoricalForTarget.
        """
        self.open_connection()
        self.cur.execute("CALL getHistoricalForTarget(?, ?);", (self.site_id, date,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result

    def update_historical(self, outlet, updateID):
        """
        This method updates the target for the specific day.
        """
        self.open_connection()
        query = "UPDATE historical SET Outlet = %s WHERE ID = %s;" % (outlet, updateID)
        self.cur.execute(query)
        self.connection.commit()
        self.close_connection()

    def update_target(self, day, target):
        """
        This method updates the target for the specific day.
        """
        self.open_connection()
        query = "UPDATE typical_inlet SET %s = %s WHERE SiteID = %s ORDER BY ID DESC LIMIT 1;" % (day, target, self.site_id)
        self.cur.execute(query)
        self.connection.commit()
        self.close_connection()

    def update_target_new(self, target):
        """
        This method updates the target for today and inserts for use and tracability.
        """
        self.open_connection()
        self.cur.execute("CALL updateTarget(?, ?);", (self.site_id, target,))
        self.connection.commit()
        self.close_connection()

    def get_target(self):
        """
        This method gets target for today.
        """
        self.open_connection()
        self.cur.execute("SELECT * FROM target WHERE SiteID = ? AND DATE(Created) = CURDATE() ORDER BY ID DESC LIMIT 1;", (self.site_id,))
        headers = [x[0] for x in self.cur.description]
        result = []
        for row in self.cur:
            result.append(dict(zip(headers, list(map(str, list(row))))))
        self.close_connection()
        return result   

    def insert_target(self, init_target, demand_adjustment, level_adjustment, pumped_volume, new_target):
        """
        This method creates the target for today and inserts for use and tracability.
        """
        self.open_connection()
        self.cur.execute("INSERT INTO target (ID, SiteID, InitialTarget, DemandAdj, LevelAdj, PumpedVolume, NewTarget) VALUES (NULL, ?, ?, ?, ?, ?, ?);", (self.site_id, init_target, demand_adjustment, level_adjustment, pumped_volume, new_target,))
        self.connection.commit()
        self.close_connection()

    def insert_suction_pressure(self, suction_pressure):
        """
        This method creates the target for today and inserts for use and tracability.
        """
        self.open_connection()
        self.cur.execute("INSERT INTO suction_pressure (ID, SiteID, Pressure) VALUES (NULL, ?, ?);", (self.site_id, suction_pressure,))
        self.connection.commit()
        self.close_connection()

    def clear_data(self, site_id):
        self.open_connection()
        self.cur.execute("DELETE FROM aec_target WHERE SiteID = ? AND DATE(Created) = CURDATE();", (site_id,))
        self.cur.execute("DELETE FROM regime_management WHERE SiteID = ? AND DATE(Created) = CURDATE();", (site_id,))
        self.connection.commit()
        self.close_connection()

    def update_setpoint(self, site_id, setpoint):
        self.open_connection()
        self.cur.execute("UPDATE site SET LevelSetpoint = ? WHERE ID = ?", (setpoint, site_id,))
        self.cur.execute("DELETE FROM regime_management WHERE SiteID = ? AND DATE(Created) = CURDATE();", (site_id,))
        self.connection.commit()
        self.close_connection()
    
    def close_connection(self):
        """
        This method closes the database connection.
        """
        self.connection.close()