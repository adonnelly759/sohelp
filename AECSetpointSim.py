# Setpoint Simulator
from cmath import inf
from AEC import AEC
import numpy as np
import pandas as pd
import json

from AECDatabase import AECDatabase

# Loop between 4.0 and 5.35 in increments of 0.05
setpoints_ = [data for data in np.arange(4.25, 5.15, 0.01)]
site_id = 11

# Setup database
db = AECDatabase()
db.setup_connection('root', 'Gl0bal?12', '127.0.0.1', 3306, 'aec')
complete_data = []

# print(pd.read_json(json.dumps(data.get_regime())))

for sp in setpoints_:
    db.update_setpoint(site_id, sp)
    db.clear_data(site_id)
    try:
        data = AEC(sp, site_id, 1, False)
        cost = [data["Cost"] for data in data.get_regime()]
        sum_cost = sum(cost)
        complete_data.append({"Setpoint": sp, "Cost": sum_cost})
    except:
        complete_data.append({"Setpoint": sp, "Cost": inf})

df = pd.read_json(json.dumps(complete_data))

df.sort_values(by=['Cost'], inplace=True)

df.to_csv('setpoint_sim.csv', index=False)
