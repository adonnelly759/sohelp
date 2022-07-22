import csv, sys, math
from AEC import AEC

# Command line arguments
current_level = float(sys.argv[1])
pump_combo = float(sys.argv[2])
site_id = 2

AEC(current_level, site_id, pump_combo, True)