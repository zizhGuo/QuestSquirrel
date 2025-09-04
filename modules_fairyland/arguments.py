import argparse

parser = argparse.ArgumentParser(description='Quest Squirrel Argument Parser')

# general settings
parser.add_argument("end_dt", type = str, help="Current date in YYYY-MM-DD format")

parser.add_argument("config_file", type = str, help="config file name in yaml format")

