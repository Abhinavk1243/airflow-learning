import os
import gc
import json
import logging
import argparse
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import re

def main(params):
    print('----------------')

if __name__ == "__main__":
    try:
        print('--------HERE 1----------')
        parser = argparse.ArgumentParser()

        print('--------HERE 2----------')
        parser.add_argument('--conf','-conf',
                            type=json.loads,
                            required=False,
                            help='Configuration parameters with startdate')
        print('--------HERE 3----------')
        print(parser)
        print('--------')
        args = parser.parse_args()
        print('--------HERE 4----------')
        params = args.conf
        print('--------HERE 5----------')
        print(params)
        main(params)

        # day = '2021-10-09'
        # param  = json.dumps({"startdate":str(day), "enddate":str(day), "process":"create", "clear_directory": False})
        # param = json.loads(param)
        # print(type(param['clear_directory']))

        # logging.info('Successfully completed.')

    except Exception as error:
        print(f'Exception occurs : {error}')
