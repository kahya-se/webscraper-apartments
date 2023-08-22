#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd

from datetime import datetime

from scrape_apartment_ads import Advertisements


if __name__ == '__main__':
    time1 = time.time()

    OUTPUT_PATH = '/home/user/Proj/scrape-apartments/output'

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    class Ads(Advertisements):
            PAGE = 'homegate_immoscout'
            ROOMS_MIN = 2.5
            ROOMS_MAX = 4.5
            PRICE_MIN =  1200
            PRICE_MAX = 2500
            AREA_MIN = 30
            AREA_MAX = 80
            RADIUS = 5 
            MAX_WORKERS = 100
            LOCATION = "Zürich"

    ads = Ads()
    df = ads.scrape()
    timestamp = datetime.fromtimestamp(time1).strftime("%Y%m%d-%H%M")
    time2 = time.time()

    fname = os.path.join(OUTPUT_PATH, 'apartments_'+ads.LOCATION+'_'+timestamp+'.csv') 
    df.to_csv(fname)
    
    print('Entries: {}'.format(len(df)))
    print('Saved: '+fname)
    print('Time: {} sec'.format(round(time2-time1,2)))