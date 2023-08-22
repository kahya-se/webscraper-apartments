#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrapes popular websites in Switzerland advertising appartments for rent.
Yields a dataframe with the following columns:
    - url: URL to advertisement
    - address
    - number of rooms
    - area (in m2)
    - rent (in CHF/Month)
    - short description
    - publication date
    - latitude and longitude
  

Prospective use cases:
    - downloading a list of advertisements
    - creating a map with currently online ads
    - observing the housing market (e.g.: spatial distribution of certain variables, lifespan of the ad, etc.)
    - ...

To be fixed:
    - errors of commission: homegate ads beyond restriction (MAX_RENT) #URGENT


author: sk
date: 21.08.2023
"""

import time
import re
import json
import requests
import concurrent.futures

import numpy as np
import pandas as pd

from json import JSONDecodeError

class Advertisements:
    """
    Define the ads to be scraped from homegate, immoscout, and in future also from comparis.

    PAGE (str, list): Websites to scrape. Accepts homegate and immoscout.
        In near future: comparis. 
        E.g.: 'immoscout', 'homegate_immoscout', 'all', ['immoscout', 'homegate']
    ROOMS_MIN (numeric): Min. number of rooms in the appartment
    ROOMS_MAX (numeric): Max. number of rooms in the appartment
    AREA_MIN (numeric): Min. area of the appartment
    AREA_MAX (numeric): Max. area of the appartment
    PRICE_MIN (numeric): Min. price of the appartment
    PRICE_MAX (numeric): Max. price of the appartment
    LOCATION (str): Name of the city.
    RADIUS (int): Search radius in km.
    MAX_WORKERS (int): Max. workers for multithreading
    """
    
    PAGE = 'homegate_immoscout'
    ROOMS_MIN = None
    ROOMS_MAX = None
    AREA_MIN = None
    AREA_MAX = None
    PRICE_MIN = None
    PRICE_MAX = None
    LOCATION = "Zürich"
    RADIUS = 1
    MAX_WORKERS = 100
    
    def __init__(self):
        
        if isinstance(self.LOCATION, int):
            self.LOCATION = postalcode2city(self.LOCATION)

        if isinstance(self.PAGE, list):
            self.PAGE = '_'.join(self.PAGE)
            
        if self.PAGE == 'all':
            self.PAGE = 'homegate_immoscout_comparis'
            
        self.URLS = self._getURL()
        
        self.MAX_WORKERS = int(self.MAX_WORKERS)
        

    def scrape(self):
        self.results = pd.DataFrame({'url':[], 'address':[], 'rooms':[], 'area':[], 'rent':[], 
                                  'description':[], 'published':[], 'lat':[], 'lon':[], 'source':[]})
            
        if 'immoscout' in self.PAGE:
            tt0 = time.perf_counter()
            self.results =  pd.concat([self.results, self._scrape_immoscout()]) 
            tt1 = time.perf_counter()
            print('Immoscout scraped: {} seconds'.format(round(tt1-tt0, 2)))
            
        if 'homegate' in self.PAGE:
            tt0 = time.perf_counter()
            self.results = pd.concat([self.results, self._scrape_homegate()])
            tt1 = time.perf_counter()
            print('Homegate scraped. {} seconds'.format(round(tt1-tt0, 2)))
            
        if 'comparis' in self.PAGE:
            #self.results =  pd.concat([self.results, self._scrape_comparis()]) 
            print('The comparis scraper is not yet developed.')
            pass

        self.results = self.results.sort_values('url', ascending=True)
        return self.results
  
    
    def _getURL(self):
        self.URL = {}

        if 'immoscout' in self.PAGE:
            if self.RADIUS == "null":
                self.RADIUS = None
            
            base_URL = "https://www.immoscout24.ch/de/immobilien/mieten/"
            close_part = "&"
            
            location_string = 'ort-' + self.LOCATION.lower().replace('ü','ue').replace('ö','oe').replace('ä','ae') + "?"
            
            search_string = location_string
            
            if self.PRICE_MIN is not None:
                min_price =  "pf=" + str(int(self.PRICE_MIN/100))+"h" + close_part
                search_string += min_price
                
            if self.PRICE_MAX is not None:
                max_price = "pt="+ str(int(self.PRICE_MAX/100))+"h" + close_part
                search_string += max_price
            
            if self.ROOMS_MIN is not None:
                min_rooms = "nrf=" + str(self.ROOMS_MIN) + close_part
                search_string += min_rooms
                
            if self.ROOMS_MAX is not None:
                max_rooms = "nrt=" + str(self.ROOMS_MAX) + close_part 
                search_string += max_rooms
                
            if self.AREA_MIN is not None:
                min_size = "slf=" + str(self.AREA_MIN) + close_part
                search_string += min_size
                
            if self.AREA_MAX is not None:
                max_size = "slt=" + str(self.AREA_MAX) + close_part 
                search_string += max_size
            
            if self.RADIUS == None:
                pass

            elif self.RADIUS > 0:
                search_radius = "r=" + str(round(self.RADIUS))
                search_string += search_radius
            
            _URL = base_URL + search_string
            
            self.URL['immoscout'] = _URL.rstrip('&')
        
        if 'homegate' in self.PAGE:
            if self.RADIUS == "null":
                self.RADIUS = 0
            
            location_string = 'ort-' + self.LOCATION.lower().replace('ü','ue').replace('ö','oe').replace('ä','ae')
            base_URL = "https://www.homegate.ch/mieten/immobilien/"+location_string+"/trefferliste?"
            close_part = "&"
            
            search_string = ''
            
            if self.PRICE_MIN is not None:
                min_price = "ag=" + str(self.PRICE_MIN) + close_part
                search_string += min_price
                
            if self.PRICE_MAX is not None:
                max_price = "ah=" + str(self.PRICE_MAX) + close_part
                search_string += max_price
            
            if self.ROOMS_MIN is not None:
                min_rooms = "ac=" + str(self.ROOMS_MIN) + close_part
                search_string += min_rooms
                
            if self.ROOMS_MAX is not None:
                max_rooms = "ad=" + str(self.ROOMS_MAX) + close_part 
                search_string += max_rooms
                
            if self.AREA_MIN is not None:
                min_size = "ak=" + str(self.AREA_MIN) + close_part
                search_string += min_size
                
            if self.AREA_MAX is not None:
                max_size = "al=" + str(self.AREA_MAX) + close_part 
                search_string += max_size
            
            if self.RADIUS == None:
                pass
            
            elif self.RADIUS > 0:
                search_radius = "be=" + str(int(self.RADIUS*1000)) 
                search_string += search_radius
            
            _URL = base_URL + search_string
            self.URL['homegate'] = _URL.rstrip('&')
        
        if 'comparis' in self.PAGE:
            comparis_url_dict = {"DealType": 10, "Sort": 3}
            base_URL = 'https://www.comparis.ch/immobilien/result/list?requestobject='
            
            if self.ROOMS_MIN is not None:
                comparis_url_dict['RoomsFrom'] = str(self.ROOMS_MIN)
                
            if self.ROOMS_MAX is not None:
                comparis_url_dict['RoomsTo'] = str(self.ROOMS_MIN)
                
            if self.AREA_MIN is not None:
                comparis_url_dict['LivingSpaceFrom'] = str(self.AREA_MIN)
                
            if self.PRICE_MIN is not None:
                comparis_url_dict['PriceFrom'] = str(self.AREA_MAX)
                
            if self.PRICE_MAX is not None:
                comparis_url_dict['PriceTo'] = str(self.ROOMS_MIN)
                
            if self.LOCATION is not None:
                comparis_url_dict['LocationSearchString'] = str(self.LOCATION)
                
            if self.RADIUS is not None:
                comparis_url_dict['Radius'] = str(self.RADIUS)
                
            comparis_url_string = json.dumps(comparis_url_dict, ensure_ascii=False)
            _URL = base_URL + comparis_url_string
            self.URL['comparis'] = _URL
        return self.URL
       
    def _scrape_homegate(self):
        def __get_HTML_chunks(url):
            response = requests.get(url)
            html = response.content.decode('utf-8')
            chunk_start_idx = re.search('<script>window.__INITIAL_STATE__=', html).span()[1]
            chunk_end_idx = re.search(',"page":\d{1,5},"pageCount":\d{1,5},"', html).span()[0]
            
            htmlChunk = html[chunk_start_idx:chunk_end_idx]
            htmlChunks.append(htmlChunk)
          
        def __get_homegate_ads(matches_pair):
            start = matches_pair[0]
            end = matches_pair[1]-1 
            
            try:
                chunkDict = json.loads(htmlChunks_str[start : end])
                
            except JSONDecodeError:
                chunkend = htmlChunks_str.find('\"remoteViewing\":',start+1)
                chunkDict = json.loads(htmlChunks_str[start : chunkend+len('\"remoteViewing\":')+5]+"}" )
            
            url = 'https://www.homegate.ch/mieten/'+chunkDict['listing']['id']
            
            try: 
                if chunkDict['listing']['prices']['rent']['interval'] == 'WEEK':
                    multiplier = 4
                else:
                    multiplier = 1
                
            except KeyError:
                multiplier = 1
            try:
                chunkDict['listing']['prices']['rent']['gross']
                rent = chunkDict['listing']['prices']['rent']['gross'] * multiplier
            except KeyError:
                rent = None
            
            plzAdr = " ".join([chunkDict['listing']['address']['postalCode'], chunkDict['listing']['address']['locality']])
            
            try:
                address = ", ".join([chunkDict['listing']['address']['street'], plzAdr])
            except KeyError:
                address = plzAdr
        
            address = address.replace(',,',',')
            
            try:
                room = chunkDict['listing']['characteristics']['numberOfRooms']
            except KeyError:
                room = None
                       
            try:
                area = chunkDict['listing']['characteristics']['livingSpace']
            except KeyError:
                area = None
                
                
            try:
                description = chunkDict['listing']['localization']['de']['text']['title']
            except KeyError:
                description = None
                
            pdates = None
            
            try:
                lat = chunkDict['listing']['address']['geoCoordinates']['latitude']
                lon = chunkDict['listing']['address']['geoCoordinates']['longitude']
            except KeyError:
                lat = None
                lon = None
                
            try:
                agencyLogo = chunkDict['listerBranding']['logoUrl']
                agency = agencyLogo
            except KeyError:
                agency = None
                    
            newRow = pd.Series({'url':url, 'address':address, 'rooms':room, 'area':area, 'rent':rent, 
                                      'description':description, 'agency':agency,
                                      'published':pdates, 'lat':lat, 'lon':lon})
        
            rows.append(newRow)  
            
        
        URL = self.URLS['homegate']
        response = requests.get(URL)
        html = response.content.decode('utf-8')
        
        maxPageSpan = re.search('"pageCount":\d{1,5}', html).span()
        maxPageStr = html[maxPageSpan[0]:maxPageSpan[1]].split(':')[1]
        maxPage = int(maxPageStr)
        print("Homegate accessed, no. of pages: {}".format(maxPage))
        
        urls = [URL+"&ep="+str(p) for p in range(maxPage+1)]
        htmlChunks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as e:
            e.map(__get_HTML_chunks, urls)
        
        htmlChunks_str = " ".join(htmlChunks)
        
        matches = [m.start(0) for m in re.finditer('{"listingType":', htmlChunks_str)]
        matches.append(-1)
            
        matches_pairs = [(matches[i],matches[i+1]) for i in range(len(matches)-1)]    
    
        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as e:
            e.map(__get_homegate_ads, matches_pairs)
        
        homegate_scraped = pd.DataFrame(rows)
        homegate_scraped = homegate_scraped.assign(source='homegate')
        homegate_scraped.drop_duplicates(subset=["url"], inplace=True)
        return homegate_scraped
     
    
    def _scrape_immoscout(self):     
        def __get_HTML_chunks(url):
            response = requests.get(url)
            html = response.content.decode('utf-8')
            chunk_start = re.search('\{"id":\d{7},"accountId"', html).span()[0]
            chunk_end = re.search(',"adData"', html).span()[0]
            
            html_chunk = html[chunk_start:chunk_end]
            html_chunks.append(html_chunk)
            
            
        def __get_immoscout_ads(matches_start):
            matches_end = re.search(',"sortalgoScore"', html_chunks_str[matches_start:]).span()[0]
            
            info = html_chunks_str[matches_start:matches_start+matches_end] + "}"
            infoAsDict = json.loads(info)
        
            _url = 'https://www.immoscout24.ch'+infoAsDict['propertyUrl']
            _url = _url.replace("https://www.immoscout24.chhttps://", "https://www.")
            urls = _url
            
            if 'street' in infoAsDict:
                addresses = ", ".join([infoAsDict['street'], " ".join([infoAsDict['zip'],  infoAsDict['cityName']])])
            else:
                addresses = " ".join([infoAsDict['zip'],  infoAsDict['cityName']])
            addresses = addresses.strip(',')
            
            if infoAsDict['priceFormatted'] == 'Preis auf Anfrage':
                prices = None
            elif 'grossPrice' in infoAsDict:
                prices = infoAsDict['grossPrice']
            else:
                prices = infoAsDict['price']
            
            if 'numberOfRooms' in infoAsDict:
                rooms = infoAsDict['numberOfRooms']
            else:
                rooms = None
            
            if 'surfaceLiving' in infoAsDict:
                areas = infoAsDict['surfaceLiving']
            else:
                areas = None
                
            if 'title' in infoAsDict:
                descriptions = infoAsDict['title']
            else:
                descriptions = None
            
            pdate = infoAsDict['lastPublished']
            pdates = pdate
        
            if 'latitude' in infoAsDict:
                lat = infoAsDict['latitude']
                lon = infoAsDict['longitude']
            else:
                lat = None
                lon = None

            try:
                companyName_strings = [x for x in infoAsDict['agency'].keys() if 'companyName' in x]
                if len(companyName_strings) == 0:
                    agency = None
                    
                company_names = [infoAsDict['agency'][f] for f in companyName_strings]
                companyString = ", ".join(company_names)
                agency = companyString
               
            except KeyError:
                agency = None
            
            
            row = pd.Series({'url':urls, 'address':addresses, 'rooms':rooms, 'area':areas, 'rent':prices,
                                      'description':descriptions, 'agency':agency,
                                      'published':pdates, 'lat':lat, 'lon':lon})
            rows.append(row)
        
        
        URL = self.URLS['immoscout']
        response = requests.get(URL)
        html = response.content.decode('utf-8')
           
        maxPagesTagStart = "<section class=\"Pagination__PaginationSection"
        maxPagesTagEnd = "</section>"
        paginationChunk = html[html.find(maxPagesTagStart):html.find(maxPagesTagEnd,html.find(maxPagesTagStart))]
        paginationsImmo = re.findall(">([0-9]|[1-9][0-9])</button", paginationChunk)
        
        if len(paginationChunk) == 0:
            maxPagination = 1
        else:
            maxPagination = np.max([int(x) for x in paginationsImmo])
                
        print("Immoscout accessed, no. of pages: {}".format(maxPagination))
        
        urls = [URL+"&pn="+str(p) for p in range(1, maxPagination+1)]
        
        html_chunks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as e:
            e.map(__get_HTML_chunks, urls)
        
        html_chunks_str = " ".join(html_chunks)
        matches_starts = [m.start(0) for m in re.finditer('\{"id":\d{7},"accountId"', html_chunks_str)]

        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as e:
            e.map(__get_immoscout_ads, matches_starts)
        
        immoscout_scraped = pd.DataFrame(rows)
        immoscout_scraped = immoscout_scraped.assign(source='immoscout')
        immoscout_scraped.drop_duplicates(subset=['url'], inplace=True)
        return immoscout_scraped


    def _scrape_comparis(self):
        # To be done.
        pass
 
    
def postalcode2city(postalcode):
    """ Maps postal codes to cities (online) """
    tablesFromWeb = pd.read_html("https://postleitzahlenschweiz.ch/tabelle/")[0]
    tablesFromWeb.columns = ['postal', 'city', 'Kanton', 'Canton', 'Cantone',
           'Abkürzung / Abréviation / Abbreviazione', 'Land', 'Pays', 'Paese']
    
    lookupPLZ = tablesFromWeb.postal.tolist()
    lookupCities = tablesFromWeb.city.tolist()
    lookupdict = dict(zip(lookupPLZ,lookupCities))
    
    if isinstance(postalcode,int):
        return lookupdict[postalcode]
    elif isinstance(postalcode, list):
        cities = []
        for plz in postalcode:
            city = lookupdict[plz]
            cities.append(city)
        return cities