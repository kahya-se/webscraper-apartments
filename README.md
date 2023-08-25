# Web scraper: Apartments in Switzerland
## Purpose
Scrapes apartment advertisements from popular Swiss websites.

### History
During the spring of 2021, I was in search of an apartment. In addition to the standard criteria such as rental price and apartment size, 
I observed that filtering advertisements by a specific region was not a straightforward process. As a result, I developed a web scraper to extract 
"the lats and lons" for apartments, enabling the use of a spatial filter.
In the meantime, this tool runs multiple times a day to collect data on rent, apartment size, and the duration of advertisements. 
This data gathering process aims to provide insights into the spatial distribution of the collected information.

## Use
Set up an environment with the specified requirements (see requirements.txt) and use it as for example in main.py.
Alternatively, you can employ it by using a container (see Dockerfile). 
Some advertisements appear in various websites, do not forget to clean duplicates.

## Bugs
- Error of commission scraping homegate.ch: advertisements with erroneous entries (rent, area, rooms) are added. Description and address remains correct.

## Final note
This web scraper retrieves data from various sources and cannot assert the correctness, completeness, accuracy, or timeliness of the data.
Utilizing this web scraper does not confer ownership of the data to you.
The content available in this repository is intended solely for non-commercial, educational purposes.
Please be aware that frequent scraping of comparis.ch may result in a ban of your IP address.
Lastly, any feedback on writing cleaner code would be greatly appreciated.
