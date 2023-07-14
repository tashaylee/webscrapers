from pathlib import Path
import scrapy
import json
from unicodedata2 import normalize
import pandas as pd
import os
from datetime import datetime


class ForeclosedSpider(scrapy.Spider):
    # name for spider
    name = "foreclosure"
    homes = []

    def start_requests(self):
        urls = [
            "https://www.zillow.com/new-york-ny/foreclosures/?searchQueryState=%7B%22mapBounds%22%3A%7B%22north%22%3A41.05401596352842%2C%22east%22%3A-73.5058955996094%2C%22south%22%3A40.339772728407404%2C%22west%22%3A-74.45346640039065%7D%2C%22isMapVisible%22%3Atrue%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A6181%2C%22regionType%22%3A6%7D%5D%2C%22pagination%22%3A%7B%7D%7D",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_property_list_html)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = f"foreclosures-ny-{page}.html"
        Path(filename).write_bytes(response.body)
        self.log(f"Saved file {filename}")
    
    def extract_property_specifics(self,response):
        url = response.url
        address = response.xpath('//h1/text()').getall()
        zestimate = response.xpath('//span[contains(@data-testid,"zestimate-text")]/span/span/text()').get()
        listing_price = response.xpath('//span[contains(@data-testid,"price")]/span/text()').get()
        home_type = response.xpath('//*[@class="data-view-container"]//ul/li//div//ul/li//span/text()').get()

        address = normalize('NFKD'," ".join(address))
        self.homes.append([url, address, listing_price, zestimate, home_type])

    
    def extract_properties(self,response):
        properties_str = response.xpath('//script[contains(text(), "listResults")]/text()').get()
        properties_dict = json.loads(properties_str)
        results = properties_dict.get('props',dict()).get("pageProps", dict()).get("searchPageState",dict()).get('cat1',dict()).get('searchResults', dict()).get('listResults', [])
        
        for property in results:
            try:
                detail_url = property.get('detailUrl', None)
                yield scrapy.Request(url=detail_url, callback=self.extract_property_specifics)
            except Exception as e:
                raise e



    def parse_property_list_html(self, response):
        paginated_pages = response.xpath('//*[@class="search-pagination"]//ul/li/a[contains(@title,"Page")]/@href').getall()
        for page in paginated_pages:
            baseurl = 'https://www.zillow.com'
            url = baseurl + page
            self.start_urls.append(url)
            yield scrapy.Request(url=url, callback=self.extract_properties)
        
    def closed(self, reason):
        '''Method to be called after the spider finishes'''
        foreclosed_homes_df = pd.DataFrame(self.homes, columns=['url','address','listing_price','zestimate','home_type'])
        file_location = os.path.expanduser("~/Desktop/")
        now = datetime.now().strftime("%m_%d_%Y")
        file_name = f"nyc_foreclosed_homes_{now}.csv"
        return foreclosed_homes_df.to_csv(f'{file_location}{file_name}', index=False)