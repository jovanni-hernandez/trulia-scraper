import scrapy
from scrapy.crawler import CrawlerProcess
import re
from datetime import datetime, timedelta
import gspread
import pandas as pd
import numpy as np

class TruliaScraper(scrapy.Spider):

    name = 'trulia'
    GOOGLE_SHEET_NAME = 'Trulia Home Analysis'
    
    def __init__(self):
        self.client = gspread.service_account()
        self.worksheet = self.open_google_worksheet(self.client)
        self.worksheet_df = pd.DataFrame(self.worksheet.get_all_records())
        self.worksheet_df = self.worksheet_df.replace(r'^\s*$', np.nan, regex=True)

    
    def get_urls(self):
    
        urls_to_crawl = []

        if self.worksheet_df.size > 0:
            homes_not_sold =  self.worksheet_df[self.worksheet_df['Sold Date'].isna()]
            urls_to_crawl = homes_not_sold['URL'].tolist()

        return urls_to_crawl
        
        
    def submit_for_sale(self, url, address, city_state, price, list_date):

        target_index = self.worksheet_df[self.worksheet_df["URL"] == url].index
        is_already_populated = self.worksheet_df.iloc[target_index]["List Date"].notna().iloc[0]

        if not is_already_populated:
            self.worksheet_df.at[target_index, 'Address'] = address
            self.worksheet_df.at[target_index, 'City, State, Zip'] = city_state
            self.worksheet_df.at[target_index, 'List Price'] = price
            self.worksheet_df.at[target_index, 'List Date'] = list_date
        
    
    def submit_pending(self, url, address, city_state, trulia_estimate, todays_date):
        
        target_index = self.worksheet_df[self.worksheet_df["URL"] == url].index
        is_already_populated = self.worksheet_df.iloc[target_index]["Pending Date"].notna().iloc[0]
        
        if not is_already_populated:
            self.worksheet_df.at[target_index, 'Pending Price Estimate'] = trulia_estimate
            self.worksheet_df.at[target_index, 'Pending Date'] = todays_date
        
        
    def submit_off_market(self, url, address, city_state, trulia_estimate, todays_date):
      
        target_index = self.worksheet_df[self.worksheet_df["URL"] == url].index
        is_already_populated = self.worksheet_df.iloc[target_index]["Off Market Date"].notna().iloc[0]
        
        if not is_already_populated:
            self.worksheet_df.at[target_index, 'Off Market Price Estimate'] = trulia_estimate
            self.worksheet_df.at[target_index, 'Off Market Date'] = todays_date
        
        
    def submit_sold(self, url, address, city_state, sold_price, sold_date, todays_date):
        
        target_index = self.worksheet_df[self.worksheet_df["URL"] == url].index
        is_already_populated = self.worksheet_df.iloc[target_index]["Sold Date"].notna().iloc[0]
        
        if not is_already_populated:
            self.worksheet_df.at[target_index, 'Sold Price'] = sold_price
            self.worksheet_df.at[target_index, 'Sold Date'] = sold_date
            self.worksheet_df.at[target_index, 'Sold Public Record Date'] = todays_date     
        
        
    def open_google_worksheet(self, client):
    
        worksheet = None
        #COLUMNS = ["URL", "Picture", "Address", "City, State, Zip",	"List Price", "List Date", "Pending Date", "Pending Price Estimate", "Off Market Date", "Off Market Price Estimate",
        #    "Sold Date", "Sold Price", "Days before pending"]
        
        try:
            spreadsheet = client.open(self.GOOGLE_SHEET_NAME)
            worksheet = spreadsheet.get_worksheet(0)

            #worksheet.insert_row(COLUMNS, index=1, value_input_option='RAW')
            #worksheet.format('A1:K1', {'textFormat': {'bold': True}})

        except gspread.SpreadsheetNotFound:
            worksheet = self.create_google_worksheet(client)
            
        return worksheet
            
            
    def create_google_worksheet(self, client):

        COLUMNS = ["URL", "Picture", "Address", "City, State, Zip",	"List Price", "List Date", "Pending Date", "Pending Price Estimate", "Off Market Date", "Off Market Price Estimate",
            "Sold Date", "Sold Price", "Sold Public Record Date", "Days before pending", "Days to close", "Difference in List & Estimate Price", "Difference in List & Sold Price"]
            
        spreadsheet = client.create(self.GOOGLE_SHEET_NAME)
        spreadsheet.share('jovanni.m.hernandez@gmail.com', perm_type='user', role='owner')
        
        worksheet = spreadsheet.get_worksheet(0)
        worksheet.insert_row(COLUMNS, index=1, value_input_option='RAW')
        worksheet.format('A1:Q1', {'textFormat': {'bold': True}})

        return worksheet
    
    
    def start_requests(self):
        urls = self.get_urls()
        
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(url=url))


    def parse(self, response, url):
        
        address = response.css("span[data-testid='home-details-summary-headline']::text").extract_first()
        city_state = response.css("span[data-testid='home-details-summary-city-state']::text").extract_first()
        price = response.css("div[data-testid='home-details-sm-lg-xl-price-details'] h3 div::text").extract_first()
        sold_date = response.css("span[data-testid='hero-image-property-tag-1'] span::text").extract_first()
        status = response.css("span[data-testid='hero-image-property-tag-0'] span::text").extract_first()
        days_on_trulia = response.css("ul[data-testid='home-features'] li::text").re_first('(\d{1,})\+? (?:Day|Days) on Trulia')
        picture_url = response.css("div[data-testid='hdp-hero-img-tile'] picture source::attr(srcset)").extract_first()
        
        list_date_str = None
        if days_on_trulia:  
            list_date = datetime.today() - timedelta(int(days_on_trulia))
            list_date_str = datetime.strftime(list_date, "%m/%d/%Y")
          
        # Sometimes the box for viewing the SOLD date is used for a "NEW" listing tag
        try:
            sold_date_str = None
            if sold_date:    
                sold_date = datetime.strptime(sold_date, "%b %d, %Y")
                sold_date_str = datetime.strftime(sold_date, "%m/%d/%Y")
        except ValueError:
            pass
            
        todays_date = datetime.strftime(datetime.today(), "%m/%d/%Y")

        print("-------------------------------------------------")
        print(status)
        print(address)
        print(city_state)
        print(price)
        print(days_on_trulia)
        print(list_date_str)
        print(sold_date)
        print(sold_date_str)

        
        
        if status == "FOR SALE":
            self.submit_for_sale(url, address, city_state, price, list_date_str)
        elif status == "PENDING":
            self.submit_pending(url, address, city_state, price, todays_date)
        elif status == "OFF MARKET":
            self.submit_off_market(url, address, city_state, price, todays_date)
        elif status == "SOLD":
            self.submit_sold(url, address, city_state, price, sold_date_str, todays_date)
            
            
        # Adding Formulas
        target_index = self.worksheet_df[self.worksheet_df["URL"] == url].index
        worksheet_index = target_index.values[0] + 2
        self.worksheet_df.at[target_index, 'Picture'] = '=IMAGE(\"{}\")'.format(picture_url)
        self.worksheet_df.at[target_index, 'Days before pending'] = "=MINUS(G{},F{})".format(worksheet_index, worksheet_index)
        self.worksheet_df.at[target_index, 'Days to close'] = "=MINUS(I{},F{})".format(worksheet_index, worksheet_index)
        self.worksheet_df.at[target_index, 'Difference in List & Estimate Price'] = "=MINUS(H{},E{})".format(worksheet_index, worksheet_index)
        self.worksheet_df.at[target_index, 'Difference in List & Sold Price'] = "=MINUS(L{},E{})".format(worksheet_index, worksheet_index)
        
        

    def closed(self, reason):
        
        # Update Google Sheets with updated dataframe
        self.worksheet_df = self.worksheet_df.replace(np.nan, '')     
        print(self.worksheet_df)        
        self.worksheet.update([self.worksheet_df.columns.values.tolist()] + self.worksheet_df.values.tolist(), value_input_option='USER_ENTERED')          

        print(self.worksheet_df.values.tolist())