import scrapy
from urllib.parse import urlencode

def get_zenrows_api_url(url, api_key):
    params = {
        "url": url,            
        "js_render": "true",      
        "premium_proxy": "true"   
    }

    api_url = f"https://api.zenrows.com/v1/?apikey={api_key}&{urlencode(params)}"
    return api_url

class collect_player_url(scrapy.Spider):
  name='players_urls' 
  api_key = ""
  def __init__(self, api_key):
    self.count = 0
    if api_key:
      self.api_key = api_key

  def start_requests(self):
    urls = [
            'https://sofifa.com/players?col=oa&sort=desc&offset=0',
    ]
    for url in urls:
      api_url = get_zenrows_api_url(url, self.api_key)
      yield scrapy.Request(url = api_url, callback = self.parse)
    
  
  def parse(self, response):
    player_urls = response.css("td a").re(r'/player/(\w*)/')
    for player_url in player_urls:
      yield{
        'player_url': player_url
      }
    if self.count < 60:
      self.count += 60
      next_page_url = 'https://sofifa.com/players?col=oa&sort=desc&offset=' + str(self.count)
      api_url = get_zenrows_api_url(next_page_url, self.api_key)
      yield scrapy.Request(url=api_url, callback=self.parse) 
