import scrapy
import json
import parse as par
from urllib.parse import urlencode

def get_zenrows_api_url(url, api_key):
    params = {
        "url": url,            
        "js_render": "true",      
        "premium_proxy": "true"   
    }

    api_url = f"https://api.zenrows.com/v1/?apikey={api_key}&{urlencode(params)}"
    return api_url

class collect_player_info(scrapy.Spider):
  name='players_info'
  api_key = ""
  def __init__(self, api_key):
    if api_key:
      self.api_key = api_key
    try:
      with open('./dataset/players_urls.json') as f:
        self.players = json.load(f)
      self.player_count = 0
    except IOError:
      print("File not found")

  def start_requests(self):
    urls = ['https://sofifa.com/player/{}?units=mks'.format(self.players[self.player_count]['player_url'])]
    for url in urls:
      api_url = get_zenrows_api_url(url, self.api_key)
      yield scrapy.Request(url = api_url, callback = self.parse)
  
  def parse(self, response):
      info_grid = response.css("article div.grid")  # List of grid-blocks that contains player information

      # Profile
      name = response.css("div.profile.clearfix h1::text").get()
      text = response.css("div.profile.clearfix p::text").getall()[-1]      # Text contains player age, birthday, height
      par_text = par.parse("{}y.o. ({}) {}cm / {} {}kg / {}lbs", text)      # Extract value from string
      (age, birthday, height), weight = par_text[0:3], par_text[4]          # Basic information
      positions = response.css("div.profile.clearfix p span::text").getall()

      player_dictionary = {'id' : self.players[self.player_count]['player_url'],
                           'name' : name,  
                           'positions' : positions, 
                           'age' : age, 
                           'birth_date' : birthday, 
                           'height' : height, 
                           'weight' : weight
                           }
      
      # More details
      profile = info_grid[1].css("div.col")[0]
      profile_cols = profile.css("p label::text").getall()
      profile_vals = list(filter(lambda a: a != ' ', profile.css("p::text").getall()))

      profile_dictionary = dict(zip(profile_cols, profile_vals))

      # Player specialities
      specialities = info_grid[1].css("div.col")[1].css("p a::text").getall()

      specialities_dict = {'Player specialities' : specialities}

      # Teams
      club = info_grid[1].css("div.col")[2].css("p a::text").get()            # Club
      club_value = info_grid[1].css("div.col")[2].css("p::text").get()        # Club
      nation_team = info_grid[1].css("div.col")[3].css("p a::text").get()     # National team
      nation_team_value = info_grid[1].css("div.col")[3].css("p::text").get() # National team

      team_dictionary = {
        'teams' : {
            club : club_value, 
            nation_team : nation_team_value
        }
      }

      # Overall attributes
      o_attributes = info_grid.css("div.col div.sub::text").getall()          # Overall
      o_values = info_grid[0].css("div.col em::text").getall()                # Overall attribute's parameters

      overall_dictionary = dict(zip(o_attributes, o_values))

      # Described attributes
      div_att = info_grid[3:5].css("div.col")                                 # List of div contains detailed attributes
      d_attributes = [  list(filter(lambda a: ('+' not in a) and ('-' not in a), div_att[i].css("p span::text").getall())) 
                        for i in range(len(div_att))]                                         # list attributes 
      d_values = [div_att[i].css("em::text").getall() for i in range(len(div_att))]           # attribute value

      attribute_dict = {div_att[i].css("h5::text").get() : dict(zip(d_attributes[i], d_values[i])) for i in range(len(div_att) - 1)}
      if d_attributes[-1]:
        attribute_dict.update({div_att[-1].css("h5::text").get() : d_attributes[-1]})                 # Handle 'PlayStyles' list 

      # Write player information to json
      player_dictionary.update(overall_dictionary)
      player_dictionary.update(profile_dictionary)
      player_dictionary.update(team_dictionary)
      player_dictionary.update(specialities_dict)
      player_dictionary.update(attribute_dict)
      yield player_dictionary

      if self.player_count < len(self.players) - 1:
        self.player_count += 1
        next_page_url = 'https://sofifa.com/player/' + self.players[self.player_count]['player_url'] + '?units=mks'
        api_url = get_zenrows_api_url(next_page_url, self.api_key)  
        yield scrapy.Request(url=api_url, callback=self.parse) 