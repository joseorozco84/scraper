#importar librerias
from ast import parse
from wsgiref import headers
import requests #peticion al servidor
import lxml.html as html
import pandas as pd
from tqdm.auto import tqdm #barras de progreso
from lxml.etree import ParseError
from lxml.etree import ParserError
import csv
from fake_useragent import UserAgent

ua = UserAgent()

#inicialize variables & XPATH
url_padre = f'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=0' #inicializar en 0
url_root = 'https://www.metacritic.com'
header = {'User-Agent': ua.random}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
genre = '//li[@class="summary_detail product_genre"]/span[@class="data"]/text()'

s = requests.Session() #pasa a trabajar con sesiones por ende mas rapido hace los requests
r = s.get(url_padre, headers=header) #headers evita error 400
home = r.content.decode('utf-8')
parser = html.fromstring(home)

titles_url = parser.xpath(link_title)
titles_url = [url_root+x for x in titles_url]

genres_list = []

links_offline = []
def get_genres(): 
    for x in tqdm(titles_url):
        header = {'User-Agent': ua.random}
        try:
            r = s.get(x, headers=header) #headers evita error 400
            if (r == ''):
                print(f'Status Code: {r.status_code} - {x}')
                links_offline.append(x)
                pass
            home = r.content.decode('utf-8')
            parser = html.fromstring(home)
            genres = parser.xpath(genre)
            genres_aux = set(genres)
            genres = list(genres_aux)
            genres_list.append(genres)
        except (ParserError, ParseError, IndexError) as error:
            print(f'\n{error} {r.status_code}')
            links_offline.append(x)
            continue
    return genres_list
df=pd.DataFrame(get_genres())
print(df)
print(links_offline)
df.to_csv('metacritic_game_ranking_test.csv', index=False)