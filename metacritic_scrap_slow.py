#importar librerias
from ast import parse
from wsgiref import headers
import requests #peticion al servidor
import lxml.html as html
import pandas as pd
import csv
import time
from tqdm import tqdm #barras de progreso
from memory_profiler import profile



#inciar contador de tiempo
start = time.time()

#inicialize variables & XPATH
plataform_list = ['pc', 'ps5', 'xbox-series-x', 'xboxone', 'ps4', 'switch']
actual_page = 0
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()' #get last number of pages
url_padre = f'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page={actual_page}'
url_root = 'https://www.metacritic.com'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
title = '//td[@class="clamp-summary-wrap"]/a/h3/text()'
platform = '//div[@class="clamp-details"]//span[@class="data"]/text()'
date = '//div[@class="clamp-details"]/span/text()'
metascore = '//div[@class="browse-score-clamp"]/div[@class="clamp-metascore"]//div[@class="metascore_w large game positive" or @class="metascore_w large game mixed" or @class="metascore_w large game negative"]/text()'
userscore = '//div[@class="browse-score-clamp"]/div[@class="clamp-userscore"]//div[@class="metascore_w user large game positive" or @class="metascore_w user large game mixed" or @class="metascore_w user large game negative" or @class="metascore_w user large game tbd"]/text()' #SOLUCIONADO (traia 81 valores)
genre = '//li[@class="summary_detail product_genre"]/span[@class="data"]/text()'

print('\n')
r = requests.get(url_padre, headers=header) #headers evita error 400
home = r.content.decode('utf-8')
parser = html.fromstring(home)

#guarda el numero de la ultima pagina
last_page = parser.xpath(last_)
last_page = int(last_page[-1])

@profile
#funcion web scrap
def get_content(url):

    print('\n')
    content_dict = {} #declaracion de dictionary

    r = requests.get(url, headers=header) #headers evita error 400
    home = r.content.decode('utf-8')
    parser = html.fromstring(home)

    dates = parser.xpath(date)
    content_dict['Fecha'] = dates
    
    platform_list = parser.xpath(platform)
    platforms_stripped = [x.strip() for x in platform_list] #strip de espacios en blanco en los resultados de la lista
    content_dict['Plataforma']=platforms_stripped
    
    titles = parser.xpath(title)
    content_dict['Title']=titles
    
    titles_url = parser.xpath(link_title)
    titles_url = [url_root+x for x in titles_url]
    content_dict['Link']=titles_url
        
    metascores = parser.xpath(metascore)
    content_dict['Metascore']=metascores
    
    userscores = parser.xpath(userscore)
    content_dict['Userscore']=userscores

    #busca en la [lista de links] de cada titulo y trae el genero
    genres_list = []
    for x in tqdm(titles_url, desc=f'Scraping page {index+1}/{last_page}', ncols=100, dynamic_ncols=False, ascii="▒▓█"):
        r = requests.get(x, headers=header) #headers evita error 400
        home = r.content.decode('utf-8')
        parser = html.fromstring(home)
        genres = parser.xpath(genre)
        genres_list.append(genres)
    content_dict['Generos']=genres_list
    
    return content_dict

#lista de links de las paginas
links = []
while actual_page < last_page:
    links.append(f'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page={actual_page}')
    actual_page +=1

data=[]
for index,x in enumerate(tqdm(links, desc='Scrap progress...', ncols=100, dynamic_ncols=False, ascii="▒▓█")):
    #print(f'Scraping page {index+1}/{last_page}', end='\r')
    data.append(get_content(x))

#generar dataframe
df=pd.DataFrame()
for x in data:
    df_uno=pd.DataFrame(x)
    df=pd.concat([df,df_uno])

#finaliza contador de tiempo
end = time.time()

#guardar el dataframe en un archivo csv
df.to_csv('metacritic_game_ranking_ps5.csv', index=False)

print(df.head)

#Imprime cuanto duro el scrap
print(f'The scrap lasted {round((end - start),2)} seconds')