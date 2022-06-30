#importar librerias
from ast import parse
from calendar import c
from email import contentmanager
from os import link
from concurrent.futures import ThreadPoolExecutor
from wsgiref import headers
import requests #peticion al servidor
import lxml.html as html
from lxml.etree import ParseError
from lxml.etree import ParserError
import pandas as pd #dataframe
import csv
import time
from tqdm.auto import tqdm #barras de progreso

start_time = time.time() #inicia contador de tiempo

#inicialize variables & XPATH
all_platforms = ['pc', 'ps5', 'xbox-series-x', 'xboxone', 'ps4', 'switch']
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()'
url_padre = f'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=0'
url_root = 'https://www.metacritic.com'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
title = '//div[@class="product_title"]/a[@class="hover_none"]/h1/text()'
date = '//li[@class="summary_detail release_data"]//span[@class="data"]//text()'
platform = '//span[@class="platform"]/a//text()'
metascore = '//span[@itemprop="ratingValue"]//text()'
userscore = '//div[@class="score_summary"]//a[@class="metascore_anchor"]//div[@class="metascore_w user large game positive" or @class="metascore_w user large game mixed" or @class="metascore_w user large game negative" or @class="metascore_w user large game tbd"]/text()'
genre = '//li[@class="summary_detail product_genre"]/span[@class="data"]/text()'
company = '//li[@class="summary_detail publisher" or @class="summary_detail developer"]//span[@class="data"]/a/text()'

#requests y session
s = requests.Session() #pasa a trabajar con sesiones por ende mas rapido hace los requests
r = s.get(url_padre, headers=header) #headers evita error 400
home = r.content.decode('utf-8')
parser = html.fromstring(home)

#busca la ultima pagina
last_page = parser.xpath(last_)
last_page = int(last_page[0])

#funcion que devuelve la lista de todos los links para scrapear
actual_page = 0
links_list = []
for x in tqdm(range(last_page)): #00:02
    url_start = f'{url_root}/browse/games/release-date/available/ps5/metascore?page={actual_page}'
    r = s.get(url_start, headers=header) #headers evita error 400
    home = r.content.decode('utf-8')
    parser = html.fromstring(home)
    links_titles = parser.xpath(link_title)
    links_titles = [url_root+x for x in links_titles]
    links_list.extend(links_titles)
    actual_page +=1

#funcion que devuelve el diccionario
#se definen listas y diccionario
content_dict = {}
genres_list = []
dates_list = []
titles_list = []
metascores_list = []
userscores_list = []
platforms_list = []
platforms_list_strip = []
companies_list = []
companies_list_strip = []

links_fail = []

#loop por cada item que devuelve la funcion, y luego trae las listas segun categoria
for x in tqdm(links_list): # 02:31, 1.5it/s(requests) vs 00:51, 4.6it/s(session) lista de ps5 demora 49:47
    try:
        r = s.get(x, headers=header) #headers evita error 400
        if r == '':
            links_fail.append(x)
            print(f'{x} page empty!')
            continue
        home = r.content.decode('utf-8')
        parser = html.fromstring(home)

        if r.status_code > 299: #omite la url offline
            print(f'{x} offline! {r.status_code}')
            links_fail.append(x)
            continue

        dates = parser.xpath(date)
        dates_list.append(dates[0])
                    
        titles = parser.xpath(title)
        titles_list.append(titles[0])
        
        platforms = parser.xpath(platform)
        platforms_list.append(platforms[0])
        
        genres = parser.xpath(genre)
        genres_list.append(genres)

        companies = parser.xpath(company)
        if companies:
            companies_list.append(companies[0])
        else:
            companies_list.append('?')
        
        metascores = parser.xpath(metascore)
        if metascores: #si metascores contiene algo (si es verdadero), lo apenda a la lista
            metascores_list.append(metascores[0])
        else: #si esta vacio, le apenda "tbd" a la lista
            metascores_list.append('tbd')

        userscores = parser.xpath(userscore)
        if userscores: #si userscores contiene algo (si es verdadero), lo apenda a la lista
            userscores_list.append(userscores[0])
        else: #si esta vacio, le apenda "tbd" a la lista
            userscores_list.append('tbd')
    except (ParserError, ParseError) as e:
        print(e)
    pass

#strip de espacios en blanco en las listas
companies_list_strip = [x.strip() for x in companies_list] 
platforms_list_strip = [x.strip() for x in platforms_list]

#se asignan las listas en el diccionario segun nombre
content_dict['Platform'] = platforms_list_strip
content_dict['Date'] = dates_list
content_dict['Title'] = titles_list
content_dict['Genre'] = genres_list
content_dict['Company'] = companies_list_strip
content_dict['Metascore'] = metascores_list
content_dict['Userscore'] = userscores_list
    
#inicializa un dataframe con el contenido que devuelve la funcion get_content()
#la funcion get_content() devuelve un diccionario
df=pd.DataFrame(content_dict)

#guarda el dataframe en un archivo .csv
df.to_csv('metacritic_game_ranking_ps5.csv', index=False)

print(links_fail)
print(df)
print('Dataframe guardado en .csv')

end_time = time.time() #finaliza contador de tiempo
print(f'{len(df)} juegos scrapeados en {round(end_time - start_time,2)} segundos') #imprime cuanto tarda en ejecutar