#importar librerias
from wsgiref import headers
import lxml.html as html
from lxml.etree import ParseError
from lxml.etree import ParserError
import pandas as pd #dataframe
import csv
import time
from tqdm.auto import tqdm #barras de progreso
import asyncio
import aiohttp

start_time = time.time() #inicia contador de tiempo

#inicializa variables & XPATH
all_platforms = ['pc', 'ps5', 'xbox-series-x', 'xboxone', 'ps4', 'switch']
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()'
url_padre = f'https://www.metacritic.com/browse/games/release-date/available/xbox-series-x/metascore?page=0'
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

#FUNCION async para obtener la lista completa de links por pagina
async def get_link(session, url): #recibe parametro de sesion y url
    async with session.get(url, headers=header) as response:
        response = await response.text()
        parser = html.fromstring(response)
        #busca y guarda los links
        links_titles = parser.xpath(link_title)
        links_titles = [url_root + x for x in links_titles] #corrige los links sumandole url_rool
    return links_titles

#FUNCION async para scrapear por link y devolver diccionario
async def scrap_link(session, url): #recibe parametro de sesion y url
    content_dict = {} #inicializa el diccionario
    companies_strip = []
    async with session.get(url, headers=header) as response:
        print(f'{url} {response.status}')
        response = await response.text()
        parser = html.fromstring(response)
        #busca y guarda los datos segun categoria
        titles = parser.xpath(title)
        dates = parser.xpath(date)
        genres = parser.xpath(genre)
        companies = parser.xpath(company)
        if companies:
            companies_strip = [x.strip() for x in companies] #quita espacios en blanco en los items de la lista
        else:
            companies_strip.append('?')
        metascores = parser.xpath(metascore)
        if metascores: #si metascores contiene algo (si es verdadero), lo apenda a la lista
            metascores.append(metascores)
        else: #si esta vacio, le apenda "tbd" a la lista
            metascores.append('tbd')
        userscores = parser.xpath(userscore)
        if userscores: #si userscores contiene algo (si es verdadero), lo apenda a la lista
            userscores.append(userscores)
        else: #si esta vacio, le apenda "tbd" a la lista
            userscores.append('tbd')
        platforms = parser.xpath(platform)
        platforms_strip = [x.strip() for x in platforms] #quita espacios en blanco en los items de la lista
        #inicia llenado de dicionario
        content_dict['Platform'] = platforms_strip[0]
        content_dict['Date'] = dates[0]
        content_dict['Title'] = titles[0]
        content_dict['Genre'] = genres
        content_dict['Company'] = companies_strip[0]
        content_dict['Metascore'] = metascores[0]
        content_dict['Userscore'] = userscores[0]
    return content_dict

#FUNCION async para obtener el diccionario comleto usando funcion get_link() y scrap_link()
async def scrap_em_all():
    link_task = [] #tareas para asyncio
    scrap_task = [] #tareas para asyncio
    actual_page = 0
    links_list = []
    async with aiohttp.ClientSession() as session: #inicia la sesion
        async with session.get(url_padre, headers=header) as response:
            response = await response.text()
            parser = html.fromstring(response)
            #busca y guarda la ultima pagina
            last_page = parser.xpath(last_)
            last_page = int(last_page[0])
            #inicia tarea para completar la lista de links
            for _ in tqdm(range(last_page)):
                url_start = f'{url_root}/browse/games/release-date/available/xbox-series-x/metascore?page={actual_page}'
                task = asyncio.ensure_future(get_link(session, url_start))
                link_task.append(task)
                actual_page +=1
            results = await asyncio.gather(*link_task)
            links_list = sum(results, []) #convierte una [lista de [listas] en una [lista]
            #inicia tarea para scrapear la lista de links
            for link in tqdm(links_list):
                task = asyncio.ensure_future(scrap_link(session, link))
                scrap_task.append(task)
            results = await asyncio.gather(*scrap_task)
            content_dict = results
    return content_dict

#crea el dataframe
df = pd.DataFrame(asyncio.get_event_loop().run_until_complete(scrap_em_all()))

print(df)
#guarda el contenido del dataframe en un archivo
df.to_csv('metacritic_game_ranking_xbox-series-x.csv', index=False)
print('Dataframe guardado en .csv')

end_time = time.time() #finaliza contador de tiempo
print(f'{len(df)} juegos scrapeados en {round(end_time - start_time,2)} segundos') #imprime cuanto tarda en ejecutar