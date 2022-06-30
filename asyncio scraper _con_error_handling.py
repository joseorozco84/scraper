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
from fake_useragent import UserAgent
import sys


start_time = time.time() #inicia contador de tiempo

#inicializa variables & XPATH
ua = UserAgent() #fake random user agent
all_platforms = ['pc', 'ps5', 'xbox-series-x', 'xboxone', 'ps4', 'switch']
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()'
url_padre = f'https://www.metacritic.com/browse/games/release-date/available/ps5/metascore?page=0'
url_root = 'https://www.metacritic.com'
header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
title = '//div[@class="product_title"]/a[@class="hover_none"]/h1/text()'
date = '//li[@class="summary_detail release_data"]//span[@class="data"]//text()'
platform = '//span[@class="platform"]/a//text()'
metascore = '//span[@itemprop="ratingValue"]//text()'
userscore = '//div[@class="score_summary"]//a[@class="metascore_anchor"]//div[@class="metascore_w user large game positive" or @class="metascore_w user large game mixed" or @class="metascore_w user large game negative" or @class="metascore_w user large game tbd"]/text()'
genre = '//li[@class="summary_detail product_genre"]/span[@class="data"]/text()'
company = '//li[@class="summary_detail publisher" or @class="summary_detail developer"]//span[@class="data"]/a/text()'
summary = '//*[@id="main"]/div/div[1]/div[1]/div[3]/div/div/div[2]/div[2]/div[1]/ul/li/span[2]/span/span[2]/text()'


#FUNCION async para obtener la lista completa de links por pagina
async def get_link(session, url): #recibe parametro de sesion y url
    links_titles = []
    header = {'User-Agent': ua.random} #fake a random user agent
    async with session.get(url, headers=header) as response:
        try:
            r = await response.text()
            parser = html.fromstring(r)
            #busca y guarda los links
            links_titles = parser.xpath(link_title)
            links_titles = [url_root + x for x in links_titles] #corrige los links sumandole url_rool
        except (ParserError, ParseError, IndexError) as error:
            print('Error en funcion get_link')
            print(error)
        pass
    return links_titles

links_offline = []

#FUNCION async para scrapear por link y devolver diccionario
async def scrap_link(session, url): #recibe parametro de sesion y url
    await asyncio.sleep(8) #espera algunos segundos para bajar la velocidad
    content_dict = {} #inicializa el diccionario
    companies_strip = []
    global links_offline    
    header = {'User-Agent': ua.random} #fake a random user agent
    async with session.get(url, headers=header) as response:
        try:    
            r = await response.text()
            parser = html.fromstring(r)
            #busca y guarda los datos segun categoria
            titles = parser.xpath(title)
            summaries = parser.xpath(summary)
            dates = parser.xpath(date)
            genres = parser.xpath(genre)
            #se convierte la lista en un set auxiliar para borrar los duplicados
            genres_aux = set(genres)
            #se pasan los datos limpios sin duplicar de nuevo a la lista
            genres = list(sorted(genres_aux))
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
            content_dict['Summary'] = summaries[0]
        except (ParserError, ParseError, IndexError) as error:
            # print('Error en funcion scrap_link')
            # print(error,url)
            links_offline.append(url)
            pass
    return content_dict

#FUNCION async para obtener el diccionario completo usando funcion get_link() y scrap_link()
async def scrap_em_all():
    link_task = [] #tareas para asyncio
    scrap_task = [] #tareas para asyncio
    actual_page = 0
    links_list = []
    async with aiohttp.ClientSession() as session: #inicia la sesion
        async with session.get(url_padre, headers=header) as response:
            r = await response.text()
            parser = html.fromstring(r)
            #busca y guarda la ultima pagina
            last_page = parser.xpath(last_)
            last_page = int(last_page[0])
            #inicia tarea para completar la lista de links
            for _ in tqdm(range(last_page)):
                url_start = f'{url_root}/browse/games/release-date/available/ps5/metascore?page={actual_page}'
                task = asyncio.ensure_future(get_link(session, url_start))
                link_task.append(task)
                actual_page +=1
            results = await asyncio.gather(*link_task)
            links_list = sum(results, []) #convierte una [lista de [listas] en una [lista]
            #inicia tarea para scrapear la lista de links
            for link in tqdm(links_list):
                task = asyncio.ensure_future(scrap_link(session, link))
                await asyncio.sleep(1/50) #create a Future instance every 1/50 second
                scrap_task.append(task)
            results = await asyncio.gather(*scrap_task)
            #pasa los resultados de las tareas al diccionario
            content_dict = results
    return content_dict


# Set the policy to prevent "Event loop is closed" error on Windows - https://github.com/encode/httpx/issues/914
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

#crea el dataframe
# df = pd.DataFrame(asyncio.get_event_loop().run_until_complete(scrap_em_all()))
df = pd.DataFrame(asyncio.run(scrap_em_all()))
df1 = df.dropna(axis=0) #elimina las lineas vacias (NaN)

print(df1)
#guarda el contenido del dataframe en un archivo
file_name = 'metacritic_game_ranking_ps5.csv'
df1.to_csv(file_name, index=False, encoding='utf-8-sig')
print(f'Dataframe guardado en {file_name}')
print(f'Links offline: {len(links_offline)}')
end_time = time.time() #finaliza contador de tiempo
print(f'{len(df1)} juegos scrapeados en {round(end_time - start_time,2)} segundos') #imprime cuanto tarda en ejecutar
