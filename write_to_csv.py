###Scrapear links y guardarlos en un .txt###

from dataclasses import replace
import requests #peticion al servidor
import csv
import lxml.html as html
from tqdm import tqdm

url_root = 'https://www.metacritic.com'
url_padre = f'{url_root}/browse/games/release-date/available/xbox-series-x/metascore?page=0'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()' #ultima pagina de la lista de paginas
title = '//div[@class="product_title"]/a[@class="hover_none"]/h1/text()'
metascore = '//span[@itemprop="ratingValue"]//text()'
userscore = '//div[@class="score_summary"]//a[@class="metascore_anchor"]//div[@class="metascore_w user large game positive" or @class="metascore_w user large game mixed" or @class="metascore_w user large game negative" or @class="metascore_w user large game tbd"]/text()'
date = '//li[@class="summary_detail release_data"]//span[@class="data"]//text()'
platform = '//span[@class="platform"]/a//text()'
genre = '//li[@class="summary_detail product_genre"]/span[@class="data"]/text()'

#requests y session
s = requests.Session() #pasa a trabajar con sesiones por ende mas rapido hace los requests
r = s.get(url_padre, headers=header) #headers evita error 400
home = r.content.decode('utf-8')
parser = html.fromstring(home)

total_genres = []
total_genres_aux = []
# #busca la ultima pagina
# last_page = parser.xpath(last_)
# last_page = int(last_page[0])

def get_genres(url):
    global total_genres
    global total_genres_aux

    r = s.get(url, headers=header) #headers evita error 400
    home = r.content.decode('utf-8')
    parser = html.fromstring(home)

    genres = parser.xpath(genre)
    genres_aux = set(genres) #se convierte en set para eliminar duplicados
    genres = list(sorted(genres_aux)) #se convierte el set en una lista y se guarda en genres
    total_genres.append(genres)

    total_genres_aux = sorted(list(set(sum(total_genres, []))))


def get_data(url,aux):
    data = []
    # global total_genres
    r = s.get(url, headers=header) #headers evita error 400
    home = r.content.decode('utf-8')
    parser = html.fromstring(home)

    #scrapear cada dato limpio
    titles = parser.xpath(title)
    dates = parser.xpath(date)
    metascores = parser.xpath(metascore)
    userscores = parser.xpath(userscore)
    platforms = parser.xpath(platform)
    platforms_strip = [x.strip() for x in platforms]
    genres = parser.xpath(genre)
    genres_aux = set(genres) #se convierte en set para eliminar duplicados
    genres = list(sorted(genres_aux)) #se convierte el set en una lista y se guarda en genres

    #agregar cada dato a la lista
    data.append(platforms_strip[0])
    data.append(titles[0])
    data.append(dates[0])
    data.append(metascores[0])
    data.append(userscores[0])
    data.append(genres)
    

    for _ in aux:
        if _ in genres:
            data.append(True)
        else:
            data.append(False)

    # for x in genres:
    #     for _ in aux:
    #         if x == _:
    #             data.append(1)
    #             continue
    #         else:
    #             data.append(0)
    #             continue
    return data

#abre un nuevo archivo .txt con los links y escribe los datos scrapeados del loop
with open('xbox-series-x_links.txt', 'r') as txt: #abre el .txt en modo lectura
    links = txt.readlines() #lee cada linea y los guarda como una lista
    with open('xbox-series-x.csv', 'w', newline='') as file: #crea un .csv para guardar la data en modo escritura
        counter = int() #contador de juegos scrapeados
        writer = csv.writer(file)
        for _ in tqdm(links):
            get_genres(_)
            link = _.replace('\n', '')
            counter += 1
            # writer.writerow(get_data(link)) #escribe una linea por cada return de la funcion get_data()
        print(counter)
        columns = ['Platform','Title','Date','Metascore','Userscore','Genre'] + total_genres_aux #genero lista del encabezado
        writer.writerow(columns) #escribe la linea del encabezado en el .csv
        for link in tqdm(links):
            writer.writerow(get_data(link,total_genres_aux)) #escribe una linea por cada return de la funcion get_data()
        print(columns)