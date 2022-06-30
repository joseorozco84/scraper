###Scrapear links y guardarlos en un .txt###

import requests #peticion al servidor
import csv
import lxml.html as html
from tqdm import tqdm
from memory_profiler import profile

@profile

url_root = 'https://www.metacritic.com'
url_padre = f'{url_root}/browse/games/release-date/available/pc/metascore?page=0'
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'}
link_title = '//td[@class="clamp-summary-wrap"]/a/@href'
last_ = '//li[@class="page last_page"]/a[@class="page_num"]/text()' #ultima pagina de la lista de paginas

#requests y session
s = requests.Session() #pasa a trabajar con sesiones por ende mas rapido hace los requests
r = s.get(url_padre, headers=header) #headers evita error 400
home = r.content.decode('utf-8')
parser = html.fromstring(home)

#busca la ultima pagina
last_page = parser.xpath(last_)
last_page = int(last_page[0])

#crea un nuevo archivo .txt y escribe los datos scrapeados del loop
with open('pc_links.txt', 'w') as f:
    actual_page = 0
    cantidad = int()
    for x in tqdm(range(last_page)):
        url = f'{url_root}/browse/games/release-date/available/pc/metascore?page={actual_page}'
        r = s.get(url, headers=header) #headers evita error 400
        home = r.content.decode('utf-8')
        parser = html.fromstring(home)
        links_titles = parser.xpath(link_title)
        links_titles = [url_root+x for x in links_titles]
        cantidad += len(links_titles)
        for link in links_titles:
            f.writelines(f'{link}\n') #escribe en el .txt una linea por cada link en la lista
        actual_page += 1
    print(f'{cantidad} scraped links.')