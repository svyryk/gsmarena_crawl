import requests
import urllib
import time
from bs4 import BeautifulSoup
import sys, os
import cx_Oracle


headers = {
'User-Agent': '*'
}


def crawler(seed):
    frontier = [seed]
    crawled = []
    brands =[]
    start = seed
    numparsed = 0
    default = 'http://www.gsmarena.com/'
    #i = 1
    con = cx_Oracle.connect('system/q1w2e3r4@localhost/orcl.local')
    print(con.version)
    cur = con.cursor()
    cur.execute('select page from ddw_phone_model')
    for result in cur:
        crawled.append(result[0])
        numparsed = numparsed + 1
    cur.close()

    while frontier:
        page = frontier.pop()
        try:
            if page not in crawled:
                print('Crawled:' + page)
                time.sleep(0.3)
                source = requests.get(page).text
                soup = BeautifulSoup(source, "html5lib")
                links = soup.findAll('a', href=True)
                numdevs = 0
                if page == start:
                    makesdiv = soup.find("div", {"class": "st-text"})
                    links = makesdiv.findAll('a')
                    for link in links:
                        devs = link.find('span').next
                        numdevs = numdevs+int(devs[0:devs.find(' ')])
                        if urllib.parse.urljoin(start, link['href']) not in crawled:
                            frontier.append(urllib.parse.urljoin(default, link['href']))
                    print(numdevs)
                else:
                    makersdiv = soup.find("div", {"class": "makers"})
                    if makersdiv == None:
                        #parse phone
                        numparsed = numparsed+1
                        print('Parsed: '+' '+page+' '+str(numparsed))

                        title = soup.find("title")
                        model = soup.find("h1", {"class": "specs-phone-name-title"})
                        imgTag = soup.find("div", {"class": "specs-photo-main"})
                        img = imgTag.find('img')['src']
                        chipset = ''
                        memory = ''
                        camera = ''
                        specs = soup.find('div', {"id": "specs-list"})
                        groups = specs.findAll('table')
                        for group in groups:
                            if group.find('th').next == 'Platform':
                                specrows = group.findAll('tr')
                                for row in specrows:
                                    if row.find('td', {"class": "ttl"}).find('a').next == 'CPU':
                                        chipset = chipset+' '+row.find('td', {"class": "nfo"}).next
                                    if row.find('td', {"class": "ttl"}).find('a').next == 'Chipset':
                                        chipset = chipset+row.find('td', {"class": "nfo"}).next
                            if group.find('th').next == 'Memory':
                                specrows = group.findAll('tr')
                                for row in specrows:
                                    if row.find('td', {"class": "ttl"}).find('a').next == 'Internal':
                                        memory = memory+row.find('td', {"class": "nfo"}).next
                            if group.find('th').next == 'Camera':
                                specrows = group.findAll('tr')
                                for row in specrows:
                                    if row.find('td', {"class": "ttl"}).find('a').next == 'Primary':
                                        camera = camera+row.find('td', {"class": "nfo"}).next

                        cur = con.cursor()
                        statement = 'insert into DDW_PHONE_MODEL(PM_ID, Title, Page, Model, ImgRef, chipset, memory, camera) values (DDW_PM_SEQ.nextval, :1, :2, :3, :4, :5, :6, :7)'
                        cur.execute(statement, (title.next, page, model.next, img, chipset, memory, camera))
                        con.commit()
                    else:
                        #phone list
                        links = makersdiv.findAll('a')
                        for link in links:
                            if urllib.parse.urljoin(start, link['href']) not in crawled:
                                frontier.append(urllib.parse.urljoin(default, link['href']))
                        pagesdiv = soup.find("div", {"class": "nav-pages"})
                        links = pagesdiv.findAll('a')
                        for link in links:
                            if urllib.parse.urljoin(start, link['href']) not in crawled:
                                frontier.append(urllib.parse.urljoin(default, link['href']))
                    crawled.append(page)


        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, e)

    #i += 1
    #if i == 10:
    # break
    con.close()
    return crawled


crawler('http://www.gsmarena.com/makers.php3')