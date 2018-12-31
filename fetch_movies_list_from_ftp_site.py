import urllib2
import re

urls = ["http://dl8.heyserver.in/film/"]
movies = list()
counter = 0
while len(urls) != 0:
    print("Number of urls: %s" % len(urls))
    mylurls = list()
    for url in urls:
        # page = urllib2.urlopen(url)
        page = urllib2.urlopen(url).read()
        links = re.findall(r"<a.*?\s*href=\"(.*?)\".*?>.*?</a>", page)
        for link in links:
            mylink = url + link
            if mylink.endswith('../'):
                pass
            elif mylink.endswith('/'):
                mylurls.append(mylink)
            else:
                movies.append(mylink)
    urls = mylurls

print("Number of movies: %s" % len(movies))
with open('output.txt', 'w') as f:
    for m in movies:
        f.write(m + '\n')

