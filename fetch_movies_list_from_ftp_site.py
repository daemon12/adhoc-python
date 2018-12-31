import urllib2
import re

urls = ["http://dl5.filmha.co/2018/film/khareji/"]
movies = list()
counter = 0
while len(urls) != 0:
    print("\nNumber of urls: %s" % len(urls))
    mylurls = list()
    for url in urls:
        print".",
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

