import glob
import csv

import mincemeat

# text_files = glob.glob('/home/diego/HD/Trab2.3/*')
text_files = glob.glob('/media/diego/316695744CF3C06C/hd2/Trab2.3/*')


def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()


source = dict((file_name, file_contents(file_name)) for file_name in text_files)


def mapfn(k, v):
    print('map ' + k.decode('string_escape'))
    from stopwords import allStopWords
    for line in v.splitlines():
        obra, autores, titulo = line.split(":::")
        for autor in autores.split("::"):
            for word in titulo.split():
                if word not in allStopWords and len(word) > 1 and not word.isdigit():
                    import re
                    # x = re.sub("[\.:,!()<>]","",word)
                    x = re.sub("[^a-zA-Z -]", "", word)
                    x = x.strip().lower()
                    yield (autor, x), 1


def reducefn(k, v):
    print("reduce " + str(k).decode('string_escape'))
    return sum(v)


s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("Result.csv", "w"))

for a, p in results:
    w.writerow([a.decode('string_escape'), p.decode('string_escape'), results[(a, p)]])
    #print("%s,%s,%s" % (a.decode('string_escape'), p.decode('string_escape'), results[(a, p)]))
