import csv
import sys


with open(sys.argv[1], mode ='r') as f:
    reader = csv.reader(f)
    result = []
    for row in reader:
        # tmp = 'http://www.wikidata.org/entity/' + row[0].replace('w','')
        # row[0] = 'http://www.wikidata.org/entity/' + row[0].replace('h','')
        row[0] = row[0].replace('h','') + 'b'
        # row[1] = tmp
        #row[2] = ''
        row[1] = 'http://example.com/country/id/' + row[0]


        result.append(row)

with open('t.csv', mode = 'w') as g:
    writer = csv.writer(g)
    writer.writerows(result)
    