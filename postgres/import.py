#!/usr/bin/env python


import csv

import psycopg2

conn = psycopg2.connect('dbname=address user=dima password=********')
c = conn.cursor()


sql = 'CREATE TABLE addrobj (actstatus integer,aoguid text,aoid text,aolevel integer,areacode integer,autocode integer,centstatus integer,citycode integer,code text,currstatus integer,enddate text,formalname text,ifnsfl text,ifnsul text,nextid text,offname text,okato text,oktmo text,operstatus text,parentguid text,placecode text,plaincode text,postalcode text,previd text,regioncode text,shortname text,startdate text,streetcode text,terrifnsfl text,terrifnsul text,updatedate text,ctarcode text,extrcode text,sextcode text,livestatus text,normdoc text);'
c.execute(sql)
conn.commit()


reader = csv.reader(open('ADDROBJ.csv'))
next(reader, None)  # skip the headers

i = 0

for line in reader:
    try:
        line = [s.replace("'", "''") for s in line]
        data = "'" + "', '".join(line) + "'"
        sql = 'INSERT INTO addrobj VALUES (%s)' % (data, )
        c.execute(sql)
    except:
        print data
        print sql
        raise
    i += 1
    if i % 1000  == 0:
        print i
        conn.commit()

print i

sql = 'CREATE INDEX aoguid_idx ON addrobj (aoguid);'
c.execute(sql)
conn.commit()

sql = 'CREATE UNIQUE INDEX aoid_idx ON addrobj (aoid);'
c.execute(sql)
conn.commit()

sql = 'CREATE INDEX aolevel_idx ON addrobj (aolevel);'
c.execute(sql)
conn.commit()

sql = 'CREATE INDEX parentguid_idx ON addrobj (parentguid);'
c.execute(sql)
conn.commit()


sql = """ALTER TABLE addrobj ADD COLUMN ts_name tsvector;
UPDATE addrobj SET ts_name =
  to_tsvector('ru', coalesce(formalname,'') || ' ' || coalesce(offname,''));
CREATE INDEX ts_name_idx ON addrobj USING gin(ts_name);
"""
c.execute(sql)
conn.commit()

sql = """CREATE TEXT SEARCH DICTIONARY russian_ispell (
    TEMPLATE = ispell,
    DictFile = russian,
    AffFile = russian,
    StopWords = russian
);
CREATE TEXT SEARCH CONFIGURATION ru (COPY=russian);
ALTER TEXT SEARCH CONFIGURATION ru ALTER MAPPING FOR hword, hword_part, word WITH russian_stem;
"""
c.execute(sql)
conn.commit()

sql = "CREATE INDEX formalname_idx ON addrobj USING gin(to_tsvector('ru', formalname));"
c.execute(sql)
conn.commit()


# sql = ''
# c.execute(sql)
# conn.commit()



conn.close()
