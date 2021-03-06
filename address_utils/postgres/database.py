# encoding: utf-8

import csv

from sqlalchemy import create_engine


from models import Base, DBSession

# TODO: fix connection string
connection_string = 'postgresql://geocoder:@localhost:5432/geocoder'
engine = create_engine(connection_string)

Base.metadata.bind = engine
DBSession.configure(bind=engine)


def _initdb(csvfilename, drop_db=False):
    session = DBSession()

    from models import Name, Addrobj

    # if drop_db:
    #    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    reader = csv.reader(open(csvfilename))
    # next(reader, None)  # skip the headers

    i = 0
    for line in reader:
        line = [s.replace("'", "''") for s in line]
        (
            actstatus,
            aoguid,
            aoid,
            aolevel,
            areacode,
            autocode,
            centstatus,
            citycode,
            code,
            currstatus,
            enddate,
            formalname,
            ifnsfl,
            ifnsul,
            nextid,
            offname,
            okato,
            oktmo,
            operstatus,
            parentguid,
            placecode,
            plaincode,
            postalcode,
            previd,
            regioncode,
            shortname,
            startdate,
            streetcode,
            terrifnsfl,
            terrifnsul,
            updatedate,
            ctarcode,
            extrcode,
            sextcode,
            livestatus,
            normdoc
        ) = line

        try:
            name = Name(
                name=formalname,
                name_tsvect=formalname,
                name_tsquery=formalname)
            ts_query1 = name.name_tsquery
            session.add(name)
            session.commit()
        except:
            session.rollback()
        try:
            name = Name(
                name=offname,
                name_tsvect=offname,
                name_tsquery=offname)
            ts_query2 = name.name_tsquery
            session.add(name)
            session.commit()
        except:
            session.rollback()

        # Different names can be translated to one name_tsquery
        # so we search name_tsquery to prevent storing of small differences
        # in the DB
        names = session.query(Name).filter(Name.name_tsquery.in_([
            ts_query1, ts_query2
        ]))
        names = names.all()

        place = Addrobj(
            actstatus=actstatus,
            aoguid=aoguid,
            aoid=aoid,
            aolevel=aolevel,
            areacode=areacode,
            autocode=autocode,
            centstatus=centstatus,
            citycode=citycode,
            code=code,
            currstatus=currstatus,
            formalname=formalname,
            ifnsfl=ifnsfl,
            ifnsul=ifnsul,
            nextid=nextid,
            offname=offname,
            operstatus=operstatus,
            parentguid=parentguid,
            placecode=placecode,
            plaincode=plaincode,
            postalcode=postalcode,
            previd=previd,
            regioncode=regioncode,
            shortname=shortname,
            streetcode=streetcode,
            terrifnsfl=terrifnsfl,
            terrifnsul=terrifnsul,
            ctarcode=ctarcode,
            extrcode=extrcode,
            sextcode=sextcode,
            livestatus=livestatus,
            names=names)

        session.add(place)
        session.commit()

        i += 1
        if i % 10000 == 0:
            print i


if __name__ == "__main__":
    import sys

    filename = sys.argv[1]
    _initdb(filename)

    text = u"""Посреди предложения встречаются два
    наименования город Казань
    и его часть улица Баумана, остальные слова -
    для заполнения"""

    from models import Name
    # import ipdb; ipdb.set_trace()
    addresses = Name.extract_addresses(text)
    for addr in addresses:
        print addresses[addr], unicode(addr).encode('utf-8')
