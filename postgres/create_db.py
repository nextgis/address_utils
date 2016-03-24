
import sys
import csv

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from models import Base
from models import Name, Addrobj



def _import(session):

   
    reader = csv.reader(open('ADDROBJ.csv'))
    next(reader, None)  # skip the headers
    
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
            session.add(name)
            session.commit()
        except:
            session.rollback()
        try:
            name = Name(
                name=offname,
                name_tsvect=offname,
                name_tsquery=offname)
            session.add(name)
            session.commit()
        except:
            session.rollback()
        
        names = session.query(Name).filter(Name.name.in_([formalname, offname]))
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
            enddate=enddate,
            formalname=formalname,
            ifnsfl=ifnsfl,
            ifnsul=ifnsul,
            nextid=nextid,
            offname=offname,
            okato=okato,
            oktmo=oktmo,
            operstatus=operstatus,
            parentguid=parentguid,
            placecode=placecode,
            plaincode=plaincode,
            postalcode=postalcode,
            previd=previd,
            regioncode=regioncode,
            shortname=shortname,
            startdate=startdate,
            streetcode=streetcode,
            terrifnsfl=terrifnsfl,
            terrifnsul=terrifnsul,
            updatedate=updatedate,
            ctarcode=ctarcode,
            extrcode=extrcode,
            sextcode=sextcode,
            livestatus=livestatus,
            normdoc=normdoc,
            names=names)
            
        session.add(place)
        session.commit()
        
        i += 1
        if i % 100 == 0:
            print i
        
    
def main():
    connection_string = sys.argv[1]
    # connection_string = 'postgresql://dima:@localhost:5432/address'
    engine = create_engine(connection_string)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
 
    _import(session)
    
    

if __name__ == "__main__":
    main()