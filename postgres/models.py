from sqlalchemy import Table, Column, ForeignKey, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from sqlalchemy import types
from sqlalchemy import func

from sqlalchemy.dialects.postgresql import TSVECTOR

Base = declarative_base()


class TSQUERY(types.UserDefinedType):
    def get_col_spec(self):
        return 'TSQUERY'

    def bind_expression(self, bindvalue):
        if bindvalue is None:
            bindvalue = ''
        return func.plainto_tsquery('ru', bindvalue)

    def column_expression(self, col):
        return col



placenames_table = Table(
    'addr_names', Base.metadata,
    Column('addrobj_id', Integer, ForeignKey('addrobj.id')),
    Column('name_id', Integer, ForeignKey('name.id'))
)

class Addrobj(Base):
    __tablename__ = 'addrobj'
    id = Column(Integer, primary_key=True)
    actstatus = Column(Integer)
    aoguid = Column(Text, index=True)
    aoid = Column(Text, unique=True, index=True)
    aolevel = Column(Integer, index=True)
    areacode = Column(Integer)
    autocode = Column(Integer)
    centstatus = Column(Integer)
    citycode = Column(Integer)
    code = Column(Text)
    currstatus = Column(Integer)
    enddate = Column(Text)
    formalname = Column(Text)
    ifnsfl = Column(Text)
    ifnsul = Column(Text)
    nextid = Column(Text)
    offname = Column(Text)
    okato = Column(Text)
    oktmo = Column(Text)
    operstatus = Column(Text)
    parentguid = Column(Text)
    placecode = Column(Text)
    plaincode = Column(Text)
    postalcode = Column(Text)
    previd = Column(Text)
    regioncode = Column(Text)
    shortname = Column(Text)
    startdate = Column(Text)
    streetcode = Column(Text)
    terrifnsfl = Column(Text)
    terrifnsul = Column(Text)
    updatedate = Column(Text)
    ctarcode = Column(Text)
    extrcode = Column(Text)
    sextcode = Column(Text)
    livestatus = Column(Text)
    normdoc = Column(Text)
    
    names = relationship('Name', secondary=placenames_table, backref='place')
 
 
class Name(Base):
    __tablename__ = 'name'
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    name_tsvect = Column(TSVECTOR)
    name_tsquery = Column(TSQUERY)
    
 



###################################################
