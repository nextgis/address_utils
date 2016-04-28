# coding=utf-8

from collections import Counter

from sqlalchemy import Table, Column, ForeignKey, Integer, Text
from sqlalchemy import text
from sqlalchemy.orm import relationship, sessionmaker

from sqlalchemy import types
from sqlalchemy import func

from sqlalchemy.dialects.postgresql import TSVECTOR

from sqlalchemy.ext.declarative import declarative_base


from address_utils.address import Address as BaseAddress

DBSession = sessionmaker()
dbsession = DBSession()
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


class Address(BaseAddress):
    def __init__(self,
                 addrobj=None,
                 raw_address=None,
                 index=None,
                 country=None,
                 region=None,
                 city=None,
                 subregion=None,
                 settlement=None,
                 subcity=None,
                 street=None,
                 house=None,
                 poi=None):
        self._addrobj = addrobj
        super(Address, self).\
            __init__(
                 raw_address,
                 index,
                 country,
                 region,
                 city,
                 subregion,
                 settlement,
                 subcity,
                 street,
                 house,
                 poi)

    @property
    def addrobj(self):
        return self._addrobj

    @addrobj.setter
    def addrobj(self, value):
        self._addrobj = value


class AddressParser(object):
    def __init__(self):
        self.bind = Base.metadata.bind

    def extract_addresses(self, searched_text):
        names = Name.find_in_text(searched_text)
        if not names:
            return Counter()

        # Находим веса для адресов:
        # Чем больше названий (Name) задействовано в адресе, тем лучше
        # Считаем сколько под-адресов входит в каждый адрес, возвращаем
        # адрес с наибольшим числом под-адресов

        # Создадим хранилище адресов в формате:
        # {название: множество адресов, в которых встречается название}
        tmp_groups = {
            n: set([   # Remove possible duplicates
                        addr.get_address_hierarhy(raw_address=searched_text) for addr in n.addrobjs
                    ]
            ) for n in names
        }
        # Remove duplicates of addresses:
        name_groups = {names[0]: tmp_groups[names[0]]}
        for n in names[1:]:
            addresses = tmp_groups[n]
            for stored_names in name_groups:
                addresses -= name_groups[stored_names]
            if len(addresses) > 0:
                name_groups[n] = addresses


        addr_counter = Counter()
        for n in name_groups:
            for addr in name_groups[n]:
                addr_counter[addr] += 1

        for name1 in name_groups:
            for name2 in name_groups:
                if name1 == name2:
                    continue
                for addr1 in name_groups[name1]:
                    for addr2 in name_groups[name2]:
                        if addr2.subaddress_of(addr1):
                            addr_counter[addr1] += 1

        return addr_counter

    def tokenize(self, address_text, count=False):
        # Convert address_text to tokens and their positions
        sql = """ SELECT to_tsvector('ru', %s)"""
        tsvector = self.bind.execute(sql, address_text)
        tsvector = tsvector.fetchone()[0]
        tokens = {}
        for description in tsvector.split():
            token, positions = description.split(':')
            positions = [int(p) for p in positions.split(',')]
            token = token.decode('utf-8')[1:-1]   # Drop quotes
            tokens[token] = positions

        if count:
            for key, val in tokens.iteritems():
                tokens[key] = len(val)

        return tokens


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
    formalname = Column(Text)
    ifnsfl = Column(Text)
    ifnsul = Column(Text)
    nextid = Column(Text)
    offname = Column(Text)
    operstatus = Column(Text)
    parentguid = Column(Text)
    placecode = Column(Text)
    plaincode = Column(Text)
    postalcode = Column(Text)
    previd = Column(Text)
    regioncode = Column(Text)
    shortname = Column(Text)
    streetcode = Column(Text)
    terrifnsfl = Column(Text)
    terrifnsul = Column(Text)
    ctarcode = Column(Text)
    extrcode = Column(Text)
    sextcode = Column(Text)
    livestatus = Column(Text)

    names = relationship('Name', secondary=placenames_table, backref='names')

    def get_admin_order(self):
        # dbsession = DBSession()
        qs = u'''
            WITH RECURSIVE child_to_parents AS (
              SELECT addrobj.*
                  FROM addrobj
                  WHERE addrobj.aoid = '%s'
              UNION ALL
              SELECT addrobj.*
                  FROM addrobj, child_to_parents
                  WHERE addrobj.aoguid = child_to_parents.parentguid
                    AND addrobj.currstatus = 0
            )
            SELECT
                child_to_parents.*
              FROM child_to_parents
              ORDER BY aolevel
        ''' % (self.aoid,)

        adm_order = dbsession.query(Addrobj).from_statement(text(qs)).all()
        # dbsession.close()

        return adm_order

    def get_address_hierarhy(self, raw_address=None):
        adm_order = self.get_admin_order()

        address = Address(addrobj=self, raw_address=raw_address)
        for adm in adm_order:
            name = "%s %s" % (adm.shortname, adm.formalname)
            if adm.aolevel == 1:
                address.region = name
            elif adm.aolevel == 3:
                address.subregion = name
            elif adm.aolevel == 4:
                address.city = name
            elif adm.aolevel == 5:
                address.subcity = name
            elif adm.aolevel in [6, 90]:
                address.settlement = name
            elif adm.aolevel in [7, 91]:
                address.street = name
            else:
                raise ValueError("Unknown address level: %s" % (adm.aolevel, ))

        return address


class Name(Base):
    __tablename__ = 'name'
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    name_tsvect = Column(TSVECTOR)
    name_tsquery = Column(TSQUERY, unique=True)

    addrobjs = relationship('Addrobj', secondary=placenames_table, backref='addrobjs')

    @staticmethod
    def find_in_text(searched_text):
        sql = """
        SELECT * FROM name
        WHERE
            name_tsquery @@ to_tsvector('ru', '%s')
        """ % (searched_text, )
        names = dbsession.query(Name).from_statement(text(sql)).all()

        return names



