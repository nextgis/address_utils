# coding=utf-8

from collections import Counter

from sqlalchemy import Table, Column, ForeignKey, Integer, Text
from sqlalchemy import text

from sqlalchemy import types
from sqlalchemy import func

from sqlalchemy.orm import relationship

from sqlalchemy.dialects.postgresql import TSVECTOR

from address_utils.address import Address as BaseAddress


from address_utils.postgres import Base


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
                 full_addr_str=None,
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

        # String of names for all addresses
        self._full_addr_str = full_addr_str

        self.tokenizer = Tokenizer()
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

    @property
    def full_addr_str(self):
        return self._full_addr_str

    @full_addr_str.setter
    def full_addr_str(self, value):
        self._full_addr_str = value

    def bag_of_words(self):
        counts = self.tokenizer.tokenize(self.full_addr_str, count=True)
        return Counter(counts)

class Tokenizer(object):
    def __init__(self):
        self.bind = Base.metadata.bind

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


class AddressParser(object):
    
    EPSILON = 0.000001   # A small number
    
    @staticmethod
    def extract_addresses(session, searched_text):
        names = Name.find_in_text(session, searched_text)
        if not names:
            return Counter()

        addresses = set()
        for n in names:
            addresses |= set([
                        addr.get_address_hierarhy(session, raw_address=searched_text) for addr in n.addrobjs
            ])

        return addresses

    @staticmethod
    def _dist(address1, address2):
        """Return distance between addresses
        """
        bag1 = address1.bag_of_words()
        bag2 = address2.bag_of_words()

        # bagX is a Counter; use Counter's arithmetic
        diff = (bag2 - bag1) + (bag1 - bag2)

        res = [v for (k, v) in diff.iteritems()]
        return sum(res)

    def parse_address(self, session, searched_text, count=10):
        
        if count <= 0:
            raise ValueError('Count of adresses must be positive integer')
            
        pattern = Address(full_addr_str=searched_text)
        addresses = self.extract_addresses(session, searched_text)
        
        # Similarity of adresses and the text (sort by similarity): 
        sims = [(a, 1.0/(self._dist(pattern, a) + self.EPSILON) ) for a in addresses]
        sims = sorted(sims, key=lambda s: s[1])
        
        return sims[:count]


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

    def get_admin_order(self, dbsession):
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

        return adm_order

    def get_address_hierarhy(self, session, raw_address=None):
        adm_order = self.get_admin_order(session)

        address = Address(addrobj=self,
                          raw_address=raw_address,
                          full_addr_str='')
        full_name = []
        for adm in adm_order:
            # construct string of all names:
            full_name.append(adm.shortname)
            for name in adm.names:
                full_name.append(name.name)

            # Fill address
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

        address.full_addr_str = ' '.join(full_name)
        return address


class Name(Base):
    __tablename__ = 'name'
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    name_tsvect = Column(TSVECTOR)
    name_tsquery = Column(TSQUERY, unique=True)

    addrobjs = relationship('Addrobj', secondary=placenames_table, backref='addrobjs')

    @staticmethod
    def find_in_text(dbsession, searched_text):
        sql = """
        SELECT * FROM name
        WHERE
            name_tsquery @@ to_tsvector('ru', '%s')
        """ % (searched_text, )
        names = dbsession.query(Name).from_statement(text(sql)).all()

        return names


