from sqlalchemy import Table, Column, ForeignKey, Integer, Text
from sqlalchemy import text
from sqlalchemy.orm import relationship, sessionmaker

from sqlalchemy import types
from sqlalchemy import func

from sqlalchemy.dialects.postgresql import TSVECTOR

from sqlalchemy.ext.declarative import declarative_base


from address import Address

DBSession = sessionmaker()
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
        dbsession = DBSession()
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

    def get_address_hierarhy(self):
        adm_order = self.get_admin_order()

        address = Address()
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
                raise ValueError("Unknown address level: %s" %(adm.aolevel, ))

        return address


class Name(Base):
    __tablename__ = 'name'
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True)
    name_tsvect = Column(TSVECTOR)
    name_tsquery = Column(TSQUERY)

    addrobjs = relationship('Addrobj', secondary=placenames_table, backref='addrobjs')

    @staticmethod
    def find_in_text(searched_text):
        dbsession = DBSession()
        sql = """
        SELECT * FROM name
        WHERE
            name_tsquery @@ to_tsvector('ru', '%s')
        """ % (searched_text, )
        names = dbsession.query(Name).from_statement(text(sql)).all()

        return names

    @staticmethod
    def hierarchies_for_text(searched_text):
        """
        Find all address names that are matched in searched_text,
        then create list of Addresses that corresponds to the names.

        Return list of addresses (Address objects):
            * addresses are unique (the list doesn't contain duplicates);
            * addresses are 'gready': every address takes as much address names as possible.
        """
        names = Name.find_in_text(searched_text)

        addresses = []
        for name in names:
            for addr in name.addrobjs:
                addresses.append(addr.get_address_hierarhy())

        # Remove subaddresses:
        result = []
        size = len(addresses)
        for i in range(size):
            subaddress = False
            for j in range(i + 1, size):
                if addresses[i].subaddress_of(addresses[j]):
                    subaddress = True
                    break
            if not subaddress:
                addresses[i].raw_address = searched_text
                result.append(addresses[i])

        return result



