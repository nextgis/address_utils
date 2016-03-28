
import sys

import unittest

from sqlalchemy import create_engine

from postgres.models import (
    Addrobj,
    Name,
    Base,
    DBSession
)

connection_string = 'postgresql://addr_user:@localhost:5432/test_addr'
engine = create_engine(connection_string)

Base.metadata.bind = engine
DBSession.configure(bind=engine)

class TestModels(unittest.TestCase):

    def setUp(self):
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        # conn = engine.connect()

        session = DBSession()
        # try:
        #     sql = """CREATE TEXT SEARCH DICTIONARY russian_ispell (
        #         TEMPLATE = ispell,
        #         DictFile = russian,
        #         AffFile = russian,
        #         StopWords = russian
        #     );
        #     CREATE TEXT SEARCH CONFIGURATION ru (COPY=russian);
        #     ALTER TEXT SEARCH CONFIGURATION ru ALTER MAPPING FOR hword, hword_part, word WITH russian_stem;
        #     """
        #     conn.execute(sql)
        # except:
        #     raise

        # Insert into DB the next node hierarchy:
        # 1
        # | -- 2
        #      | -- 4
        #      | -- 5
        #      | -- 6
        #
        # | -- 3
        #      | -- 7
        #      | -- 8
        #           | -- 9

        name1 = Name(name='Name-1-1', name_tsquery='Name-1-1')
        name2 = Name(name='Name-1-2', name_tsquery='Name-1-2')
        node = Addrobj(aoid='1', aoguid='1', aolevel=1, currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################
        name1 = Name(name='Name-2-1', name_tsquery='Name-2-1')
        name2 = Name(name='Name-2-2', name_tsquery='Name-2-2')
        node = Addrobj(aoid='2', aolevel=2, aoguid='2', parentguid='1', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name-3-1', name_tsquery='Name-3-1')
        name2 = Name(name='Name-3-2', name_tsquery='Name-3-2')
        node = Addrobj(aoid='3', aolevel=2, aoguid='3', parentguid='1', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################

        name1 = Name(name='Name-4-1', name_tsquery='Name-4-1')
        name2 = Name(name='Name-4-2', name_tsquery='Name-4-2')
        node = Addrobj(aoid='4', aolevel=3, aoguid='4', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name-5-1', name_tsquery='Name-5-1')
        name2 = Name(name='Name-5-2', name_tsquery='Name-5-2')
        node = Addrobj(aoid='5', aolevel=3, aoguid='5', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name-6-1', name_tsquery='Name-6-1')
        name2 = Name(name='Name-6-2', name_tsquery='Name-6-2')
        node = Addrobj(aoid='6', aolevel=3, aoguid='6', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name-7-1', name_tsquery='Name-7-1')
        name2 = Name(name='Name-7-2', name_tsquery='Name-7-2')
        node = Addrobj(aoid='7', aolevel=3, aoguid='7', parentguid='3', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name-8-1', name_tsquery='Name-8-1')
        name2 = Name(name='Name-8-2', name_tsquery='Name-8-2')
        node = Addrobj(aoid='8', aolevel=3, aoguid='8', parentguid='3', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)


        ######################################

        name1 = Name(name='Name-9-1', name_tsquery='Name-9-1')
        name2 = Name(name='Name-9-2', name_tsquery='Name-9-2')
        node = Addrobj(aoid='9', aolevel=4, aoguid='9', parentguid='8', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)
        ######################################

        session.commit()

    def test_get_parents(self):
        session = DBSession()

        node = session.query(Addrobj).filter(Addrobj.aoid=='9').one()
        parents = node.get_admin_order()

        y = parents.pop()
        self.assertEquals(y.aoid, '9')
        y = parents.pop()
        self.assertEquals(y.aoid, '8')
        y = parents.pop()
        self.assertEquals(y.aoid, '3')
        y = parents.pop()
        self.assertEquals(y.aoid, '1')
        self.assertEquals(parents, [])


if __name__ == '__main__':
    suite = unittest.makeSuite(TestModels, 'test')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


