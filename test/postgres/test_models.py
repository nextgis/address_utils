
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

    @classmethod
    def setUpClass(cls):
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

        name1 = Name(name='Name11', name_tsquery='Name11')
        name2 = Name(name='Name12', name_tsquery='Name12')
        node = Addrobj(aoid='1', aoguid='1', aolevel=1, currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################
        name1 = Name(name='Name21', name_tsquery='Name21')
        name2 = Name(name='Name22', name_tsquery='Name22')
        node = Addrobj(aoid='2', aolevel=2, aoguid='2', parentguid='1', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name31', name_tsquery='Name31')
        name2 = Name(name='Name32', name_tsquery='Name32')
        node = Addrobj(aoid='3', aolevel=2, aoguid='3', parentguid='1', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################

        name1 = Name(name='Name41', name_tsquery='Name41')
        name2 = Name(name='Name42', name_tsquery='Name42')
        node = Addrobj(aoid='4', aolevel=3, aoguid='4', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name51', name_tsquery='Name51')
        name2 = Name(name='Name52', name_tsquery='Name52')
        node = Addrobj(aoid='5', aolevel=3, aoguid='5', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name61', name_tsquery='Name61')
        name2 = Name(name='Name62', name_tsquery='Name62')
        node = Addrobj(aoid='6', aolevel=3, aoguid='6', parentguid='2', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name71', name_tsquery='Name71')
        name2 = Name(name='Name72', name_tsquery='Name72')
        node = Addrobj(aoid='7', aolevel=3, aoguid='7', parentguid='3', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name81', name_tsquery='Name81')
        name2 = Name(name='Name82', name_tsquery='Name82')
        node = Addrobj(aoid='8', aolevel=3, aoguid='8', parentguid='3', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)


        ######################################

        name1 = Name(name='Name91', name_tsquery='Name91')
        name2 = Name(name='Name92', name_tsquery='Name92')
        node = Addrobj(aoid='9', aolevel=4, aoguid='9', parentguid='8', currstatus=0,
                       names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)
        ######################################

        session.commit()

    @classmethod
    def tearDownClass(cls):
        # import ipdb; ipdb.set_trace()
        # Base.metadata.drop_all(engine)
        pass

    def test_addrobj_get_parents(self):
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

    def test_name_find_in_text(self):
        # TODO: check quotes
        text = "Is the node with label ''name92'' a subnode of the node with ''name31'' ?"
        names = Name.find_in_text(text)

        names = [n.name for n in names]
        self.assertEquals(len(names), 2)
        self.assertTrue('Name31' in names)
        self.assertTrue('Name92' in names)


if __name__ == '__main__':
    suite = unittest.makeSuite(TestModels, 'test')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


