# coding=utf-8

import sys

import unittest

from collections import Counter

from sqlalchemy import create_engine

from postgres.models import (
    AddressParser,
    Address,
    Addrobj,
    Name,
    Base,
    DBSession
)


connection_string = 'postgresql://geocoder:@localhost:5432/test_addr'
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
        # 1     (region)
        # | -- 2    (subregion)
        #      | -- 4  (settlement)
        #      | -- 5  (settlement)
        #      | -- 6  (settlement)
        #
        # | -- 3    (city)
        #      | -- 7   (subcity)
        #      | -- 8   (subcity)
        #           | -- 9  (street)

        name1 = Name(name='Name11', name_tsquery='Name11')
        name2 = Name(name='Name12', name_tsquery='Name12')
        node = Addrobj(aoid='1', aoguid='1', aolevel=1, currstatus=0,
                       shortname='Oblast', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################
        name1 = Name(name='Name21', name_tsquery='Name21')
        name2 = Name(name='Name22', name_tsquery='Name22')
        node = Addrobj(aoid='2', aolevel=3, aoguid='2', parentguid='1', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Название31', name_tsquery='Название31')
        name2 = Name(name='Name32', name_tsquery='Name32')
        node = Addrobj(aoid='3', aolevel=4, aoguid='3', parentguid='1', currstatus=0,
                       shortname='Gorod', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        ######################################

        name1 = Name(name='Name41', name_tsquery='Name41')
        name2 = Name(name='Name42', name_tsquery='Name42')
        node = Addrobj(aoid='4', aolevel=6, aoguid='4', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name51', name_tsquery='Name51')
        name2 = Name(name='Name52', name_tsquery='Name52')
        node = Addrobj(aoid='5', aolevel=6, aoguid='5', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name61', name_tsquery='Name61')
        name2 = Name(name='Name62', name_tsquery='Name62')
        node = Addrobj(aoid='6', aolevel=6, aoguid='6', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name71', name_tsquery='Name71')
        name2 = Name(name='Name72', name_tsquery='Name72')
        node = Addrobj(aoid='7', aolevel=5, aoguid='7', parentguid='3', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)

        name1 = Name(name='Name81', name_tsquery='Name81')
        name2 = Name(name='Name82', name_tsquery='Name82')
        node = Addrobj(aoid='8', aolevel=5, aoguid='8', parentguid='3', currstatus=0,
                       shortname='Kvartal', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)


        ######################################

        name1 = Name(name='Название91', name_tsquery='Название91')
        name2 = Name(name='Название92', name_tsquery='Название92')
        node = Addrobj(aoid='9', aolevel=7, aoguid='9', parentguid='8', currstatus=0,
                       shortname='Street', formalname=name1.name, names=[name1, name2])

        session.add(name1)
        session.add(name2)
        session.add(node)
        ######################################

        session.commit()
        session.close()

    @classmethod
    def tearDownClass(cls):
        # import ipdb; ipdb.set_trace()
        # Base.metadata.drop_all(engine)
        pass

    def test_addrobj_get_parents(self):
        session = DBSession()

        node = session.query(Addrobj).filter(Addrobj.aoid == '9').one()
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

        session.close()

    def test_addrobj_get_address_hierarhy(self):
        session = DBSession()

        node = session.query(Addrobj).filter(Addrobj.aoid == '1').one()
        adm = node.get_address_hierarhy()
        expected = Address(region=u'Oblast Name11')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '1')

        node = session.query(Addrobj).filter(Addrobj.aoid == '2').one()
        adm = node.get_address_hierarhy()
        expected = Address(region=u'Oblast Name11', subregion=u'Raion Name21')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '2')

        node = session.query(Addrobj).filter(Addrobj.aoid == '6').one()
        adm = node.get_address_hierarhy()
        expected = Address(region=u'Oblast Name11', subregion=u'Raion Name21',
                           settlement=u'Der. Name61')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '6')

        node = session.query(Addrobj).filter(Addrobj.aoid == '9').one()
        adm = node.get_address_hierarhy()
        expected = Address(region=u'Oblast Name11', city=u'Gorod Название31',
                           subcity=u'Kvartal Name81', street=u'Street Название91')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '9')

        parts = ['region', 'city', 'subcity', 'street']
        for p in parts:
            used_parts = list(set(parts) - set([p]))
            cropped_addr = expected.mask_address_parts(used_parts)
            self.assertNotEqual(adm, cropped_addr)

        session.close()

    def test_name_find_in_text(self):
        # TODO: check quotes
        text = u"Is the node with label ''Название92'' a subnode of the node with ''Название31'' ?"
        names = Name.find_in_text(text)

        names = [n.name for n in names]
        self.assertEquals(len(names), 2)
        self.assertTrue(u'Название31' in names)
        self.assertTrue(u'Название92' in names)

    def test_addresparser_extract_addresses(self):
        parser = AddressParser()
        text = u"Is the node with label ''Название92'' a subnode of the node with ''Название31'' ?"
        addresses = parser.extract_addresses(text)
        self.assertEqual(len(addresses), 2)
        expected = Counter({
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ): 2,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31'
            ): 1

        })
        addr, count = addresses.most_common(1)[0]
        self.assertEqual(expected, addresses)

        text = u"""Список названий: Название91 Name81 Name71 Name11, встречающихся в тексте"""
        addresses = parser.extract_addresses(text)
        # N1: (N1): 1
        # N7: (N7, N3, N1): 2
        # N8: (N8, N3, N1):2
        # N9: (N9, N8, N3, N1): 3
        self.assertEqual(len(addresses), 4)
        expected = Counter({
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ): 3,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Name71',
            ): 2,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81'
            ): 2,

            Address(
                raw_address=text,
                region=u'Oblast Name11'
            ): 1
        })
        self.assertEqual(expected, addresses)


        # Добавим несколько адресов в иерархию с одинаковыми называниями
        # дубликаты названий не должны влиять на результат
        session = DBSession()

        text = u"""Список названий: Название91 Название31, встречающихся в тексте"""

        name1 = session.query(Name).filter(Name.name == 'Название31').one()
        name2 = session.query(Name).filter(Name.name == 'Name32').one()

        node1 = Addrobj(aoid='33', aolevel=5, aoguid='33', parentguid='3', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        node2 = Addrobj(aoid='333', aolevel=7, aoguid='333', parentguid='33', currstatus=0,
                       shortname='Street', formalname=name1.name, names=[name1, name2])

        session.add(node2)
        session.add(node1)
        session.commit()
        session.close()

        addresses = parser.extract_addresses(text)
        # N3: (N3, N1): 1
        # N3: (N3, N3, N1): 1
        # N3: (N3, N3, N3, N1): 1
        # N9: (N9, N8, N3, N1): 2
        self.assertEqual(len(addresses), 4)

        addresses = parser.extract_addresses(text)
        expected = Counter({
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
            ): 1,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31'
            ): 1,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31',
                street=u'Street Название31'
            ): 1,
            Address(raw_address=text,
                   region=u'Oblast Name11',
                   city=u'Gorod Название31',
                   subcity=u'Kvartal Name81',
                   street=u'Street Название91'
            ): 2
        })
        self.assertEqual(addresses, expected)

        # Пусть теперь в тексте встречаются дубликаты

        text = u"Список названий: Название91 Название31  Название31 Название31"
        # import ipdb; ipdb.set_trace()
        addresses = parser.extract_addresses(text)
        # N3: (N3, N1): 1
        # N3: (N3, N3, N1): 2
        # N3: (N3, N3, N3, N1): 3
        # N9: (N9, N8, N3, N1): 2
        self.assertEqual(len(addresses), 4)

        expected = Counter({
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
            ): 1,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31'
            ): 2,
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31',
                street=u'Street Название31'
            ): 3,
            Address(raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ): 2,
        })
        # self.assertEqual(addresses, expected)


    def test_addressparser_tokenize(self):
        parser = AddressParser()
        test_text = '''Просто текст. Проверяем сответствие текста
        тем, что загоняем его в Postgres:
            select to_tsvector('ru', 'Текст')'''
        expected = {
            u'postgr': [11],
            u'ru': [15],
            u'select': [12],
            u'tsvector': [14],
            u'загоня': [8],
            u'проверя': [3],
            u'прост': [1],
            u'сответств': [4],
            u'текст': [2, 5, 16],
        }
        tokens = parser.tokenize(test_text)
        self.assertEqual(tokens, expected)

        expected = {
            u'postgr': 1,
            u'ru': 1,
            u'select': 1,
            u'tsvector': 1,
            u'загоня': 1,
            u'проверя': 1,
            u'прост': 1,
            u'сответств': 1,
            u'текст': 3
        }
        tokens = parser.tokenize(test_text, count=True)
        self.assertEqual(tokens, expected)




if __name__ == '__main__':
    suite = unittest.makeSuite(TestModels, 'test')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
