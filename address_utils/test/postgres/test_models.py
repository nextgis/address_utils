# coding=utf-8

import sys

import unittest

from sqlalchemy import create_engine

from postgres import DBSession
from postgres.models import (
    AddressParser,
    Address,
    Tokenizer,
    Addrobj,
    Name,
    Base,
)

connection_string = 'postgresql://geocoder:@localhost:5432/test_addr'
engine = create_engine(connection_string)

Base.metadata.bind = engine
DBSession.configure(bind=engine)

def _sort_addr_str(addr_str):
    return ' '.join(sorted(addr_str.split()))

class TestModels(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Base.metadata.create_all(engine)

        # conn = engine.connect()

        cls.session = DBSession()
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

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        ######################################
        name1 = Name(name='Name21', name_tsquery='Name21')
        name2 = Name(name='Name22', name_tsquery='Name22')
        node = Addrobj(aoid='2', aolevel=3, aoguid='2', parentguid='1', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        name1 = Name(name='Название31', name_tsquery='Название31')
        name2 = Name(name='Name32', name_tsquery='Name32')
        node = Addrobj(aoid='3', aolevel=4, aoguid='3', parentguid='1', currstatus=0,
                       shortname='Gorod', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        ######################################

        name1 = Name(name='Name41', name_tsquery='Name41')
        name2 = Name(name='Name42', name_tsquery='Name42')
        node = Addrobj(aoid='4', aolevel=6, aoguid='4', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        name1 = Name(name='Name51', name_tsquery='Name51')
        name2 = Name(name='Name52', name_tsquery='Name52')
        node = Addrobj(aoid='5', aolevel=6, aoguid='5', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        name1 = Name(name='Name61', name_tsquery='Name61')
        name2 = Name(name='Name62', name_tsquery='Name62')
        node = Addrobj(aoid='6', aolevel=6, aoguid='6', parentguid='2', currstatus=0,
                       shortname='Der.', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        name1 = Name(name='Name71', name_tsquery='Name71')
        name2 = Name(name='Name72', name_tsquery='Name72')
        node = Addrobj(aoid='7', aolevel=5, aoguid='7', parentguid='3', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)

        name1 = Name(name='Name81', name_tsquery='Name81')
        name2 = Name(name='Name82', name_tsquery='Name82')
        node = Addrobj(aoid='8', aolevel=5, aoguid='8', parentguid='3', currstatus=0,
                       shortname='Kvartal', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)


        ######################################

        name1 = Name(name='Название91', name_tsquery='Название91')
        name2 = Name(name='Название92', name_tsquery='Название92')
        node = Addrobj(aoid='9', aolevel=7, aoguid='9', parentguid='8', currstatus=0,
                       shortname='Street', formalname=name1.name, names=[name1, name2])

        cls.session.add(name1)
        cls.session.add(name2)
        cls.session.add(node)
        ######################################

        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        # import ipdb; ipdb.set_trace()
        cls.session.close()
        Base.metadata.drop_all(engine)

    def test_addrobj_get_parents(self):
        node = self.session.query(Addrobj).filter(Addrobj.aoid == '9').one()
        parents = node.get_admin_order(self.session)

        y = parents.pop()
        self.assertEquals(y.aoid, '9')
        y = parents.pop()
        self.assertEquals(y.aoid, '8')
        y = parents.pop()
        self.assertEquals(y.aoid, '3')
        y = parents.pop()
        self.assertEquals(y.aoid, '1')
        self.assertEquals(parents, [])


    def test_addrobj_get_address_hierarchy(self):

        node = self.session.query(Addrobj).filter(Addrobj.aoid == '1').one()
        adm = node.get_address_hierarchy(self.session)
        expected = Address(region=u'Oblast Name11')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '1')
        expected_addr_str = _sort_addr_str("Oblast Name11 Name12")
        self.assertEquals(_sort_addr_str(adm.full_addr_str),
                          expected_addr_str)

        node = self.session.query(Addrobj).filter(Addrobj.aoid == '2').one()
        adm = node.get_address_hierarchy(self.session)
        expected = Address(region=u'Oblast Name11', subregion=u'Raion Name21')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '2')
        expected_addr_str = _sort_addr_str("Oblast Name11 Name12 Raion Name21 Name22")
        self.assertEquals(_sort_addr_str(adm.full_addr_str),
                          expected_addr_str)


        node = self.session.query(Addrobj).filter(Addrobj.aoid == '6').one()
        adm = node.get_address_hierarchy(self.session)
        expected = Address(region=u'Oblast Name11', subregion=u'Raion Name21',
                           settlement=u'Der. Name61')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '6')
        expected_addr_str = _sort_addr_str("Oblast Name11 Name12 Raion Name21 Name22 Der. Name61 Name62")
        self.assertEquals(_sort_addr_str(adm.full_addr_str),
                          expected_addr_str)


        node = self.session.query(Addrobj).filter(Addrobj.aoid == '9').one()
        adm = node.get_address_hierarchy(self.session)
        expected = Address(region=u'Oblast Name11', city=u'Gorod Название31',
                           subcity=u'Kvartal Name81', street=u'Street Название91')
        self.assertEquals(adm, expected)
        self.assertEquals(adm.addrobj.aoid, '9')

        parts = ['region', 'city', 'subcity', 'street']
        for p in parts:
            used_parts = list(set(parts) - set([p]))
            cropped_addr = expected.mask_address_parts(used_parts)
            self.assertNotEqual(adm, cropped_addr)


    def test_name_find_in_text(self):
        # TODO: check quotes
        text = u"Is the node with label ''Название92'' a subnode of the node with ''Название31'' ?"
        names = Name.find_in_text(self.session, text)

        names = [n.name for n in names]
        self.assertEquals(len(names), 2)
        self.assertTrue(u'Название31' in names)
        self.assertTrue(u'Название92' in names)

    def test_addresparser_extract_addresses(self):
        parser = AddressParser()
        text = u"Is the node with label ''Название92'' a subnode of the node with ''Название31'' ?"
        # import ipdb; ipdb.set_trace()
        addresses = parser.extract_addresses(self.session, text)
        expected = set([
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31'
            )
        ])
        self.assertEqual(expected, addresses)

        text = u"""Список названий: Название91 Name81 Name71 Name11, встречающихся в тексте"""
        addresses = parser.extract_addresses(self.session, text)
        # N1: (N1): 1
        # N7: (N7, N3, N1): 2
        # N8: (N8, N3, N1):2
        # N9: (N9, N8, N3, N1): 3
        expected = set([
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Name71',
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81'
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11'
            )
        ])
        self.assertEqual(expected, addresses)


        # Добавим несколько адресов в иерархию с одинаковыми называниями
        # дубликаты названий не должны влиять на результат

        text = u"""Список названий: Название91 Название31, встречающихся в тексте"""

        name1 = self.session.query(Name).filter(Name.name == 'Название31').one()
        name2 = self.session.query(Name).filter(Name.name == 'Name32').one()

        node1 = Addrobj(aoid='33', aolevel=5, aoguid='33', parentguid='3', currstatus=0,
                       shortname='Raion', formalname=name1.name, names=[name1, name2])

        node2 = Addrobj(aoid='333', aolevel=7, aoguid='333', parentguid='33', currstatus=0,
                       shortname='Street', formalname=name1.name, names=[name1, name2])

        self.session.add(node2)
        self.session.add(node1)
        self.session.commit()

        addresses = parser.extract_addresses(self.session, text)
        # N3: (N3, N1): 1
        # N3: (N3, N3, N1): 1
        # N3: (N3, N3, N3, N1): 1
        # N9: (N9, N8, N3, N1): 2
        addresses = parser.extract_addresses(self.session, text)
        expected = set([
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31'
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31',
                street=u'Street Название31'
            ),
            Address(raw_address=text,
                   region=u'Oblast Name11',
                   city=u'Gorod Название31',
                   subcity=u'Kvartal Name81',
                   street=u'Street Название91'
            )
        ])
        self.assertEqual(addresses, expected)

        # Пусть теперь в тексте встречаются дубликаты

        text = u"Список названий: Название91 Название31  Название31 Название31"
        # import ipdb; ipdb.set_trace()
        addresses = parser.extract_addresses(self.session, text)
        # N3: (N3, N1): 1
        # N3: (N3, N3, N1): 2
        # N3: (N3, N3, N3, N1): 3
        # N9: (N9, N8, N3, N1): 2
        expected = set([
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31'
            ),
            Address(
                raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Raion Название31',
                street=u'Street Название31'
            ),
            Address(raw_address=text,
                region=u'Oblast Name11',
                city=u'Gorod Название31',
                subcity=u'Kvartal Name81',
                street=u'Street Название91'
            ),
        ])
        self.assertEqual(addresses, expected)

    def parser_test_addresparser_parse_addresses(self):
        parser = AddressParser()
        
        text = u"Is the node with label ''Название92'' a subnode of the node with ''Название31'' ?"
        # The tokens are:
        #    node: 2
        #    название92: 1
        #    subnod: 1
        #    название31: 1
        #    label: 1
          
        addresses = parser.parse_address(self.session, text)
        # Tokens of the addresses: 
        #  (1)
        #    name11 1
        #    name12 1
        #    name32 1
        #    name82 1
        #    name81 1
        #    kvartal 1
        #    название91 1
        #    oblast 1
        #    название92 1
        #    gorod 1
        #    street 1
        #    название31 1
        #  (2)
        #    gorod 1
        #    name11 1
        #    name12 1
        #    name32 1
        #    oblast 1
        #    название31 1
        
        tokens = set(['node', u'название92', 'subnod', u'название31', 
                 'label', 'name11', 'name12', 'name32', 'name82', 'name81',
                 'kvartal', u'название91', 'oblast', 'gorod',
                 'street'])
        self.assertEquals(parser.tokens, tokens)
        size = len(tokens)
        
        expected = [
            (
                Address(
                    raw_address=text,
                    region=u'Oblast Name11',
                    city=u'Gorod Название31'
                ), 
                10.0/size),
            (
                Address(
                    raw_address=text,
                    region=u'Oblast Name11',
                    city=u'Gorod Название31',
                    subcity=u'Kvartal Name81',
                    street=u'Street Название91'
                ), 
                14.0/size),
        ]
        self.assertEqual(expected, addresses)
        
        
        addresses = parser.parse_address(self.session, text, count=1)
         
        expected = [
            (
                Address(
                    raw_address=text,
                    region=u'Oblast Name11',
                    city=u'Gorod Название31'
                ), 
                10.0/size),
        ]
        self.assertEqual(expected, addresses)
        
        with self.assertRaises(ValueError):
            addresses = parser.parse_address(self.session, text, count=0)
        

    def test_tokenizer_tokenize(self):
        tokenizer = Tokenizer()
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
        tokens = tokenizer.tokenize(test_text)
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
        tokens = tokenizer.tokenize(test_text, count=True)
        self.assertEqual(tokens, expected)

    def xtest_addresparser_dist(self):
        parser = AddressParser()
        text1 = "Название1 Название2 название"
        text2 = "Название1 Название3 название"
        addr1 = Address(full_addr_str=text1)
        addr2 = Address(full_addr_str=text2)
        diff = parser._dist(addr1, addr2)
        expected = 2
        self.assertEquals(diff, expected)

        text1 = "Название1 Название3 название"
        text2 = "Название1 Название3 название"
        addr1 = Address(full_addr_str=text1)
        addr2 = Address(full_addr_str=text2)
        diff = AddressParser._dist(addr1, addr2)
        expected = 0
        self.assertEquals(diff, expected)

        text1 = "название4"
        text2 = "Название1 Название3 название"
        addr1 = Address(full_addr_str=text1)
        addr2 = Address(full_addr_str=text2)
        diff = AddressParser._dist(addr1, addr2)
        expected = 4
        self.assertEquals(diff, expected)


        text1 = ""
        text2 = "Название1 Название3 название"
        addr1 = Address(full_addr_str=text1)
        addr2 = Address(full_addr_str=text2)
        diff = AddressParser._dist(addr1, addr2)
        expected = 3
        self.assertEquals(diff, expected)


if __name__ == '__main__':
    suite = unittest.makeSuite(TestModels, 'test')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)

    suite = unittest.makeSuite(TestModels, 'parser')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
