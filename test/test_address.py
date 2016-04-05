#!/bin/env python
# -*- coding: utf-8 -*-

import sys

import unittest

from address import Address


class TestAddress(unittest.TestCase):

    def test_init(self):
        index = u'123456'
        country = u'Российская Федерация'
        region = u'Московская область'
        subregion = u'Подольский район'
        city = u'Подольск'
        subcity = u'район Зеленый'
        street = u'Малая'
        house = u'234'
        poi = u'Станция Канавка'

        address = Address(
            index=index,
            country=country,
            region=region,
            subregion=subregion,
            city=city,
            subcity=subcity,
            street=street,
            house=house,
            poi=poi
        )

        expected = ','.join([
            index, country, region, subregion,
            city, subcity, street, house, poi
        ])
        got = ','.join([
            address.index, address.country,
            address.region, address.subregion,
            address.city, address.subcity, address.street,
            address.house, address.poi
        ])
        self.assertEqual(got, expected)

        address = Address()
        address.raw_address = expected + 'qwerty'
        self.assertEqual(address.raw_address, expected + 'qwerty')

        address = Address()
        address.index = index
        address.country = country
        address.region = region
        address.subregion = subregion
        address.city = city
        address.subcity = subcity
        address.street = street
        address.house = house
        address.poi = poi
        got = ','.join([
            address.index, address.country,
            address.region, address.subregion,
            address.city, address.subcity,
            address.street,
            address.house, address.poi
        ])

        self.assertEqual(got, expected)

    def test___unicode__(self):
        index = u'123456'
        country = u'Российская Федерация'
        region = u'Московская область'
        subregion = u'Подольский район'
        city = u'Подольск'
        subcity = u'район Зеленый'
        street = u'Малая'
        house = u'234'
        poi = u'Ресторан Рыбный'

        address = Address(
            index=index,
            country=country,
            region=region,
            subregion=subregion,
            city=city,
            subcity=subcity,
            street=street,
            house=house,
            poi=poi
        )

        # Index is deleted from the representation
        expected = "Country: %s; Region: %s; Subregion: %s; City: %s; Subcity: %s; Street: %s; House: %s; POI: %s" % (country, region, subregion, city, subcity, street, house, poi)
        self.assertEqual(unicode(address), expected)

    def test_equal(self):
        addr1 = Address()
        addr2 = Address()
        self.assertEqual(addr1, addr2)

        for key, val in addr1.__dict__.iteritems():
            addr1.__dict__[key] = 'qwerty' + unicode(val)
            self.assertNotEqual(addr1, addr2)
            addr2.__dict__[key] = 'qwerty' + unicode(val)
            self.assertEqual(addr1, addr2)

    def test_subaddress_of(self):
        addr1 = Address()
        addr2 = Address()
        self.assertTrue(addr1.subaddress_of(addr2))

        # import ipdb; ipdb.set_trace()
        for key, val in addr1.__dict__.iteritems():
            if key == '_raw_address':
                continue
            addr1.__dict__[key] = 'qwerty' + unicode(val)
            self.assertTrue(addr2.subaddress_of(addr1))
            self.assertFalse(addr1.subaddress_of(addr2))
            addr2.__dict__[key] = 'qwerty' + unicode(val)

    def test_mask_address_parts(self):
        index = u'123456'
        country = u'Российская Федерация'
        region = u'Московская область'
        subregion = u'Подольский район'
        city = u'Подольск'
        subcity = u'район Зеленый'
        street = u'Малая'
        house = u'234'
        poi = u'Станция Канавка'
        raw_address = u'sdlkjsldjflsdfjksdjffedsjdnv'

        address = Address(
            raw_address=raw_address,
            index=index,
            country=country,
            region=region,
            subregion=subregion,
            city=city,
            subcity=subcity,
            street=street,
            house=house,
            poi=poi
        )

        used_parts = ['index', 'country']
        new = address.mask_address_parts(used_parts)
        expected = Address(raw_address=raw_address,
                           index=index, country=country)
        self.assertEqual(new, expected)

        used_parts = ['region', 'city']
        new = address.mask_address_parts(used_parts)
        expected = Address(raw_address=raw_address,
                           region=region, city=city)
        # import ipdb; ipdb.set_trace()
        self.assertEqual(new, expected)


if __name__ == '__main__':
    suite = unittest.makeSuite(TestAddress, 'test')
    runner = unittest.TextTestRunner()
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
