#!/bin/env python
# -*- coding: utf-8 -*-

import copy


class Address(object):
    """Class for store address information
    """
    @staticmethod
    def address_parts_list():
        return ['country', 'region', 'subregion', 'index',
                'settlement', 'city', 'subcity', 'street', 'house', 'poi']

    def __init__(self,
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
        """Initialization of address

        :param raw_address: address string (unparsed)
        :type raw_address:  unicode

        :param index:     index
        :type index:      unicode

        :param country:     country
        :type country:      unicode

        :param region:     region (FIAS level: 1)
        :type region:      unicode

        :param subregion:     subregion (FIAS level: 3)
        :type subregion:      unicode

        :param city:      Name of the city (FIAS level: 4)
        :type city:       unicode

        :param settlement:      Name of the settlement (FIAS level: 6)
        :type settlement:       unicode
        
        :param subcity:      Name of the subsettlement (FIAS level: 5)
        :type settlement:       unicode

        :param street:          Street name (FIAS level: 7)
        :type street:           unicode

        :param house:           House number
        :type house:            unicode

        :param poi:         Point Of Interest
        :type poi:          unucode
        """

        self._raw_address = raw_address

        self._index = index
        self._country = country
        self._region = region
        self._subregion = subregion
        self._city = city
        self._subcity = subcity
        self._settlement = settlement
        self._street = street
        self._house = house
        self._poi = poi

    def __unicode__(self):
        parts = [
            "%s: %s" % (part, p)  for (part, p) in
            [('Country', self.country),
             ('Region', self.region),
             ('Subregion', self.subregion),
             ('City', self.city),
             ('Subcity', self.subcity),
             ('Settlement', self.settlement),
             ('Street', self.street),
             ('House', self.house),
             ('POI', self.poi)]
            if p]
        return '; '.join(parts)

    def __eq__(self, other):
        if self.raw_address != other.raw_address:
            return False
        if self.index != other.index:
            return False
        if self.country != other.country:
            return False
        if self.region != other.region:
            return False
        if self.subregion != other.subregion:
            return False
        if self.settlement != other.settlement:
            return False
        if self.city != other.city:
            return False
        if self.subcity != other.subcity:
            return False
        if self.street != other.street:
            return False
        if self.house != other.house:
            return False
        if self.poi != other.poi:
            return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def subaddress_of(self, other):
        """Return true if the addres is a subaddress of the other address
        """
        if self.raw_address != other.raw_address:
            # The addresses are results of analysis for different address strings
            return False

        for prop_name in self.address_parts_list():
            self_prop = self.__getattribute__(prop_name)
            other_prop = other.__getattribute__(prop_name)
            if (other_prop is None) and (self_prop is not None):
                return False
            elif self_prop is None and other_prop is not None:
                pass    # It's Ok
            elif self_prop != other_prop:
                return False

        return True

    @property
    def raw_address(self):
        return self._raw_address

    @raw_address.setter
    def raw_address(self, value):
        self._raw_address = value

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @property
    def country(self):
        return self._country

    @country.setter
    def country(self, value):
        self._country = value

    @property
    def region(self):
        return self._region

    @region.setter
    def region(self, value):
        self._region = value

    @property
    def subregion(self):
        return self._subregion

    @subregion.setter
    def subregion(self, value):
        self._subregion = value

    @property
    def settlement(self):
        return self._settlement

    @settlement.setter
    def settlement(self, value):
        self._settlement = value

    @property
    def city(self):
        return self._city

    @city.setter
    def city(self, value):
        self._city = value

    @property
    def subcity(self):
        return self._subcity

    @subcity.setter
    def subcity(self, value):
        self._subcity = value

    @property
    def street(self):
        return self._street

    @street.setter
    def street(self, value):
        self._street = value

    @property
    def house(self):
        return self._house

    @house.setter
    def house(self, value):
        self._house = value

    @property
    def poi(self):
        return self._poi

    @poi.setter
    def poi(self, value):
        self._poi = value

    def mask_address_parts(self, used_parts):
        """Delete from address unused address parts. Return the modified copy.

        :param used_parts:   A list of parts, that are used, the other parts
                             will be masked
        :type used_parts:    list

        :rtype:             Address
        """
        address = copy.copy(self)
        for part in self.address_parts_list():
            if part not in used_parts:
                setattr(address, part, None)

        return address
