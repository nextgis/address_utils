#!/usr/bin/env python
# encoding: utf-8

import sys

from collections import namedtuple

import psycopg2


AddressRecord = namedtuple('AddressRecord', 'aoguid, parentguid, formalname, offname, shortname')



def get_parents(cursor, guid):
    """Return list of guids and levels for parent nodes
    """
    sql = """
    WITH RECURSIVE child_to_parents AS (
      SELECT addrobj.*
          FROM addrobj
          WHERE addrobj.aoguid = %s
      UNION ALL
      SELECT addrobj.*
          FROM addrobj, child_to_parents
          WHERE addrobj.aoguid = child_to_parents.parentguid
            AND addrobj.currstatus = 0
    )
    SELECT
        child_to_parents.aoguid,
        child_to_parents.aolevel
      FROM child_to_parents
      ORDER BY aolevel;
    """
    cursor.execute(sql, (guid, ))

    row = cursor.fetchone()
    result = []
    while row:
        result.append(row)
        row = cursor.fetchone()

    return result


def find_address_for_name(cursor, name, aoguid_only=False):
    """Return list of rows that contains name
    """

    if aoguid_only:
       sql = """
            SELECT aoguid
            FROM addrobj
            WHERE
                ts_name @@ to_tsquery('ru', %s)
                AND addrobj.currstatus = 0
        """
    else:
        sql = """
            SELECT aoguid, parentguid, formalname, offname, shortname
            FROM addrobj
            WHERE
                ts_name @@ to_tsquery('ru', %s)
                AND addrobj.currstatus = 0
        """

    cursor.execute(sql, (name, ))

    if aoguid_only:
        result = [row[0] for row in cursor.fetchall()]
    else:
        result = map(AddressRecord._make, cursor.fetchall())

    return result


def main():
    
    connect_string = sys.argv[1]

    street = 'Октябрьская'
    city = "Казань"

    conn = psycopg2.connect(connect_string)
    c = conn.cursor()

    for addr in find_address_for_name(c, street, aoguid_only=False):
        print addr

    conn.close()


if __name__ == "__main__":
    main()
