# -*- coding: utf-8 -*-
import sys
import logging
import psycopg2

class PostgresqlDatabase:

    def __init__(self, dbparams):
        self.dbhost = dbparams['dbhost']
        self.dbport = dbparams['dbport']
        self.dbdatabase = dbparams['dbdatabase']
        self.dbusr = dbparams['dbusr']
        self.dbpwd = dbparams['dbpwd']
        self.dburl = "host='"+self.dbhost+"' dbname='"+self.dbdatabase+"' user='"+self.dbusr+"' password='"+self.dbpwd+"'"

    def get_user_tables(self, dbschema=None):
        query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('information_schema')
        AND table_schema NOT ILIKE 'pg_%'
        """

        if dbschema:
            query +=  "AND table_schema = '"+dbschema+"'\n"

        query += "ORDER BY table_schema, table_name;"

        tables = []

        try:
            con = psycopg2.connect(self.dburl)
            #con = psycopg2.connect("host='localhost' dbname='asdf' user='"+self.dbusr+"' password='"+self.dbpwd+"'")
            cur = con.cursor()
            #cur.execute('SELECT version()')
            #ver = cur.fetchone()
            #print ver

            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                tables.append(row[0]+"."+row[1])
        except psycopg2.DatabaseError, e:
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                raise UnboundLocalError(e)

        return tables
