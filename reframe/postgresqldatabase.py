# -*- coding: utf-8 -*-
import sys
import logging
import psycopg2
import psycopg2.extras

class PostgresqlDatabase:

    def __init__(self, dbparams):
        self.dbhost = dbparams['dbhost']
        self.dbport = dbparams['dbport']
        self.dbdatabase = dbparams['dbdatabase']
        self.dbusr = dbparams['dbusr']
        self.dbpwd = dbparams['dbpwd']
        self.dburl = "host='"+self.dbhost+"' dbname='"+self.dbdatabase+"' user='"+self.dbusr+"' password='"+self.dbpwd+"'"

        self.x_range = range(100000,1000000)
        self.y_range = range(10000,1000000)

        logging.debug(self.dburl)

    def prepare_list(self, table_names):
        logging.debug("Postgis Version: " +  self.postgis_version())

        transformation_tables = {} # All tables we need to transform (w/ one or more geometry columns)

        for table_name in table_names:
            logging.debug(" ")
            logging.debug("***** "+table_name+" *****")

            columns = self.get_geometry_columns(table_name)

            if len(columns) == 0:
                logging.debug("This table has no geometry column(s)")
                continue

            tables = [] # All geometry attributes from one table.
            for column in columns:
                #print column, columns[column]
                logging.debug("Do we need to transform the geometry column: '" + column + "'?")

                # Check if this geometry needs to be transformed.
                # Depends on SRID. But also SRID=-1 needs to be
                # transformed most of the time
                transform = self.has_transformable_geometry(table_name, column)

                if transform:
                    logging.debug("Yes! (" + str(transform) + ")")

                    # Now check if there is a srid constraint that we need to disable/enable.
                    constraint = self.has_srid_constraint(table_name)
                    if constraint:
                        print "constraint"

                    table = {}
                    table['schema_name'] = table_name.split('.')[0]
                    table['table_name'] = table_name.split('.')[1]
                    table['geometry_column'] = column
                    table['constraint_srid'] = False # is boolean enough?
                    tables.append(table)

                else:
                    logging.debug("No.")

            transformation_tables[table_name] = tables

        return transformation_tables

    def has_srid_constraint(self, table_name):
        schema_name = table_name.split('.')[0]
        table_name = table_name.split('.')[1]

        query = """
        SELECT n.nspname, pc.relname, r.contype, r.conname, r.consrc, pg_catalog.pg_get_constraintdef(r.oid, true) as sqlstr
        FROM pg_class as pc, pg_constraint as r, pg_namespace as n
        WHERE pc.oid = r.conrelid
        AND pc.relnamespace = n.oid
        AND r.contype = 'c'
        """

        query += " AND n.nspname = '" + schema_name + "'"
        query += " AND pc.relname = '" + table_name + "'"
        query += " ORDER BY 1, 2, 4;"

        print query

        try:
            con = psycopg2.connect(self.dburl)
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query)
            rows = cur.fetchall()

            if not len(rows):
                return

            for row in rows:
                print row

                # here we need some magic to find out if it's a srid constraint.
                # remove all whitespace then something like srid(-1) or srid(21871) should be found in string
                # if found -> disable check constraint (set to true to enable it afterwards with 2056)
                # ALTER TABLE products DROP CONSTRAINT some_name; -> we need to save the name of the constraint
            #print rows
#            for row in rows:

        except psycopg2.DatabaseError, e:
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                raise UnboundLocalError(e)



    def has_transformable_geometry(self, table_name, column_name):
        # Several tests:
        # - SRID = 21781 -> True
        # - SRID = -1 but x/y in correct range (600,200) -> True
        # - Result is empty (no geometry) -> True (should check for a geometry constraint though...)

        # OMG: ST_X() do not exist in Postgis 1.5...
        # Will use ST_XMax()
        query = """
        SELECT ST_SRID("""+column_name+""") as srid,
        ST_XMax((ST_DumpPoints("""+column_name+""")).geom) as x,
        ST_YMax((ST_DumpPoints("""+column_name+""")).geom) as y
        FROM """+table_name+""" LIMIT 1;
        """

        try:
            con = psycopg2.connect(self.dburl)
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query)
            row = cur.fetchone()

            if not row:
                return "Geometry column is empty (= no rows)"

            srid = row['srid']
            x = row['x']
            y = row['y']

            if srid == 21781:
                return 21781
            elif int(x) in self.x_range and int(y) in self.y_range:
                return str(int(x)) + "/" + str(int(y))
            else:
                return False
        except psycopg2.DatabaseError, e:
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                raise UnboundLocalError(e)

    def get_geometry_columns(self, table_name):
        schema_name = table_name.split('.')[0]
        table_name = table_name.split('.')[1]

        query = """
        SELECT c.nspname, a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM pg_attribute a
        JOIN pg_class b ON (a.attrelid = b.relfilenode)
        JOIN pg_namespace c ON (c.oid = b.relnamespace)
        WHERE b.relname = '"""+table_name+"""' AND a.attstattarget = -1
        AND c.nspname = '"""+schema_name+"""';
        """

        columns = {}

        try:
            con = psycopg2.connect(self.dburl)
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                column_name = row['column_name']
                data_type = row['data_type']

                # Fake Postgis 1.5
                if len(data_type) > 7:
                    if data_type[0:8] == 'geometry':
                        columns[column_name] = data_type
        except psycopg2.DatabaseError, e:
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                raise UnboundLocalError(e)

        return columns

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
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                tables.append(row['table_schema']+"."+row['table_name'])
        except psycopg2.DatabaseError, e:
            print str(e)
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                print str(e)
                raise UnboundLocalError(e)

        return tables

    def postgis_version(self):
        query = "SELECT PostGIS_Lib_Version();"

        try:
            con = psycopg2.connect(self.dburl)
            cur = con.cursor()
            cur.execute(query)
            ver = cur.fetchone()
            return ver[0]
        except psycopg2.DatabaseError, e:
            raise psycopg2.DatabaseError(e)
        finally:
            try:
                if con:
                    con.close()
            except UnboundLocalError, e:
                raise UnboundLocalError(e)
