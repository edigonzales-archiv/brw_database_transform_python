# -*- coding: utf-8 -*-
import os
import sys
import logging
import time
import datetime
import psycopg2

from options import Options
from postgresqldatabase import PostgresqlDatabase
from transformation import Transformation

def main():
    # Read the options and arguments from the command line (w/ some default settings).
    options = Options()
    opts = options.parse(sys.argv[1:])

    # Configure logging.
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename = opts.logfile, filemode = "w", format = FORMAT, level = logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler())

    # Create a dbparams map for convenience.
    dbparams = {}
    dbparams['dbhost'] = opts.dbhost
    dbparams['dbport'] = opts.dbport
    dbparams['dbdatabase'] = opts.dbdatabase
    dbparams['dbusr'] = opts.dbusr
    dbparams['dbpwd'] = opts.dbpwd

    # Behaviour:
    # dbschema = 'all': Transform all tables except blacklisted ones.
    # dbschema = 'some valid schema': Transform this schema AND the whitelisted ones except the blacklisted ones.
    # dbschema NOT set at all: Transform only whitelisted tables w/o blacklisted ones.

    tables = []
    whitelist = []
    blacklist = []

## WIE SIEHT DAS MIT DEN RECHTEN AUS?
## Falls keine Rechte, kann ichs einfach nicht lesen.

    pg = PostgresqlDatabase(dbparams)

    try:
        if opts.white:
            whitelist = read_black_or_white_list(opts.white)

        if opts.black:
            blacklist = read_black_or_white_list(opts.black)

        if opts.dbschema == "all":
            tables = pg.get_user_tables()
        elif opts.dbschema:
            tables = pg.get_user_tables(opts.dbschema)

        # Append whitelist to tables.
        tables.extend(whitelist)

        # Remove blacklisted tables.
        tables = [x for x in tables if x not in blacklist]

        if len(tables) == 0:
            logging.info("No tables found.")
            sys.exit(1)

        # Sort out anything that has no geometry or one we don't need to
        # transform.
        # Figure out:
        # - SRID
        # - geometry constraint.
        prepared_tables = pg.prepare_list(tables)

        ## TEST:
        #transform = Transformation()
        #transform.fubar()


    except (psycopg2.DatabaseError, UnboundLocalError) as e:
        # Needs some love: it throws "local variable 'con' referenced before assignment"
        # even if it is a database error. Other way round: the database error is not
        # thrown visually.
        logging.error(str(e))
        sys.exit(1)

def read_black_or_white_list(filename):
    with open(filename) as f:
        return [line.strip() for line in f]

if __name__ == '__main__':
    sys.exit(main())
