# -*- coding: utf-8 -*-
import os
import sys
import logging
import time
import datetime
import psycopg2

from options import Options
from postgresqldatabase import PostgresqlDatabase

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

    # tables.append(whitelist)

    pg = PostgresqlDatabase(dbparams)

    try:
        tables = pg.get_user_tables(opts.dbschema)

        if opts.white:
            white = read_black_or_white_list(opts.white)
            #print white

        if opts.black:
            black = read_black_or_white_list(opts.black)
            #print black

        if opts.dbschema == "all":
            print "all"
        elif opts.dbschema:
            print "ein schema plus white minus black"
        else:
            print "nur whitelist falls vorhanden... minus blacklist."

    except (psycopg2.DatabaseError, UnboundLocalError) as e:
        logging.error(str(e))
        sys.exit(1)



def read_black_or_white_list(filename):
    with open(filename) as f:
        return f.readlines()

if __name__ == '__main__':
    sys.exit(main())
