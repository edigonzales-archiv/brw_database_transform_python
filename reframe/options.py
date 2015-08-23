# -*- coding: utf-8 -*-

from argparse import ArgumentParser

class Options:

    def __init__(self):
        self._init_parser()

    def _init_parser(self):
        usage = 'bin/reframe'
        self.parser = ArgumentParser(usage=usage)
        self.parser.add_argument('-x',
                                '--example',
#                                 required=True,
                                default='example-value',
                                dest='example',
                                help='An example option')

        self.parser.add_argument('--simulate',
                                action = 'store_true',
                                default = False,
                                dest ='simulate',
                                help = "Do not transform data. Simulate only!")

        self.parser.add_argument('--white',
                                action = 'store',
                                type = str,
                                dest ='white',
                                help = 'File with whitelisted tables.')

        self.parser.add_argument('--black',
                                action = 'store',
                                type = str,
                                dest ='black',
                                help = 'File with blacklisted tables.')

        self.parser.add_argument('--dbschema',
                                action = 'store',
                                type = str,
                                dest ='dbschema',
                                help = 'Transform this dbschema only.')

        self.parser.add_argument('--dbhost',
                                action = 'store',
                                type = str,
                                default = 'localhost',
                                dest ='dbhost',
                                help = 'The host name of the server. Default: localhost.')

        self.parser.add_argument('--dbport',
                                action = 'store',
                                type = str,
                                default = '5432',
                                dest ='dbport',
                                help = 'The port number the server is listening on. Default: 5432.')

        self.parser.add_argument('--dbdatabase',
                                action = 'store',
                                type = str,
                                default = 'xanadu2',
                                dest ='dbdatabase',
                                help = 'The database name. Default: xanadu.')

        self.parser.add_argument('--dbusr',
                                action = 'store',
                                type = str,
                                default = 'stefan',
                                dest ='dbusr',
                                help = 'User name to access database. Default: stefan.')

        self.parser.add_argument('--dbpwd',
                                action = 'store',
                                type = str,
                                default = 'ziegler12',
                                dest ='dbpwd',
                                help = 'Password of user used to access database. Default: ziegler12.')

        self.parser.add_argument('--log',
                                action = 'store',
                                type = str,
                                default = 'reframe.log',
                                dest ='logfile',
                                help = 'Log message to given file. Default: reframe.log.')


    def parse(self, args=None):
        return self.parser.parse_args(args)
