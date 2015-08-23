# -*- coding: utf-8 -*-
from PyQt4.QtCore import *
from qgis.core import *

import os

class Transformation:

    def __init__(self):
        self.triangles = []
        with open(os.path.join(os.path.dirname(__file__), "chenyx06.tri")) as f:
            self.triangles = [line.strip() for line in f]

        print self.triangles


    def fubar(self):
        print "asdf"
        fields = QgsFields()
        fields.append(QgsField("first", QVariant.Int))
        fields.append(QgsField("second", QVariant.String))

        print

#fields = QgsFields()
#fields.append(QgsField("first", QVariant.Int))
#fields.append(QgsField("second", QVariant.String))
