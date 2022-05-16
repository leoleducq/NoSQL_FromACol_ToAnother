#!/usr/bin/env python3.9
from pymongo import MongoClient

#Connection BDD NoSQL

def NoSQLConnect():
    client = MongoClient("localhost", 27017)
    db = client['testpython']
    return db