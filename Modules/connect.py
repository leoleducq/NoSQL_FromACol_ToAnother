#!/usr/bin/env python3.9
from pymongo import MongoClient

#Connection BDD NoSQL

def NoSQLConnect():
    client = MongoClient("url", "port")
    db = client['collection_name']
    return db
