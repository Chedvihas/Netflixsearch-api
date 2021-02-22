from flask import Flask, render_template, request,jsonify
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(HOST="http://localhost", PORT=9200)
#es = Elasticsearch()


def generator(key):
    dict = {}
    dict['_index'] = 'netflix'
    dict['_type'] = 'doc'
    

    dict['_id'] = key
    dict['title'] = data[key]['title']
    dict['type'] = data[key]['type']
    dict['director'] = data[key]['director']
    dict['cast'] = data[key]['cast']
    dict['country'] = data[key]['country']
    dict['date_added'] = data[key]['date_added']
    dict['release_year'] = data[key]['release_year']
    dict['rating'] = data[key]['rating']
    dict['duration'] = data[key]['duration']
    dict['listed_in'] = data[key]['listed_in']
    dict['description'] = data[key]['description']
    return dict

if not es.indices.exists(index="netflix"):
    
    with open(r'C:\Users\Chedvihas\Desktop\netflix.json',encoding='utf-8') as f:
        data = json.load(f)
        l = list(data['s1'].keys())


    d= []


    for i in data:
        d.append(generator(i))

    mapping = {
    "settings": {
        "netflix": {
        "max_ngram_diff": 0
        },
        "analysis": {
        "analyzer": {
            "default": {
            "tokenizer": "whitespace",
            "filter": [ "5_gram" ]
            }
        },
        "filter": {
            "5_gram": {
            "type": "ngram",
            "min_gram": 5,
            "max_gram": 5
            }
        }
        }
    }
    }
    es.indices.create(index="netflix",body=mapping,ignore=400)
    helpers.bulk(es,d)

