from flask import Flask, render_template, request,jsonify
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(['https://elastic:6cE9R2GNUefR754084TxtNgg@911c6f6acda14877af9a983a7c3db94f.eastus2.azure.elastic-cloud.com:9243/'])

def upload():
    global data
    if  not es.indices.exists(index="netflix"):
        
        with open(r'.\netflix.json',encoding='utf-8') as f:
            data = json.load(f)
            l = list(data['s1'].keys())

        d= []

        for i in data:
            d.append(generator(i))

        helpers.bulk(es,d)

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