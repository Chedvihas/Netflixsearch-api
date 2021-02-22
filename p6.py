from flask import Flask, render_template, request,jsonify
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(['https://elastic:6cE9R2GNUefR754084TxtNgg@911c6f6acda14877af9a983a7c3db94f.eastus2.azure.elastic-cloud.com:9243/'])
################-------------API---------------###################################
app = Flask(__name__)

@app.route('/autocomplete_adult', methods=['GET'])
def autocomplete_adult():
    
    keyword = request.args.get('search')

    body =   {"size" : 5,
    "query": {
      "bool": {
          "must": [
              {
                  "match_phrase_prefix": {
                      "title": {
                          "query": keyword
                      }
                  }
              }
          ]
      }
  }
        }
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])

@app.route('/autocomplete_children', methods=['GET'])
def autocomplete_children():
    
    keyword = request.args.get('search')

    body =  { "size":5,
  "query": {
      "bool": {
          "must": [
              {
                     "match_phrase_prefix": {
                      "title": {
                          "query": keyword
                      }
                  }
              }
          ],
          "must_not": [
            { "match": { "rating": "R" }} ,
             {"match": { "rating": "PG" } },
             { "match": { "rating": "NC" }
            }
            ]
      }
  }
        }
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])


@app.route('/pagination_movies', methods=['GET'])
def pagination_movies():
    
    pagenumber = int(request.args.get('pagenumber'))
    pagesize = int(request.args.get('pagesize') )
    start = pagenumber*pagesize - (pagesize-1)
    body = {"from":start,
  "size":pagesize,
  "query": {
      "bool": {
          "must": 
              [{
                 "match" : {"type": "Movie" }
              }]
      }
  },
  "sort": [
    {
      "release_year": {"order": "desc"}
    }
  ]
}
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])


@app.route('/pagination_tvshow', methods=['GET'])
def pagination_tvshow():
    
    pagenumber = int(request.args.get('pagenumber'))
    pagesize = int(request.args.get('pagesize') )
    start = pagenumber*pagesize - (pagesize-1)
    body = {"from":start,
  "size":pagesize,
  "query": {
      "bool": {
          "must": 
              [{
                 "match" : {"type": "TV Show" }
              }]
      }
  },
  "sort": [
    {
      "release_year": {"order": "desc"}
    }
  ]
}
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])

@app.route('/exactmatch', methods=['GET'])
def exactmatch():
    
    field = request.args.get('field')
    query = request.args.get('query')
    
    body = {
      "query": {
        "match": {
          "director": query
    }
  }
}
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])


@app.route('/prefixmatch', methods=['GET'])
def prefixmatch():
    
    description = request.args.get('description')
   
    
    body = {
  "query": {
    "span_first": {
      "match": {
        "span_term": {
          "description": description
        }
      },
      "end": 1
    }
  }

}
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])




@app.route('/genresmatch', methods=['GET'])
def genresmatch():
    
    genre = request.args.get('genres')
   
    
    body = {
  "query": {
    "query_string": {
        "query": genre,
        "default_field": "listed_in"
    }
  }
     }
    res = es.search(index="netflix", doc_type="doc", body=body)
    return jsonify(res['hits']['hits'])


app.run(port=5000, debug=True)



