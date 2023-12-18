index=$1; #references index

curl -X PUT "localhost:9200/${index}?pretty" -H 'Content-Type: application/json' -d'
POST index/_update_by_query
{
  "query": {
        "constant_score" : {
            "filter" : {
                "exists" : { "field" : "cluster_id" }
            }
        }

  },
  "script" : {
      "source": "ctx._source.duplicate_id2 = ctx._source.cluster_id;"
  }
}
'

