#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import re
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = 'references';

_chunk_size       =  250;
_requestimeout    =   60;

_featyp, _ngrams_n         = 'ngrams', 3; #words #wordgrams #None #5

_similarities, _thresholds = ['jaccard'], [[0.8]]; #jaccard #f1 #overlap #None
_XF_type,_FF_type,_FX_type = 'PROB', 'PROB_thr', 'PROB';

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,update_references(_index,'block_id','cluster_id',get_clusters,_featyp,_ngrams_n,[_similarities,_thresholds,_XF_type,_FF_type,_FX_type],False),chunk_size=_chunk_size, request_timeout=_requestimeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
