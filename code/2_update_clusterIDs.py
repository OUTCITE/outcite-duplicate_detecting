#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import re
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1];#'references';

_chunk_size       =  250;
_request_timeout  =   60;

_featypes = {   'refstring':    'ngrams',  #words #wordgrams #None
                'sowiportID':   False,
                'crossrefID':   False,
                'dnbID':        False,
                'openalexID':   False,
                'issue':        None,
                'volume':       None,
                'year':         None,
                'source':       'ngrams',
                'title':        'ngrams',
                'a1sur':        'ngrams',
                'a1init':       None,
                'a1first':      'ngrams',
                'a2sur':        'ngrams',
                'a2init':       None,
                'a2first':      'ngrams',
                'a3sur':        'ngrams',
                'a3init':       None,
                'a3first':      'ngrams',
                'a4sur':        'ngrams',
                'a4init':       None,
                'a4first':      'ngrams',
                'e1sur':        'ngrams',
                'e1init':       None,
                'e1first':      'ngrams',
                'publisher1':   'ngrams' }

_ftype = { 'refstring':       'refstring',
           'sowiportID':      'matchID',
           'crossrefID':      'matchID',
           'dnbID':           'matchID',
           'openalexID':      'matchID',
           'ssoarID':         'matchID',
           'arxivID':         'matchID',
           'gesis_bibID':     'matchID',
           'research_dataID': 'matchID',
           'issue':           'pubnumber',
           'volume':          'pubnumber',
           'year':            'pubnumber',
           'source':          'source',
           'title':           'title',
           'a1sur':           'surname',
           'a1init':          'init',
           'a1first':         'first',
           'a2sur':           'surname',
           'a2init':          'init',
           'a2first':         'first',
           'a3sur':           'surname',
           'a3init':          'init',
           'a3first':         'first',
           'a4sur':           'surname',
           'a4init':          'init',
           'a4first':         'first',
           'e1sur':           'editor',
           'e1init':          'editor',
           'e1first':         'editor',
           'publisher1':      'publisher' }

_fweight = { 'refstring': 0.72939795,
             'matchID':   0.0,
             'pubnumber': 0.36280852,
             'source':    0.56819319,
             'title':     7.3914298,
             'surname':  -1.53487169,
             'init':      1.21194258,
             'first':     0.36810172,
             'editor':   -0.67533247,
             'publisher': 0.15776101 }

_bias = -5.55875478;

_ngrams_n = 3;

_similarities, _thresholds = ['jaccard'], [[0.1]]; #jaccard #f1 #overlap #None
_XF_type,_FF_type,_FX_type = None, None,None#'PROB', 'PROB_thr', 'PROB';

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,update_references(_index,'block_id','cluster_id',get_clusters,_featypes,_ngrams_n,[_similarities,_thresholds,_XF_type,_FF_type,_FX_type,_ftype,_fweight,_bias],False),chunk_size=_chunk_size, request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    #print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
