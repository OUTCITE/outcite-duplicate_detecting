#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = 'references';

_chunk_size       =  250;
_request_timeout  =   60;

_featyp, _ngrams_n = 'ngrams', 3; #words #wordgrams #None #5
_configs           = [None];

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_duplicates(M,refs,featsOf,configs):
    labellings = [];
    for config in configs:
        EQUIV           = pairwise_classifier(M,refs,featsOf,config);
        n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
        labellings.append(labels);
    return labellings;

def pairwise_classifier(M,refs,featsOf,config):
    # Given sparse matrix M with docIndex->featIndices,
    # create sparse Boolean M.shape[0]*M.shape[0] docIndex->docIndex matrix
    # such that any cell with True corresponds to a classifier decision of duplicate pair
    # if wanted, M and featsOf can be used as well, here we assume simply refobjects are compared
    rows_out,cols_out = [],[];
    for row in range(M.shape[0]):
        for col in range(M.shape[0]):
            if is_equivalent(refs[row],refs[col],config):
                rows_out.append(row);
                cols_out.append(col);
    N = csr((np.ones(len(rows_out),dtype=bool),(rows_out,cols_out)),dtype=bool,shape=(M.shape[0],M.shape[0]));
    return N;

def is_equivalent(ref1,ref2,config): # <==================================================#TODO: Modify here!
    #TODO: Use the features in ref1 vs. ref2 to decide duplicate or not
    #      can be made dependend on the config to allow for different models
    if True:
        return True;
    return False;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,update_references(_index,'cluster_id','duplicate_id',get_duplicates,_featyp,_ngrams_n,[_configs],True),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
