#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index            = sys.argv[1];#'references';

_chunk_size       =  250;
_request_timeout  =   60;

_ngrams_n          = 3;
_configs           = [None];

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

_target_collections = ['sowiport','crossref','dnb','openalex','ssoar','arxiv','econbiz','gesis_bib','research_data'];

_min_title_sim  = 0.75;
_min_author_sim = 0.5;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_duplicates(M,refs,featsOf,configs): #TODO: May want to adapt like get_clusters and also adapt the tuning script for duplicates to be like the tuning for clusterIDs?
    labellings = [];
    for config in configs:
        EQUIV           = pairwise_classifier(M,refs,featsOf,config);
        n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
        labellings.append(labels);
    return labellings, [];

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

def is_equivalent(ref1,ref2,config):
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    for target_collection in _target_collections:
        if target_collection+'_id' in ref1 and target_collection+'_id' in ref2 and ref1[target_collection+'_id']==ref2[target_collection+'_id']:
            return True;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'year' in ref1 and 'year' in ref2 and isinstance(ref1['year'],int) and isinstance(ref2['year'],int) and abs(ref1['year']-ref2['year'])>1:
        print('year too different:',ref1['year'],' | vs | ',ref2['year']);
        return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'title' in ref1 and 'title' in ref2 and isinstance(ref1['title'],str) and isinstance(ref2['title'],str) and ref1['title'] and ref2['title']:
        ngrams1, ngrams2 = set(get_ngrams(ref1['title'],3)), set(get_ngrams(ref2['title'],3));
        overlap          = len(ngrams1&ngrams2) / min([len(ngrams1),len(ngrams2)]); # if one is subset of the other then sim=1
        if overlap < _min_title_sim:
            print('title too different:',ref1['title'],' | vs | ',ref2['title']);
            return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'authors' in ref1 and 'authors' in ref2 and isinstance(ref1['authors'],list) and isinstance(ref2['authors'],list) and ref1['authors'] and ref2['authors']:
        authors1 = ' '.join( [ author['author_string'] for author in ref1['authors'] if 'author_string' in author and isinstance(author['author_string'],str) and author['author_string'] ] ).replace(',',' ');
        authors2 = ' '.join( [ author['author_string'] for author in ref2['authors'] if 'author_string' in author and isinstance(author['author_string'],str) and author['author_string'] ] ).replace(',',' ');
        ngrams1, ngrams2   = set([]),set([]);
        for author_str in authors1.split():
            ngrams1 |= set(get_ngrams(author_str,3));
        for author_str in authors2.split():
            ngrams2 |= set(get_ngrams(author_str,3));
        overlap = len(ngrams1&ngrams2) / min([len(ngrams1),len(ngrams2)]) if min([len(ngrams1),len(ngrams2)])>0 else 0; # if one is subset of the other then sim=1
        if overlap < _min_author_sim:
            print('authors too different:',authors1.split(),' | vs | ',authors2.split());
            return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    return True;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,update_references(_index,'cluster_id','duplicate_id',get_duplicates,_featypes,_ngrams_n,[_configs],True),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
