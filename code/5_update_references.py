#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import re
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index     = sys.argv[1];
_dup_index = sys.argv[2];

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_recheck = _configs['recheck_references'];
_ids     = _configs['ids'];

_chunk_size       =  _configs['chunk_size_references'];
_request_timeout  =  _configs['request_timeout_references'];
_scroll_size      =  _configs['scroll_size_references'];
_max_extract_time =  _configs['max_extract_time_references'];
_max_scroll_tries =  _configs['max_scroll_tries_references'];

_featyp, _ngrams_n = _configs['featype'], _configs['ngrams_n']; #words #wordgrams #None #5

_similarities, _thresholds = _configs['similarities'], _configs['thresholds']; #jaccard #f1 #overlap #None
_XF_type,_FF_type,_FX_type = _configs['XF_type'], _configs['FF_type'], _configs['FX_type'];

_refobjs = _configs['refobjs'];

_fields = _configs['fields'];

_scr_query = { "ids": { "values": _ids } } if _ids else {'bool':{'must_not':{'term':{'has_duplicate_ids': True}}}} if not _recheck else {'match_all':{}};

_body = { '_op_type': 'update', '_index': _index, '_id': None, '_source': { 'doc': { 'has_duplicate_ids': False, 'num_duplicate_ids': 0, 'processed_duplicate_ids': True, 'has_cluster_ids': False, 'num_cluster_ids': 0, 'processed_cluster_ids': True } } };

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def update_refobjects(refobjects,fromID,toolchain,client,index,fields):
    query          = {'term':{'ids.keyword':None}};
    cluIDs         = [];
    dupIDs         = [];
    new_refobjects = [];
    for i in range(len(refobjects)):
        refID = toolchain+'_'+fromID+'_ref_'+str(i);
        new_refobjects.append(copy(refobjects[i]));
        new_refobjects[-1]['id']     = refID;
        query['term']['ids.keyword'] = refID;
        results                      = [(result['_source'],result['_id'],) for result in client.search(index=index,query=query)['hits']['hits']];
        #print(len(results),'results in duplicates index for',refID);
        if len(results) > 0:
            duplicate, dupID                   = results[0];
            cluID                              = '_'.join(dupID.split('_')[:-1]) if dupID else None;
            new_refobjects[-1]['cluster_id'  ] = cluID;
            new_refobjects[-1]['duplicate_id'] = dupID;
            if refID in duplicate['ids']:
                cluIDs.append(cluID);
                dupIDs.append(dupID);
                for field in fields: # Remember that the entire reference will be REPLACED not updated!
                    new_refobjects[-1][field+'_original'] = new_refobjects[-1][field+'_original'] if field+'_original' in new_refobjects[-1] else new_refobjects[-1][field] if field in new_refobjects[-1] else None;
                    new_refobjects[-1][field]             = duplicate[field] if field in duplicate else None;
                #print(new_refobjects[-1]);
            else:
                print('Could not find',refID,'in',duplicate['ids'],'.');
    return new_refobjects,dupIDs,cluIDs;

def update_docs(index,index_m,fields):
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=_scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            body        = copy(_body);
            body['_id'] = doc['_id'];
            #------------------------------------------------------------------------------------------------------------------------------
            cluster_ids   = [];
            duplicate_ids = [];
            for refobj in _refobjs:
                previous_refobjects            = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else [];
                new_refobjects, dupIDs, cluIDs = update_refobjects(previous_refobjects,doc['_id'],refobj,client_m,index_m,fields);
                if len(dupIDs) == 0:
                    continue;
                body['_source']['doc'][refobj]           = new_refobjects;
                cluster_ids   += cluIDs;
                duplicate_ids += dupIDs;
                #print(refobj,'=> Updating references of',body['_id'],'by attributes of duplicates',[refobject['id']+' --> '+str(refobject['duplicate_id']) for refobject in new_refobjects if 'duplicate_id' in refobject]);
            #------------------------------------------------------------------------------------------------------------------------------
            body['_source']['doc']['cluster_ids']       = list(set(cluster_ids));
            body['_source']['doc']['duplicate_ids']     = list(set(duplicate_ids));
            body['_source']['doc']['has_cluster_ids']   = len(cluster_ids)   > 0;
            body['_source']['doc']['has_duplicate_ids'] = len(duplicate_ids) > 0;
            body['_source']['doc']['num_cluster_ids']   = len(set(cluster_ids));
            body['_source']['doc']['num_duplicate_ids'] = len(set(duplicate_ids));
            #------------------------------------------------------------------------------------------------------------------------------
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,update_docs(_index,_dup_index,_fields),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
