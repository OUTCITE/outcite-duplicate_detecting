#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import sqlite3
import time
import json
from tabulate import tabulate
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE DOCUMENT INDEX TO GET THE REFERENCES FROM
_index     = sys.argv[1];
# THE REFERENCE INDEX TO WRITE THE REFERENCES TO
_out_index = sys.argv[2];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size       = _configs['chunk_size_refindex'];
_max_extract_time = _configs['max_extract_time_refindex']; #minutes
_max_scroll_tries = _configs['max_scroll_tries_refindex'];
_scroll_size      = _configs['scroll_size_refindex'];

# USING ORIGINAL FIELDS IF AVAILABLE (CAN BE USED TO RESET THE ORIGINAL VALUES AFTER DUPLICATE DETECTION)
_original = _configs['original_values'];

# THE PIPELINES TO CONSIDER FOR INDEXING THE CORRESPONDING REFERENCES
_refobjs = _configs['refobjs'];

# THE BODY TO UPDATE THE REFERENCES INDEX
_body = { '_op_type': 'index',
          '_index':   _out_index,
          '_id':      None,
          '_source':  {}
        }
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# SCROLLING OVER THE DOCUMENTS TO YIELD THEIR REFERENCES
def get_references(index):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_query = { 'match_all':{} };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['http://localhost:9200'],timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query,_source=['@id']+_refobjs);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        print('====>',page_num)
        for doc in page['hits']['hits']:
            for refobj in _refobjs:
                if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                    for i in range(len(doc['_source'][refobj])):
                        fromID                  = doc['_source']['@id'];
                        reference               = doc['_source'][refobj][i];
                        linkID                  = refobj+'_'+fromID+'_ref_'+str(i) if not refobj.startswith('matched_references_from_') else reference[refobj[24:]+'_id'];
                        reference['id']         = linkID;
                        reference['pipeline']   = refobj;
                        reference['fromID']     = fromID;
                        reference['list_index'] = i;
                        reference['from_index'] = index;
                        body                    = copy(_body);
                        body['_id']             = linkID;
                        for key in sorted(list(reference.keys())): # _original ones will come after their respective keys
                            if key.endswith('_original'):
                                if _original:
                                    body['_source'][key[:-9]] = reference[key]; # Overwrite the non-original with the original
                            else:
                                body['_source'][key] = reference[key];
                        for to_collection in ['sowiport','crossref','dnb','openalex','ssoar','arxiv','research_data','gesis_bib','econbiz','fulltext','general']: #TODO: last item not tested
                            body['_source']['has_'+to_collection+ '_id'] = (to_collection+ '_id' in reference and reference[to_collection+ '_id']!=None);
                            body['_source']['has_'+to_collection+'_url'] = (to_collection+'_url' in reference and reference[to_collection+'_url']!=None);
                        yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);

#-------------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client   = ES(['http://localhost:9200'],timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS REFERENCE INDEX
i = 0;
for success, info in bulk(_client,get_references(_index),chunk_size=_chunk_size):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 1 == 0:
        print(i,end='\r');
