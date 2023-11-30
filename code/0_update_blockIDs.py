#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
import sqlite3
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE DOCUMENT INDEX TO UPDATE THE REFERENCES IN
_index  = sys.argv[1];
# THE LOCAL DATABASE WITH THE DUPLICATE BLOCK LABELLING
_dup_db = sys.argv[2];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size       = _configs['chunk_size_blocks'];
_request_timeout  = _configs['request_timeout_blocks'];
_max_extract_time = _configs['max_extract_time_blocks'];
_max_scroll_tries = _configs['max_scroll_tries_blocks'];
_scroll_size      = _configs['scroll_size_blocks'];

# WETHER TO UPDATE THE DUPLICATE BLOCK FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_blocks'];

# THE PIPELINES TO CONSIDER FOR UPDATING THE CORRESPONDING REFERENCES' DUPLICATE BLOCKS
_refobjs = _configs['refobjs'];

# THE LIST OF IDS IF ONLY TO UPDATE SOME DOCUMENTS
_ids = _configs['ids'];

# FIELD NAME IN THE DOCUMENT INDEX WHERE TO WRITE THE BLOCK IDS FOR ALL ITS REFERENCES
_field    = "block_ids";
# FIELD NAME IN THE REFERENCES WHERE TO WRITE THE BLOCK ID
_id_field = "block_id";
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# GET THE BLOCK IDS FROM THE DATABASE FOR THE REFOBJECTS BASED ON THEIR IDENTIFIER
def get_blockID(refobjects,id_field,pipeline,docid,cur):
    labels = [];
    for i in range(len(refobjects)):
        mentionID = pipeline+'_'+docid +'_ref_'+str(i) if not pipeline.startswith('matched_references_from_') else refobjects[i][pipeline[24:]+'_id'];
        rows      = cur.execute("SELECT label FROM mentions WHERE mentionID=?",(mentionID,)).fetchall();
        label     = rows[0][0] if len(rows) >= 1 else None;
        if label != None:
            refobjects[i][id_field] = label;
            labels.append(label);
    return set(labels), refobjects;

# SCROLLING OVER INPUT DOCUMENTS AND UPDATING THEIR REFERENCES
def search(field,id_field,index,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    body     = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+field: False, 'processed_'+field: True, 'num_'+field:0, field: None } } };
    scr_body = { "query": { "ids": { "values": _ids } } } if _ids else {'query':{'bool':{'must_not':{'term':{'has_'+field: True}}}}} if not recheck else {'query':{'match_all':{}}};
    #----------------------------------------------------------------------------------------------------------------------------------
    con = sqlite3.connect(_dup_db);
    cur = con.cursor();
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_s = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body        = copy(body);
            body['_id'] = doc['_id'];
            ids         = set([]);#set(doc['_source'][field]) if field in doc['_source'] and doc['_source'][field] != None else set([]);
            for refobj in _refobjs:
                previous_refobjects            = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                new_ids, new_refobjects        = get_blockID(previous_refobjects,id_field,refobj,doc['_id'],cur) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                           |= new_ids;
                body['_source']['doc'][refobj] = new_refobjects; # The updated ones
                #print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            #print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]        = list(ids) if len(ids) > 0 else None;
            body['_source']['doc']['has_'+field] = True      if len(ids) > 0 else False;
            body['_source']['doc']['num_'+field] = len(ids);
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

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENT INDEX WITH THE BLOCK IDS
i = 0;
for success, info in bulk(_client,search(_field,_id_field,_index,_recheck,),chunk_size=_chunk_size, request_timeout=_request_timeout):
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
