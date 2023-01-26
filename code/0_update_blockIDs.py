#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
import sqlite3
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index  = sys.argv[1]; #'geocite' #'ssoar'
_dup_db = sys.argv[2];#"/home/outcite/duplicate_detecting/resources/mention_labels.db"

_chunk_size       =  250;
_max_scroll_tries =    2;
_scroll_size      =  500;
_request_timeout  =   60;
_max_extract_time =    1; #minutes

_recheck = True;

_field    = "block_ids"
_id_field = "block_id"

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'anystyle_references_from_pdftotext_fulltext',
                'cermine_references_from_cermine_xml',   #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml',
                'exparser_references_from_cermine_layout' ];

_ids = None;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_blockID(refobjects,id_field,pipeline,docid,cur):
    labels = [];
    for i in range(len(refobjects)):
        mentionID = pipeline+'_'+docid+'_ref_'+str(i);
        rows      = cur.execute("SELECT label FROM mentions WHERE mentionID=?",(mentionID,)).fetchall();
        label     = rows[0][0] if len(rows) >= 1 else None;
        if label != None:
            refobjects[i][id_field] = label;
            labels.append(label);
    return set(labels), refobjects;

#def get_dupID(refobjects,id_field,to_field,pipeline,docid,client,index): # Can be used both for stage II and stage III identifiers
#    mentionIDs = [pipeline+'_'+docid+'_ref_'+str(i) for i in range(len(refobjects))];
#    results    = client.search(index=index,query={"query":{"ids" :{"values":mentionIDs}}});
#    mapping    = {results['hits']['hits'][i]['_id']: results['hits']['hits'][i]['_source'][id_field]};
#    labels     = [];
#    for i in range(len(refobjects)):
#        label = mapping[mentionIDs[i]] if mentionIDs[i] in mapping else None;
#        if label != None:
#            refobjects[i][to_field] = label;
#            labels.append(label);
#    return set(labels), refobjects;

def search(field,id_field,index,recheck):
    #----------------------------------------------------------------------------------------------------------------------------------
    body     = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+field: False, 'processed_'+field: True, 'num_'+field:0, field: None } } };
    scr_body = { "query": { "ids": { "values": _ids } } } if _ids else {'query':{'bool':{'must_not':{'term':{'has_'+field: True}}}}} if not recheck else {'query':{'match_all':{}}};
    #print(scr_body);
    #----------------------------------------------------------------------------------------------------------------------------------
    con = sqlite3.connect(_dup_db);
    cur = con.cursor();
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    client_s = ES(['localhost'],scheme='http',port=9200,timeout=60);
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

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

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
