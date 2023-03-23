#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import sqlite3
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index_references = 'references';

_typeDB = sys.argv[1];

_chunk_size       =  250;
_requestimeout    =   60;
_scroll_size      =  500;
_max_extract_time =    1; #minutes
_max_scroll_tries =    2;

_matchfield = {'sowiport': 'id.keyword',
               'crossref': 'DOI.keyword',
               'openalex': 'id.keyword' }

_typefield  = {'sowiport': 'subtype',
               'crossref': 'type',
               'openalex': 'type' }
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_references(index,target,typefield):
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query={'term':{'has_'+target+'_id':True}},_source=['pipeline',target+'_id','type']);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #------------------------------------------------------------------------------------------------------------------------------
            refID   = doc['_id'];
            tool    = doc['_source']['pipeline'];
            matchID = doc['_source'][target+'_id']
            typ     = doc['_source'][typefield] if typefield in doc['_source'] else None;
            #------------------------------------------------------------------------------------------------------------------------------
            if typ:
                yield (refID,matchID,tool,typ,);
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

def get_matched_types(index,field,value):
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,query={'term':{field:value}},_source=['type']);
    returned = len(page['hits']['hits']);
    if returned>1:
        print('For some reason multiple documents found!',index,field,value);
    doc = page['hits']['hits'][0] if returned > 0 else {'_source':{}};
    typ = doc['_source']['type'] if 'type' in doc['_source'] else None;
    return typ;


def get_rows(target):
    for refID,matchID,tool,typ in get_references(_index_references,target,_typefield[target]):
        match_type = get_matched_types(target,_matchfield[target],matchID);
        if match_type:
            yield (refID,tool,typ,match_type,);
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------
con = sqlite3.connect(_typeDB);
cur = con.cursor();

for target in ['sowiport','crossref','openalex']:
    print(target);
    cur.execute('DROP   TABLE IF EXISTS '+target+'_types');
    cur.execute('CREATE TABLE           '+target+'_types(refID TEXT PRIMARY KEY, tool TEXT, system_type TEXT, gold_type TEXT)');
    cur.executemany('INSERT INTO '+target+'_types VALUES(?,?,?,?)',get_rows(target));

con.commit();
#-------------------------------------------------------------------------------------------------------------------------------------------------
