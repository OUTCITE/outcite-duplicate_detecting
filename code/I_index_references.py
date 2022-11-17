import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import sqlite3
import time
from tabulate import tabulate
from elasticsearch.helpers import parallel_bulk as bulk

_max_extract_time = 0.5; #minutes
_max_scroll_tries = 2;
_scroll_size      = 100;

_index         = sys.argv[1];
_out_index     = 'references';
_chunk_size    = 1000;

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'cermine_references_from_cermine_xml',          #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml' ];

_body = { '_op_type': 'index',
          '_index':   _out_index,
          '_id':      None,
          '_source':  {}
        }


def get_references(index):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id']+_refobjs };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            for refobj in _refobjs:
                if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                    for i in range(len(doc['_source'][refobj])):
                        reference               = doc['_source'][refobj][i];
                        fromID                  = doc['_source']['@id'];
                        linkID                  = refobj+'_'+fromID+'_ref_'+str(i);
                        reference['id']         = linkID;
                        reference['pipeline']   = refobj;
                        reference['fromID']     = fromID;
                        reference['list_index'] = i;
                        reference['from_index'] = index;
                        body                    = copy(_body);
                        body['_id']             = linkID;
                        body['_source']         = reference;
                        for to_collection in ['sowiport','crossref','dnb','openalex']:
                            body['_source']['has_'+to_collection+ '_id'] = to_collection+ '_id' in reference and reference[to_collection+ '_id';
                            body['_source']['has_'+to_collection+'_url'] = to_collection+'_url' in reference and reference[to_collection+'_url';
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

_client   = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_references(_index)):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 10000 == 0:
        print(i);
