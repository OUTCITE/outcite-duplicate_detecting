#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import sqlite3
import time
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS-----------------------------------------------------------------------------------------------------------------------------------------

_index = sys.argv[1];
_outDB = sys.argv[2];

_max_extract_time = 0.5; #minutes
_max_scroll_tries = 2;
_scroll_size      = 100;

_refobjs = [    #'anystyle_references_from_cermine_fulltext',
                #'anystyle_references_from_cermine_refstrings',
                #'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',
                #'anystyle_references_from_pdftotext_fulltext',   #                'anystyle_references_from_gold_fulltext',
                #'cermine_references_from_cermine_xml',          #                'anystyle_references_from_gold_refstrings',
                #'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                #'grobid_references_from_grobid_xml',
                #'exparser_references_from_cermine_layout',
                #'matched_references_from_sowiport',
                #'matched_references_from_crossref',
                #'matched_references_from_dnb',
                #'matched_references_from_openalex',
                #'matched_references_from_ssoar',
                #'matched_references_from_arxiv',
                #'matched_references_from_econbiz',
                #'matched_references_from_gesis_bib'
           ];

_id_field = 'id' if _index=='users' else '@id';
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_references(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_query = { 'match_all':{} };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query,_source=[_id_field,refobj,'results_'+refobj]);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                for i in range(len(doc['_source'][refobj])):
                    fromID     = doc['_source'][_id_field];
                    reference  = doc['_source'][refobj][i];
                    linkID     = refobj+'_'+fromID+'_ref_'+str(i) if not refobj.startswith('matched_references_from_') else reference[refobj[24:]+'_id'];
                    sowiportID = reference['sowiport_id']      if 'sowiport_id'      in reference else None;
                    crossrefID = reference['crossref_id']      if 'crossref_id'      in reference else None;
                    dnbID      = reference['dnb_id']           if 'dnb_id'           in reference else None;
                    openalexID = reference['openalex_id']      if 'openalex_id'      in reference else None;
                    econbizID  = reference['econbiz_id']       if 'econbiz_id'       in reference else None;
                    arxivID    = reference['arxiv_id']         if 'arxiv_id'         in reference else None;
                    ssoarID    = reference['ssoar_id']         if 'ssoar_id'         in reference else None;
                    dataID     = reference['research_data_id'] if 'research_data_id' in reference else None;
                    bibID      = reference['gesis_bib_id']     if 'gesis_bib_id'     in reference else None;
                    issue      = reference['issue']            if 'issue'            in reference and isinstance(reference['issue'], int) else None;
                    volume     = reference['volume']           if 'volume'           in reference and isinstance(reference['volume'],int) else None;
                    year       = reference['year']             if 'year'             in reference and isinstance(reference['year'],  int) else None;
                    source     = reference['source']           if 'source'           in reference and isinstance(reference['source'],str) else None;
                    title      = reference['title']            if 'title'            in reference and isinstance(reference['title'], str) else reference['reference']  if 'reference' in reference and isinstance(reference['reference'], str) else None;
                    a1sur      = reference['authors'][0]['surname'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 0 and 'surname' in reference['authors'][0] and isinstance(reference['authors'][0]['surname'],str) else reference['authors'][0]['author_string'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 0 and 'author_string' in reference['authors'][0] and isinstance(reference['authors'][0]['author_string'],str) else None;
                    a1init     = a1sur+'_'+reference['authors'][0]['initials'][0]   if a1sur and 'initials'   in reference['authors'][0] and reference['authors'][0]['initials']   and isinstance(reference['authors'][0]['initials'][0]  ,str) else None;
                    a1first    = a1sur+'_'+reference['authors'][0]['firstnames'][0] if a1sur and 'firstnames' in reference['authors'][0] and reference['authors'][0]['firstnames'] and isinstance(reference['authors'][0]['firstnames'][0],str) else None;
                    a2sur      = reference['authors'][1]['surname'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 1 and 'surname' in reference['authors'][1] and isinstance(reference['authors'][1]['surname'],str) else reference['authors'][1]['author_string'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 1 and 'author_string' in reference['authors'][1] and isinstance(reference['authors'][1]['author_string'],str) else None;
                    a2init     = a2sur+'_'+reference['authors'][1]['initials'][0]   if a2sur and 'initials'   in reference['authors'][1] and reference['authors'][1]['initials']   and isinstance(reference['authors'][1]['initials'][0]  ,str) else None;
                    a2first    = a2sur+'_'+reference['authors'][1]['firstnames'][0] if a2sur and 'firstnames' in reference['authors'][1] and reference['authors'][1]['firstnames'] and isinstance(reference['authors'][1]['firstnames'][0],str) else None;
                    a3sur      = reference['authors'][2]['surname'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 2 and 'surname' in reference['authors'][2] and isinstance(reference['authors'][2]['surname'],str) else reference['authors'][2]['author_string'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 2 and 'author_string' in reference['authors'][2] and isinstance(reference['authors'][2]['author_string'],str) else None;
                    a3init     = a3sur+'_'+reference['authors'][2]['initials'][0]   if a3sur and 'initials'   in reference['authors'][2] and reference['authors'][2]['initials']   and isinstance(reference['authors'][2]['initials'][0]  ,str) else None;
                    a3first    = a3sur+'_'+reference['authors'][2]['firstnames'][0] if a3sur and 'firstnames' in reference['authors'][2] and reference['authors'][2]['firstnames'] and isinstance(reference['authors'][2]['firstnames'][0],str) else None;
                    a4sur      = reference['authors'][3]['surname'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 3 and 'surname' in reference['authors'][3] and isinstance(reference['authors'][3]['surname'],str) else reference['authors'][3]['author_string'] if 'authors' in reference and reference['authors'] and len(reference['authors']) > 3 and 'author_string' in reference['authors'][3] and isinstance(reference['authors'][3]['author_string'],str) else None;
                    a4init     = a4sur+'_'+reference['authors'][3]['initials'][0]   if a4sur and 'initials'   in reference['authors'][3] and reference['authors'][3]['initials']   and isinstance(reference['authors'][3]['initials'][0]  ,str) else None;
                    a4first    = a4sur+'_'+reference['authors'][3]['firstnames'][0] if a4sur and 'firstnames' in reference['authors'][3] and reference['authors'][3]['firstnames'] and isinstance(reference['authors'][3]['firstnames'][0],str) else None;
                    e1sur      = reference['editors'][0]['surname'] if 'editors' in reference and reference['editors'] and len(reference['editors']) > 0 and 'surname' in reference['editors'][0] and isinstance(reference['editors'][0]['surname'],str) else reference['editors'][0]['editor_string'] if 'editors' in reference and reference['editors'] and len(reference['editors']) > 0 and 'editor_string' in reference['editors'][0] and isinstance(reference['editors'][0]['editor_string'],str) else None;
                    e1init     = e1sur+'_'+reference['editors'][0]['initials'][0]   if e1sur and 'initials'   in reference['editors'][0] and reference['editors'][0]['initials']   and isinstance(reference['editors'][0]['initials'][0]  ,str) else None;
                    e1first    = e1sur+'_'+reference['editors'][0]['firstnames'][0] if e1sur and 'firstnames' in reference['editors'][0] and reference['editors'][0]['firstnames'] and isinstance(reference['editors'][0]['firstnames'][0],str) else None;
                    publisher1 = reference['publishers'][0]['publisher_string'] if 'publishers' in reference and reference['publishers'] and len(reference['publishers']) > 0 and 'publisher_string' in reference['publishers'][0] and isinstance(reference['publishers'][0]['publisher_string'],str) else None;
                    yield (linkID,refobj,sowiportID,crossrefID,dnbID,openalexID,econbizID,arxivID,ssoarID,dataID,bibID,issue,volume,year,source,title,a1sur,a1init,a1first,a2sur,a2init,a2first,a3sur,a3init,a3first,a4sur,a4init,a4first,e1sur,e1init,e1first,publisher1,);
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

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_con = sqlite3.connect(_outDB);
_cur = _con.cursor();

_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

_cur.execute("DROP   TABLE IF EXISTS refmetas");
_cur.execute("CREATE TABLE           refmetas(linkID TEXT PRIMARY KEY, fromPipeline TEXT, sowiportID TEXT, crossrefID TEXT, dnbID TEXT, openalexID TEXT, econbizID TEXT, arxivID TEXT, ssoarID TEXT, research_dataID TEXT, gesis_bibID TEXT, issue INT, volume INT, year INT,source TEXT, title TEXT, a1sur TEXT, a1init TEXT, a1first TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, e1sur TEXT, e1init TEXT, e1first TEXT, publisher1 TEXT)");
for refobj in _refobjs:
    print('Loading deduplication metadata from '+refobj+'...');
    _cur.executemany("INSERT OR IGNORE INTO refmetas VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",get_references(_index,refobj));
    _con.commit();

_con.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
