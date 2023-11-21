#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import re
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from datetime import date
from collections import Counter
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index            = sys.argv[1];#'references'; #'references_geocite' #'references_ssoar_gold'
_out_index        = sys.argv[2];#'duplicates'; #'duplicates_geocite' #'duplicates_ssoar_gold'

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_priority = _configs['targets'];
_GESIS    = _configs['gesis'];

SEPS = re.compile(_configs['regex_seps']); #r'[^A-Za-zßäöü]');
WORD = re.compile(_configs['regex_word']); #r'[A-Za-zßäöü]{2,}');

YEAR = date.today().year;

_body = { '_op_type': 'index',
          '_index':   _out_index,
          '_id':      None,
          '_source':  {}
        }

_tool_f1 = {
    'grobid_references_from_grobid_xml':            {'reference':80,'title':66,'year':81,'authors':69,'editors': 8,'publishers':40,'source':48,'volume':65,'issue':33,'start':77,'end':77},
    'cermine_references_from_grobid_refstrings':    {'reference':80,'title':57,'year':76,'authors':12,'editors': 0,'publishers': 0,'source':27,'volume':30,'issue':13,'start':38,'end':37},
    'cermine_references_from_cermine_xml':          {'reference':81,'title':57,'year':78,'authors':15,'editors': 0,'publishers': 0,'source':28,'volume':32,'issue':12,'start':39,'end':39},
    'anystyle_references_from_cermine_fulltext':    {'reference':79,'title':72,'year':82,'authors':63,'editors':30,'publishers':48,'source':62,'volume':63,'issue':45,'start':68,'end':71},
    'anystyle_references_from_pdftotext_fulltext':  {'reference':82,'title':74,'year':83,'authors':65,'editors':32,'publishers':49,'source':64,'volume':64,'issue':43,'start':73,'end':76},
    'anystyle_references_from_cermine_refstrings':  {'reference':81,'title':71,'year':82,'authors':57,'editors':21,'publishers':46,'source':64,'volume':63,'issue':40,'start':73,'end':75},
    'anystyle_references_from_grobid_fulltext':     {'reference':77,'title':70,'year':79,'authors':61,'editors':31,'publishers':46,'source':62,'volume':63,'issue':43,'start':77,'end':80},
    'anystyle_references_from_grobid_refstrings':   {'reference':80,'title':72,'year':83,'authors':65,'editors':31,'publishers':49,'source':64,'volume':65,'issue':44,'start':78,'end':80},
    'exparser_references_from_cermine_layout':      {'reference':63,'title':62,'year':70,'authors':56,'editors':24,'publishers':40,'source':57,'volume':60,'issue':46,'start':62,'end':64},
    'matched_references_from_sowiport':             {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_crossref':             {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_dnb':                  {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_openalex':             {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_ssoar':                {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_arxiv':                {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_econbiz':              {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_gesis_bib':            {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99},
    'matched_references_from_research_data':        {'reference':99,'title':99,'year':99,'authors':99,'editors':99,'publishers':99,'source':99,'volume':99,'issue':99,'start':99,'end':99}
}

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_topterms(strings,tools,field,threshold):
    #reps  = [set([term.lower() for term in SEPS.split(string) if len(term)>=3]) if isinstance(string,str) else set([]) for string in strings];
    reps  = [set([ngram.lower() for ngram in get_ngrams(string,3)]) if isinstance(string,str) else set([]) for string in strings];
    freqs = sum([multiply(Counter(reps[i]),_tool_f1[tools[i]][field] if field in _tool_f1[tools[i]] else 100) for i in range(len(reps))],Counter())    #Counter(' '.join(' '.join(rep) for rep in reps).split());
    tops  = [(freqs[term]/len([string for string in strings if string]),term,) for term in freqs if freqs[term]/len([string for string in strings if string])>threshold];
    return tops,reps; # Returns all words that occur in at least <threshold>*100% strings as well as the representations of all strings in the input order

def best_representative(references,field,threshold):
    strings          = [reference[field] if field in reference and reference[field] else None for reference in references] if not field in ['authors','editors','publishers'] else [', '.join([element[field[:-1]+'_string'] for element in reference[field] if isinstance(element,dict) and field[:-1]+'_string' in element and isinstance(element[field[:-1]+'_string'],str)]) if field in reference and isinstance(reference[field],list) else None for reference in references];
    tools            = [reference['pipeline'] for reference in references]
    top_terms, reps  = get_topterms(strings,tools,field,threshold); #print('====>',field),print(strings), print(top_terms);
    if len(reps) != len(strings):
        print('ERROR: Reps:',len(reps),'Strings:',len(strings));
    tops             = set(pair[1] for pair in top_terms);
    max_val, max_ind = 0,-1;
    for i in range(len(reps)):
        denom   = len(tops|reps[i]);
        if denom > 0:
            jaccard = len(tops&reps[i])/denom;
            if jaccard > max_val:
                max_val = jaccard;
                max_ind = i;
    #print('----------------------------------------------------\n',strings,top_terms,tops)
    #for rep in reps:
    #    print(rep);
    #print('----------------------------------------------------\n',reps[max_ind])
    #print(max_val,'->',references[max_ind][field] if max_ind >= 0 else None);
    return references[max_ind][field] if max_ind >= 0 else None;

def clean_int(reference,field,lower,upper): #TODO: This may always return None
    value = None;
    if field in reference and reference[field]:
        try:
            value = int(reference[field]);
        except:
            pass;#print(field,':',reference[field],'not an integer');
    try:
        lower = int(lower);
        upper = int(upper);
    except:
        #print(field,':','at least one limit not integer:',lower,upper);
        value = None; # TODO: Debatable...
    if not (value and lower <= value and value <= upper):
        value = None;
    reference[field] = value;
    #print('===========>',reference[field] if field in reference else 'n.a.',value,field,lower,upper);
    return reference;

def majority_vote(references,fields):
    values = [tuple([reference[field] for field in fields]) for reference in references]; #TODO: What if reference[field] is a list?
    freqOf = Counter(values);
    suppOf = Counter(); # Not actually support, but the opposite
    for key in freqOf:
        for key_ in freqOf:
            check = sum([key[i]==key_[i] or key[i]==None for i in range(len(fields))]);
            if check == len(fields):
                suppOf[key_] += freqOf[key];
    #for key in suppOf:
    #    print(key,':',suppOf[key]);
    return max(suppOf,key=suppOf.get) if len(suppOf)>0 else tuple([None for field in fields]);

def majority_name(names,fields):
    for i in range(len(names)):
        for key in names[i]:
            if isinstance(names[i][key],str):
                names[i][key] = [names[i][key]];
    dicts = [{field+'_'+str(i):reference[field][i] for field in reference if reference[field] for i in range(len(reference[field]))} for reference in names];
    values = [tuple([d[field] if field in d else None for field in fields]) for d in dicts];
    freqOf = Counter(values);
    suppOf = Counter(); # Not actually support, but the opposite
    for key in freqOf:
        for key_ in freqOf:
            check = sum([key[i]==None or key_[i]==None or key[i].lower()==key_[i].lower() for i in range(len(fields))]);
            if check == len(fields):
                suppOf[key_] += freqOf[key];
    representative = max(suppOf,key=suppOf.get) if len(suppOf)>0 else tuple([None for field in fields]);
    dictionary     = dict(); #TODO: Should be possible to shorten the below lines substantially
    for i in range(len(fields)):
        if representative[i]:
            key = fields[i][:-2];
            if key in dictionary:
                if key in ['initials','firstnames']:
                    dictionary[key].append(representative[i]);
            else:
                if key in ['initials','firstnames']:
                    dictionary[key] = [representative[i]];
                else:
                    dictionary[key] = representative[i];
    return dictionary;

def majority_vote_(references,fields): #TODO: Can probably be removed
    references_ = [];
    for reference in references:
        for field in fields:
            if not (field in reference and reference[field]):
                break;
            references_.append(reference);
    values  = [tuple([reference[field] for field in fields]) for reference in references_];
    counter = Counter(values);
    counts  = sorted([(counter[key],key,) for key in counter],key=lambda x:x[0]);
    return max(values,key=values.count) if len(values) > 0 else tuple([None for field in fields]);

def best_url(references,target_collections):
    url  = None;
    urls = {target_collection:[reference[target_collection+'_url'] for reference in references if target_collection+'_url' in reference and reference[target_collection+'_url']] for target_collection in target_collections}
    maxs = {target_collection:max(urls[target_collection],key=urls[target_collection].count) for target_collection in urls if urls[target_collection]};
    for target_collection in target_collections:
        if target_collection in maxs:
            return target_collection, maxs[target_collection];
    return None, None

def consolidate_references(index,duplicateIDs=[]):
    duplicateIDs = duplicateIDs if duplicateIDs else get_distinct('duplicate_id.keyword',index);
    for duplicateID, size in duplicateIDs:
        references = [reference for reference in get_by_fieldvalue('duplicate_id',duplicateID,index)];
        for i in range(len(references)):
            for field,lower,upper in [('volume',1,1000),('issue',1,1000),('year',1500,YEAR),('start',1,references[i]['end'] if 'end' in references[i] else None),('end',references[i]['start'] if 'start' in references[i] else None,10000)]:
                references[i] = clean_int(references[i],field,lower,upper);
        #[majority_name([reference['authors'][i] for reference in references if 'authors' in reference and len(reference['authors'])>i],['author_string_0','surname_0','initials_0','initials_1','initials_2','firstnames_0','firstnames_1','firstnames_2']) for i in range(50)];
        volume,issue,year,start,end = majority_vote(references,['volume','issue','year','start','end']); #print('====>',volume,issue,year,start,end) # These all correlate so to speak
        refstring                   = best_representative(references,'reference' ,0.75); #TODO: This should never become null
        title                       = best_representative(references,'title'     ,0.75); #TODO: This should never become null
        source                      = best_representative(references,'source'    ,0.30);
        place                       = best_representative(references,'place'     ,0.50); # Might correlate with the publication info (volume, etc.) but there could still be variations of the same place
        typ                         = best_representative(references,'type'      ,0.50);
        max_num_authors             = max([len(reference['authors']) if 'authors' in reference and reference['authors'] else 0 for reference in references]);
        #authors                     = best_representative(references,'authors'   ,0.30); #TODO: This should never become null
        authors                     = [majority_name([reference['authors'][i] for reference in references if 'authors' in reference and reference['authors'] and len(reference['authors'])>i],['author_string_0','surname_0','initials_0','initials_1','initials_2','firstnames_0','firstnames_1','firstnames_2']) for i in range(max_num_authors)];
        editors                     = best_representative(references,'editors'   ,0.30);
        publishers                  = best_representative(references,'publishers',0.30);
        target_collection, top_url  = best_url(references,_priority);
        _,pdf_url                   = best_url(references,['fulltext']);
        _,general_url               = best_url(references,['general']);
        volumes                     = [reference['volume'    ] if 'volume'     in reference else None for reference in references];
        issues                      = [reference['issue'     ] if 'issue'      in reference else None for reference in references];
        years                       = [reference['year'      ] if 'year'       in reference else None for reference in references];
        starts                      = [reference['start'     ] if 'start'      in reference else None for reference in references];
        ends                        = [reference['end'       ] if 'end'        in reference else None for reference in references];
        refstrings                  = [reference['reference' ] if 'reference'  in reference else None for reference in references];
        titles                      = [reference['title'     ] if 'title'      in reference else None for reference in references];
        sources                     = [reference['source'    ] if 'source'     in reference else None for reference in references];
        places                      = [reference['place'     ] if 'place'      in reference else None for reference in references];
        types                       = [reference['type'      ] if 'type'       in reference else None for reference in references];
        authorss                    = [reference['authors'   ] if 'authors'    in reference else None for reference in references];
        editorss                    = [reference['editors'   ] if 'editors'    in reference else None for reference in references];
        publisherss                 = [reference['publishers'] if 'publishers' in reference else None for reference in references];
        reference_new               = {'individual':{},'num_duplicates':len(references)};
        matches                     = {target:[ reference[target+'_id']                    if target+'_id'   in reference else None for reference in references] for target in _priority             };
        URLs                        = {target:[ [url for url in reference[target+'_urls']] if target+'_urls' in reference else []   for reference in references] for target in ['fulltext','general']};
        for field,value in [('id',duplicateID),('toCollection',target_collection if target_collection in _GESIS or not general_url else 'general'),('toID',top_url if target_collection in _GESIS or not general_url else general_url),('url_fulltext',pdf_url),('url_general',general_url),('reference',refstring),('volume',volume),('issue',issue),('year',year),('start',start),('end',end),('title',title),('source',source),('place',place),('type',typ),('authors',authors),('editors',editors),('publishers',publishers)]:
            reference_new[field] = value;
        for field,value in [('individual_matches_'+target,matches[target]) for target in _priority] + [('individual_urls_'+target,URLs[target]) for target in ['fulltext','general']] + [('individual_url_fulltext',pdf_url),('individual_url_general',general_url),('individual_volumes',volumes),('individual_issues',issues),('individual_years',years),('individual_starts',starts),('individual_ends',ends),('individual_refstrings',refstrings),('individual_titles',titles),('individual_sources',sources),('individual_places',places),('individual_types',types),('individual_author_lists',authorss),('individual_editor_lists',editorss),('individual_publisher_lists',publisherss)]:
            reference_new['individual'][field] = value;
        reference_new['ids'] = [reference['id'] for reference in references];
        for target in _priority:
            reference_new[    'matches_'+target] = list(set([match for match in matches[target] if match]));
            reference_new['num_matches_'+target] = len(reference_new['matches_'+target]);
            reference_new['has_matches_'+target] = reference_new['num_matches_'+target] > 0;
        print(URLs)
        for target in ['fulltext','general']:
            reference_new[    'urls_'+target] = list(set([url for urls in URLs[target] for url in urls]));
            reference_new['num_urls_'+target] = len(reference_new['urls_'+target]);
            reference_new['has_urls_'+target] = reference_new['num_urls_'+target] > 0;
        reference_new['matches']     = list(set([match for target in _priority for match in matches[target] if match]));
        reference_new['num_matches'] = len(reference_new['matches']);
        reference_new['has_matches'] = reference_new['num_matches'] > 0;
        reference_new['urls']        = list(set([url for target in ['fulltext','general'] for urls in URLs[target] for url in urls]));
        reference_new['num_urls'] = len(reference_new['urls']);
        reference_new['has_urls'] = reference_new['num_urls'] > 0;
        yield duplicateID,reference_new;

def get_duplicates(index):
    for dupID, duplicate in consolidate_references(_index):
        body                    = copy(_body);
        body['_id']             = dupID;
        body['_source']         = duplicate;
        yield body;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

#_duplicateIDs = [("84_2_0",None)];
#print(list(consolidate_references(_index,duplicateIDs=_duplicateIDs)));
#'''
_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_duplicates(_index)):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 10000 == 0:
        print(i);
#'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
