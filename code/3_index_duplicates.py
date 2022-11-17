#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import re
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from datetime import date
from collections import Counter
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

_index            = 'references'; #'geocite' #'ssoar'
_out_index        = 'duplicates';

_priority = ['sowiport','crossref','dnb','openalex','bing'];

SEPS = re.compile(r'[^A-Za-zßäöü]');
WORD = re.compile(r'[A-Za-zßäöü]{2,}');
YEAR = date.today().year;

_body = { '_op_type': 'index',
          '_index':   _out_index,
          '_id':      None,
          '_source':  {}
        }

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_ngrams(string,n):
    if not string:
        return [];
    if isinstance(string,int):
        return [string];
    affix  = ''.join(['_' for j in range(n-1)]);
    string = (affix + string + affix).lower();
    return [string[i:i+n] for i in range(len(string)-(n-1))];

def get_topterms(strings,threshold):
    #reps  = [set([term.lower() for term in SEPS.split(string) if len(term)>=3]) if isinstance(string,str) else set([]) for string in strings];
    reps  = [set([ngram.lower() for ngram in get_ngrams(string,3)]) if isinstance(string,str) else set([]) for string in strings];
    freqs = Counter(' '.join(' '.join(rep) for rep in reps).split());
    tops  = [(freqs[term]/len(strings),term,) for term in freqs if freqs[term]/len(strings)>threshold];
    return tops,reps; # Returns all words that occur in at least <threshold>*100% strings as well as the representations of all strings in the input order

def best_representative(references,field,threshold):
    strings          = [reference[field] if field in reference and reference[field] else None for reference in references] if not field in ['authors','editors','publishers'] else [', '.join([element[field[:-1]+'_string'] for element in reference[field] if isinstance(element,dict) and field[:-1]+'_string' in element and isinstance(element[field[:-1]+'_string'],str)]) if field in reference and isinstance(reference[field],list) else None for reference in references];
    top_terms, reps  = get_topterms(strings,threshold); #print('====>',field),print(strings), print(top_terms);
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
    values = [tuple([reference[field] for field in fields]) for reference in references];
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

def majority_vote_(references,fields):
    references_ = [];
    for reference in references:
        for field in fields:
            if not (field in reference and reference[field]):
                break;
            references_.append(reference);
    values  = [tuple([reference[field] for field in fields]) for reference in references_];
    counter = Counter(values);
    counts  = sorted([(counter[key],key,) for key in counter],key=lambda x:x[0]);
    #for freq,key in counts:
    #    print(key,':',freq);
    #freqs = Counter(values);
    return max(values,key=values.count) if len(values) > 0 else tuple([None for field in fields]);

def best_url(references):
    url  = None;
    urls = {target_collection:[reference[target_collection+'_url'] for reference in references if target_collection+'_url' in reference and reference[target_collection+'_url']] for target_collection in _priority}
    maxs = {target_collection:max(urls[target_collection],key=urls[target_collection].count) for target_collection in urls if urls[target_collection]};
    for target_collection in _priority:
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
        volume,issue,year,start,end = majority_vote(references,['volume','issue','year','start','end']); #print('====>',volume,issue,year,start,end) # These all correlate so to speak
        refstring                   = best_representative(references,'reference' ,0.75); #TODO: This should never become null
        title                       = best_representative(references,'title'     ,0.75); #TODO: This should never become null
        source                      = best_representative(references,'source'    ,0.30);
        place                       = best_representative(references,'place'     ,0.50); # Might correlate with the publication info (volume, etc.) but there could still be variations of the same place
        authors                     = best_representative(references,'authors'   ,0.30); #TODO: This should never become null
        editors                     = best_representative(references,'editors'   ,0.30);
        publishers                  = best_representative(references,'publishers',0.30);
        target_collection, url      = best_url(references);
        reference_new               = {};
        for field,value in [('id',duplicateID),('toCollection',target_collection),('toID',url),('reference',refstring),('volume',volume),('issue',issue),('year',year),('start',start),('end',end),('title',title),('source',source),('place',place),('authors',authors),('editors',editors),('publishers',publishers)]:
            reference_new[field] = value;
        reference_new['ids'] = [reference['id'] for reference in references];
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
_client   = ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,get_duplicates(_index)):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 10000 == 0:
        print(i);
#'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
