#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import time
import re
import sqlite3
import itertools
import json
from collections import Counter
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse import diags
from scipy.sparse.csgraph import connected_components as components
from sklearn.cluster import DBSCAN
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_scroll_size      = _configs['scroll_size'];
_max_extract_time = _configs['max_extract_time']; #minutes
_max_scroll_tries = _configs['max_scroll_tries'];

# REGEX FOR WORDS
WORD = re.compile(_configs['regex_word']); #r'[A-Za-zßäöü]{2,}');

# WHETHER TO USE DBSCAN FOR CLUSTERING
_DBSCAN = _configs['dbscan'];

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# HELPER FUNCTION TO MULTIPLY A COUNTER BY A VALUE
def multiply(counter,i):
    for key in counter:
        counter[key] *= i;
    return counter;

# HELPER FUNCTION TO GET THE CHARACTER N-GRAMS OF A STRING
def get_ngrams(string,n):
    if not string:
        return [];
    if isinstance(string,int):
        return [string];
    affix  = ''.join(['_' for j in range(n-1)]);
    string = (affix + string + affix).lower();
    return [string[i:i+n] for i in range(len(string)-(n-1))];

# RETURN THE GOLD LABELLING FROM A DATABASE FILE AND A SET OF IDS WHICH SHOULD BE IN THE DATABASE
def goldlabels(ids,DB_file,REP=False):
    con            = sqlite3.connect(DB_file);
    cur            = con.cursor();
    mention2goldID = {mentionID:goldID for mentionID,goldID in cur.execute("SELECT mentionID,"+['goldID','repID'][REP]+" FROM mentions WHERE goldID IS NOT NULL")};
    gold_labelling = [mention2goldID[mentionID] for mentionID in ids if mentionID in mention2goldID]; con.close();
    return gold_labelling, mention2goldID;

# RETURN MACHINE LABELLING BASED ON LABELLING AND SET OF IDS
def autolabels(ids,mention2goldID,labelling):
    auto_labelling = [labelling[i] for i in range(len(ids)) if ids[i] in mention2goldID];
    return auto_labelling;

# EVALUATE THE MACHINE LABELLING AGAINST THE GOLD LABELLING
def evaluate(auto_labelling,gold_labelling,thrs=[None],addvals=[]):
    precs, recs = [],[];
    for thr in thrs:
        Ts            = Counter(gold_labelling);
        autolabels    = [auto_labelling[i] for i in range(len(gold_labelling)) if Ts[gold_labelling[i]]<=thr] if thr else auto_labelling;
        goldlabels    = [gold_labelling[i] for i in range(len(gold_labelling)) if Ts[gold_labelling[i]]<=thr] if thr else gold_labelling;
        Ts            = Counter(goldlabels);
        Ps, TPs       = Counter(autolabels), Counter([(goldlabels[i],autolabels[i],) for i in range(len(goldlabels))]);
        avg_size_auto = 0 if len(Ps.values())==0 else sum(Ps.values())/len(Ps.values());
        avg_size_gold = 0 if len(Ts.values())==0 else sum(Ts.values())/len(Ts.values());
        med_size_auto = 0 if len(Ps.values())==0 else sorted(list(Ps.values()))[int(len(Ps.values())/2)];
        med_size_gold = 0 if len(Ts.values())==0 else sorted(list(Ts.values()))[int(len(Ts.values())/2)];
        T, P, TP      = sum([Ts[key]**2 for key in Ts]), sum([Ps[key]**2 for key in Ps]), sum([TPs[key]**2 for key in TPs]);
        precs.append(round(100*TP/P) if P>0 else 0); recs.append(round(100*TP/T) if T>0 else 0);
    avg_prec = 0 if len(precs)==0 else sum(precs)/len(precs);
    avg_rec  = 0 if len(recs) ==0 else sum(recs)/len(recs);
    return addvals + [val for pair in zip(precs,recs) for val in pair] + [avg_prec,avg_rec,0 if avg_prec+avg_rec==0 else round((2*avg_prec*avg_rec)/(avg_prec+avg_rec))];
    #print('max_gold:',thr,'avg/med auto cluster:',avg_size_auto,'/',med_size_auto,'avg/med gold cluster:',avg_size_gold,'/',med_size_gold,'---- T:',T,'P:',P,'TP:',TP,'---- Pre:',round(100*TP/P),'Rec:',round(100*TP/T));

# UPDATE REFERENCES OF AN INDEX BY USING A GIVEN LABELLING FUNCTION BASED ON FROM FIELD WRITING RESULT TO TO FIELD
def update_references(index,fromField,toField,label_func,featypes,ngrams_n,args,KEY=False):#similarities,thresholds,XF_type,FF_type,FX_type):
    body = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+toField: True, toField: None } } };
    for ID, size in get_distinct(fromField+['','.keyword'][KEY],index):
        if size < 1 or size > 25000:
            continue;
        M, refs, featsOf, index2feat, feat2index = get_matrix(index,fromField,ID,featypes,ngrams_n);
        labellings, samples                      = label_func(*([M,refs,index2feat]+args));#,similarities,thresholds,XF_type,FF_type,FX_type);
        labelling                                = labellings[0]; # If we wanted to use the dois, we could choose an optimal labelling
        for i in range(len(refs)):
            body                            = copy(body);
            body['_id']                     = refs[i]['id'];
            body['_source']['doc'][toField] = str(refs[i][fromField])+'_'+str(labelling[i]);
            yield body;

#M,refs,index2feat,similarities,thresholds,XF_type,FF_type,FX_type,field2ftype,fweight,bias,gold_labelling=None

'''
def update_references(index,fromField,toField,label_func):
    body       = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'has_'+toField: True, toField: None } } };
    IDs, sizes = zip(*get_distinct(fromField,index));
    for i in range(len(IDs)):
        if sizes[i] < 2 or sizes[i] > 25000:
            continue;
        M, refs, featsOf = get_matrix(index,fromField,IDs[i],_featyp,_ngrams_n);
        labellings       = label_func(M,_similarities,_thresholds,_XF_type,_FF_type,_FX_type);
        labelling        = labellings[0]; # If we wanted to use the dois, we could choose an optimal labelling
        for i in range(len(refs)):
            body                            = copy(body);
            body['_id']                     = refs[i]['id'];
            body['_source']['doc'][toField] = str(refs[i][fromField])+'_'+str(labelling[i]);
            yield body;
'''
# HELPER FUNCTION TO DISPLAY REFERENCE LABELLING
def display(refs,featsOf,auto_labelling,gold_labelling,threshold,FEATS=False):
    table     = [(refs[i],featsOf[i],auto_labelling[i],gold_labelling[i],) for i in range(len(auto_labelling))];
    histogram = Counter(auto_labelling);
    for label in histogram:
        if histogram[label] > threshold:
            for row in table:
                if row[2]==label:
                    features = dict();
                    for ftype,fval in row[1]:
                        if ftype in features:
                            features[ftype].add(fval);
                        else:
                            features[ftype] = set([fval]);
                    if FEATS:
                        print('----------------------------------------------------------------------------------------------------------------------------------------------------------\n',row[0]['reference'],'\n----------------------------------------------------------------------------------------------------------------------------------------------------------');
                        for ftype in features:
                            if features[ftype] != set([None]):
                                print('    ',ftype.upper()+':','/'.join((str(fval) for fval in features[ftype]))[:150]);
                    else:
                        print('--->',row[3], row[0]['reference']);
            input('________________________________________________________________________________________________________________________________________________________________________');

# HELPER FUNCTION TO GET ALL DISTINCT VALUES IN A GIVEN FIELD OF AN INDEX
def get_distinct(field,index):
    #----------------------------------------------------------------------------------------------------------------------------------
    query = { "exists": {"field": field} };
    aggs  = { field: { "composite": { "size":100, "sources": [ {field: {"terms": { "field": field } } } ] } } };
    #----------------------------------------------------------------------------------------------------------------------------------
    client    = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page_num  = 0;
    returned  = -1;
    while returned != 0:
        page                                             = client.search(index=index, query=query, aggs=aggs, size=0);
        returned                                         = len(page['aggregations'][field]['buckets']) if 'aggregations' in page and field in page['aggregations'] and 'buckets' in page['aggregations'][field] else 0;
        aggs[field]["composite"]["after"] = page['aggregations'][field]['after_key'] if returned > 0 else None;
        for bucket in page['aggregations'][field]['buckets']:
            yield bucket['key'][field], bucket['doc_count'];
            print("# Duplicate cluster label:",bucket['key'][field]," -- count:",bucket['doc_count'],'                  ',end='\r');

# ITERATOR OVER ALL DOCUMENTS WHICH HAVE A CERTAIN VALUE IN A CERTAIN FIELD
def get_by_fieldvalue(field,ID,index):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = { "query": { "term": { field: ID } } };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            yield doc['_source'];
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

# EXTRACT FEATURES FROM A GIVEN REFERENCE
def get_features(reference):
    refstring  = reference['reference']   if 'reference'   in reference else None;
    sowiportID = reference['sowiport_id'] if 'sowiport_id' in reference else None;
    crossrefID = reference['crossref_id'] if 'crossref_id' in reference else None;
    dnbID      = reference['dnb_id']      if 'dnb_id'      in reference else None;
    openalexID = reference['openalex_id'] if 'openalex_id' in reference else None;
    issue      = reference['issue']  if 'issue'  in reference and isinstance(reference['issue'], int) else None;
    volume     = reference['volume'] if 'volume' in reference and isinstance(reference['volume'],int) else None;
    year       = reference['year']   if 'year'   in reference and isinstance(reference['year'],  int) else None;
    source     = reference['source'] if 'source' in reference and isinstance(reference['source'],str) else None;
    title      = reference['title']  if 'title'  in reference and isinstance(reference['title'], str) else None;
    a1sur      = reference['authors'][0]['surname'] if 'authors' in reference and isinstance(reference['authors'],list) and len(reference['authors']) > 0 and 'surname' in reference['authors'][0] and isinstance(reference['authors'][0]['surname'],str) else None;
    a1init     = a1sur+'_'+reference['authors'][0]['initials'][0]   if a1sur and 'initials'   in reference['authors'][0] and reference['authors'][0]['initials']   and isinstance(reference['authors'][0]['initials'][0]  ,str) else None;
    a1first    = a1sur+'_'+reference['authors'][0]['firstnames'][0] if a1sur and 'firstnames' in reference['authors'][0] and reference['authors'][0]['firstnames'] and isinstance(reference['authors'][0]['firstnames'][0],str) else None;
    a2sur      = reference['authors'][1]['surname'] if 'authors' in reference and isinstance(reference['authors'],list) and len(reference['authors']) > 1 and 'surname' in reference['authors'][1] and isinstance(reference['authors'][1]['surname'],str) else None;
    a2init     = a2sur+'_'+reference['authors'][1]['initials'][0]   if a2sur and 'initials'   in reference['authors'][1] and reference['authors'][1]['initials']   and isinstance(reference['authors'][1]['initials'][0]  ,str) else None;
    a2first    = a2sur+'_'+reference['authors'][1]['firstnames'][0] if a2sur and 'firstnames' in reference['authors'][1] and reference['authors'][1]['firstnames'] and isinstance(reference['authors'][1]['firstnames'][0],str) else None;
    a3sur      = reference['authors'][2]['surname'] if 'authors' in reference and isinstance(reference['authors'],list) and len(reference['authors']) > 2 and 'surname' in reference['authors'][2] and isinstance(reference['authors'][2]['surname'],str) else None;
    a3init     = a3sur+'_'+reference['authors'][2]['initials'][0]   if a3sur and 'initials'   in reference['authors'][2] and reference['authors'][2]['initials']   and isinstance(reference['authors'][2]['initials'][0]  ,str) else None;
    a3first    = a3sur+'_'+reference['authors'][2]['firstnames'][0] if a3sur and 'firstnames' in reference['authors'][2] and reference['authors'][2]['firstnames'] and isinstance(reference['authors'][2]['firstnames'][0],str) else None;
    a4sur      = reference['authors'][3]['surname'] if 'authors' in reference and isinstance(reference['authors'],list) and len(reference['authors']) > 3 and 'surname' in reference['authors'][3] and isinstance(reference['authors'][3]['surname'],str) else None;
    a4init     = a4sur+'_'+reference['authors'][3]['initials'][0]   if a4sur and 'initials'   in reference['authors'][3] and reference['authors'][3]['initials']   and isinstance(reference['authors'][3]['initials'][0]  ,str) else None;
    a4first    = a4sur+'_'+reference['authors'][3]['firstnames'][0] if a4sur and 'firstnames' in reference['authors'][3] and reference['authors'][3]['firstnames'] and isinstance(reference['authors'][3]['firstnames'][0],str) else None;
    e1sur      = reference['editors'][0]['surname'] if 'editors' in reference and isinstance(reference['editors'],list) and len(reference['editors']) > 0 and 'surname' in reference['editors'][0] and isinstance(reference['editors'][0]['surname'],str) else None;
    e1init     = e1sur+'_'+reference['editors'][0]['initials'][0]   if e1sur and 'initials'   in reference['editors'][0] and reference['editors'][0]['initials']   and isinstance(reference['editors'][0]['initials'][0]  ,str) else None;
    e1first    = e1sur+'_'+reference['editors'][0]['firstnames'][0] if e1sur and 'firstnames' in reference['editors'][0] and reference['editors'][0]['firstnames'] and isinstance(reference['editors'][0]['firstnames'][0],str) else None;
    publisher1 = reference['publishers'][0]['publisher_string'] if 'publishers' in reference and isinstance(reference['publishers'],list) and len(reference['publishers']) > 0 and 'publisher_string' in reference['publishers'][0] and isinstance(reference['publishers'][0]['publisher_string'],str) else None;
    return refstring,sowiportID,crossrefID,dnbID,openalexID,issue,volume,year,source,title,a1sur,a1init,a1first,a2sur,a2init,a2first,a3sur,a3init,a3first,a4sur,a4init,a4first,e1sur,e1init,e1first,publisher1;

# HELPER FUNCTION TO GET THE CHARACTER NGRAMS OF A GIVEN STRING
def get_ngrams(string,n):
    if not string:
        return [];
    if isinstance(string,int):
        return [string];
    affix  = ''.join(['_' for j in range(n-1)]);
    string = (affix + string + affix).lower();
    return [string[i:i+n] for i in range(len(string)-(n-1))];

# HELPER FUNCTION TO GET ALL WORDS OF A GIVEN STRING
def get_words(string):
    if not string:
        return [];
    if isinstance(string,int):
        return [string];
    string = string.lower();
    return [match.group(0) for match in WORD.finditer(string)];

# HELPER FUNCTION TO GET ALL WORD NGRAMS OF A GIVEN STRING
def get_wordgrams(string,n):
    if not string:
        return [];
    if isinstance(string,int):
        return [string];
    affix  = ''.join([' ### ' for j in range(n-1)]);
    string = (affix + string + affix).lower();
    words  = get_words(string);
    return ['_'.join(words[i:i+n]) for i in range(len(words)-(n-1))];

# GET ATTRIBUTE VALUE PAIRS 
def process_features(ftypes,feats,types=None,n=None):
    ftypes_, feats_ = [],[];
    for i in range(len(feats)):
        if (not types) or types[ftypes[i]]==False:
            continue;
        additional_feats = get_ngrams(feats[i],n) if types[ftypes[i]]=='ngrams' else get_words(feats[i]) if types[ftypes[i]]=='words' else get_wordgrams(feats[i],n) if types[ftypes[i]]=='wordgrams' else [feats[i]];
        feats_          += additional_feats;
        ftypes_         += [ftypes[i] for j in range(len(additional_feats))];
    return set([(ftypes_[j],feats_[j],) for j in range(len(ftypes_))]);

# PRODUCE THE DOCUMENT FEATURE MATRIX UPON WHICH EVERYTHING ELSE IS BASED
def get_matrix(index,field,ID,featypes=None,n=None):
    index2ftype = ['refstring','sowiportID','crossrefID','dnbID','openalexID','issue','volume','year','source','title','a1sur','a1init','a1first','a2sur','a2init','a2first','a3sur','a3init','a3first','a4sur','a4init','a4first','e1sur','e1init','e1first','publisher1'];
    ftype2index = {index2ftype[i]:i for i in range(len(index2ftype))};
    references  = [reference for reference in get_by_fieldvalue(field,ID,index)];
    doc2feats   = [get_features(reference) for reference in references];
    featsOf     = [process_features(index2ftype,features,featypes,n) for features in doc2feats];
    index2feat  = list(set([]).union(*featsOf));
    feat2index  = {index2feat[i]:i for i in range(len(index2feat))};
    indexfeats  = [(i,feat2index[feat],) for i in range(len(featsOf)) for feat in featsOf[i]];
    rows, cols  = zip(*indexfeats);
    M           = csr((np.ones(len(rows),dtype=bool),(rows,cols)),shape=(len(doc2feats),len(index2feat)));
    #M           = M[:,np.ravel(M.sum(0)>1)]; # Remove all features that occur only once #TODO: This is not necessarily a good idea as it removes information about original set size
    return M,references,featsOf,index2feat,feat2index;

# COSINE SIMILARITIES FOR MATRIX
def cosim(DOT):
    NORMS = np.sqrt(DOT.diagonal());
    DENOM = diags(1/NORMS);
    return DOT.dot(DENOM).dot(DENOM.T).sorted_indices();

# JACCARD DISTANCES FOR MATRIX
def jaccard(DOT,SIZES=None,nzrows=None,nzcols=None):
    SIZES          = DOT.diagonal() if SIZES is None else SIZES;
    nzrows, nzcols = DOT.nonzero() if nzrows is None else [nzrows, nzcols];
    JACCARD        = -DOT.copy();
    JACCARD.data  += SIZES[nzrows] + SIZES[nzcols];
    JACCARD.data   = DOT.data/JACCARD.data;
    return JACCARD.sorted_indices();

# F1 SIMILARITIES FOR MATRIX
def f1(DOT,SIZES=None,nzrows=None,nzcols=None):
    SIZES          = DOT.diagonal() if SIZES is None else SIZES;
    nzrows, nzcols = DOT.nonzero() if nzrows is None else [nzrows, nzcols];
    F1             = DOT.copy();
    F1.data        = (F1.data*2) / (SIZES[nzrows] + SIZES[nzcols]);
    return F1.sorted_indices();

# OVERLAP SIMILARITY FOR MATRICES
def overlap(DOT,SIZES=None,nzrows=None,nzcols=None):
    SIZES          = DOT.diagonal() if SIZES is None else SIZES;
    nzrows, nzcols = DOT.nonzero() if nzrows is None else [nzrows, nzcols];
    OVERLAP        = DOT.copy();
    OVERLAP.data   = OVERLAP.data / np.minimum(SIZES[nzrows],SIZES[nzcols]);
    return OVERLAP.sorted_indices();

# PROBABILISTIC SIMILARITY FOR THREE INPUT MATRICES
def probability(XF,FF,FX):
    return XF.dot(FF).dot(FX).sorted_indices();

# OBTAIN THE FEATURE FEATURE MATRIX
def getFF(M,thr=None):
    x_vec, f_vec         = M.sum(1), M.sum(0);
    FX                   = csr(M.astype(int).T.multiply(1.0/x_vec.T));
    XF                   = csr(M.astype(int).multiply(1.0/f_vec));
    FF                   = FX.dot(XF).sorted_indices();
    if not thr:
        return XF,FF,FX; 
    ffrows, ffcols       = FF.nonzero();
    ffsizes              = np.ravel(FF.diagonal());
    FF.data              = FF.data / ffsizes[ffrows];
    FF.data[FF.data<thr] = 0; FF.eliminate_zeros();
    return XF,FF,FX;

# GET CLUSTERS BADED ON INPUT MATRIX AND MANY PARAMETERS
def get_clusters(M,refs,index2feat,similarities,thresholds,XF_type,FF_type,FX_type,field2ftype,fweight,bias,gold_labelling=None):
    XF,FF,FX  = getFF(M,None) if FF_type=='PROB' else getFF(M,0.5) if FF_type=='PROB_thr' else [None,csr((np.ones(M.shape[1]),(np.arange(M.shape[1]),np.arange(M.shape[1]))),dtype=int),None];
    XF        = XF if XF_type=='PROB' else M;
    FX        = FX if FX_type=='PROB' else M.T;
    ftype_ind = dict();
    for index in range(len(index2feat)):
        field = index2feat[index][0];
        ftype = field2ftype[field];
        if ftype in ftype_ind:
            ftype_ind[ftype].append(index);
        else:
            ftype_ind[ftype] = [index];
    labellings = [];
    for i in range(len(similarities)):
        SIM_all = None;
        samples = [] if not gold_labelling else get_samples(gold_labelling);
        for ftype in ftype_ind:
            #print(ftype,XF.shape,FF.shape,FX.shape,len(index2feat),M.shape);
            DOT            = XF[:,ftype_ind[ftype]].dot(FF[ftype_ind[ftype],:][:,ftype_ind[ftype]]).dot(FX[ftype_ind[ftype],:]).sorted_indices();# if FF_type else XF.dot(FX).sorted_indices();
            SIZES          = DOT.diagonal();
            nzrows, nzcols = DOT.nonzero();
            SIM            = DOT if similarities[i]=='probability' else cosim(DOT) if similarities[i]=='cosim' else jaccard(DOT,SIZES,nzrows,nzcols) if similarities[i]=='jaccard' else f1(DOT,SIZES,nzrows,nzcols) if similarities[i]=='f1' else overlap(DOT,SIZES,nzrows,nzcols);
            checksum       = SIM.sum() if similarities[i]=='probability' else SIM.diagonal().sum();
            for j in range(len(samples)):
                m1,m2,equiv,_  = samples[j];
                samples[j][-1][ftype] = SIM[m1,m2];
            SIM.data *= fweight[ftype];
            SIM_all   = SIM.copy() if SIM_all==None else SIM_all + SIM;
        #print(SIM.shape,SIM.nnz,SIM.shape[0]*SIM.shape[1])
        SIM_all.data += bias;                          # Logistic Combination # The zero cells are not biased, but their result would be below threshold anyway
        SIM_all.data  = 1/(1 + np.exp(-SIM_all.data)); # Logistic Combination
        for j in range(len(samples)):
            m1,m2,equiv,_  = samples[j];
            samples[j][-1]['all'] = SIM_all[m1,m2];
        for threshold in thresholds[i]:
            EQUIV           = SIM_all > threshold;
            if refs[i]['block_id']==238:
                print(SIM_all)#print(EQUIV.sum(),'equivalent pairs before transitive closure');
            n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
            if len(labels) != len(refs):
                print('\nERROR: Different number of labels than references:',len(labels),'vs.',len(refs));
            #compsize        = Counter(labels); print(sum(compsize[label]**2 for label in compsize),'after transitive closure');
            #print( similarities[i], threshold, len(labels), max(labels)+1 ),
            labellings.append(labels);
    return labellings,samples;

# SAMPLING PAIRS FROM GOLD STANDARD
def get_samples(gold_labelling,max_equivs_per_gold_label=1000,max_equivs=10000,max_diffs_per_gold_pair=10,max_diffs=10000):
    goldlabel2mentionindex = dict();
    for i in range(len(gold_labelling)):
        if gold_labelling[i] in goldlabel2mentionindex:
            goldlabel2mentionindex[gold_labelling[i]].append(i);
        else:
            goldlabel2mentionindex[gold_labelling[i]] = [i];
    equivalents = [];
    for goldlabel in goldlabel2mentionindex:
        equivalents_ = [];
        for m1,m2 in itertools.combinations(goldlabel2mentionindex[goldlabel],2):
            equivalents_.append((m1,m2));
            if len(equivalents_) >= max_equivs_per_gold_label:
                break;
        equivalents += equivalents_;
        if len(equivalents) >= max_equivs:
            break;
    differents  = [];
    for g1,g2 in itertools.combinations(goldlabel2mentionindex.keys(),2):
        differents_ = [];
        for m1,m2 in itertools.product(goldlabel2mentionindex[g1],goldlabel2mentionindex[g2]):
            differents_.append((m1,m2));
            if len(differents_) >= max_diffs_per_gold_pair:
                break;
        differents += differents_;
        if len(differents) >= max_diffs:
            break;
    return [[m1,m2,True,dict()] for m1,m2 in equivalents]+[[m1,m2,False,dict()] for m1,m2 in differents];


def get_clusters_(M,refs,featsOf,similarities,thresholds,XF_type,FF_type,FX_type):
    XF,FF,FX         = getFF(M,None) if FF_type=='PROB' else getFF(M,0.5) if FF_type=='PROB_thr' else [None,csr((np.ones(M.shape[1]),(np.arange(M.shape[1]),np.arange(M.shape[1]))),dtype=int),None];
    XF               = XF if XF_type=='PROB' else M;
    FX               = FX if FX_type=='PROB' else M.T;
    DOT              = XF.dot(FF).dot(FX).sorted_indices()# if FF_type else XF.dot(FX).sorted_indices();
    SIZES            = DOT.diagonal();
    nzrows, nzcols   = DOT.nonzero();
    labellings       = [];
    for i in range(len(similarities)):
        SIM      = DOT if similarities[i]=='probability' else cosim(DOT) if similarities[i]=='cosim' else jaccard(DOT,SIZES,nzrows,nzcols) if similarities[i]=='jaccard' else f1(DOT,SIZES,nzrows,nzcols) if similarities[i]=='f1' else overlap(DOT,SIZES,nzrows,nzcols);
        checksum = SIM.sum() if similarities[i]=='probability' else SIM.diagonal().sum();
        message  = 'GOOD: Similarities do normalize:' if checksum == M.shape[0] else 'BAD: Similarities do not normalize:'; print(message,checksum);
        SIM      = SIM.multiply(SIM.T) if _DBSCAN else SIM;
        SIM.data = 1.0-SIM.data        if _DBSCAN else SIM.data;
        for threshold in thresholds[i]:
            if _DBSCAN:
                dbscan          = DBSCAN(eps=threshold, min_samples=3,metric='precomputed').fit(SIM); #TODO: This did not behave as expected in response to threshold changes
                n_comps, labels = len(dbscan.labels_), dbscan.labels_;
            else:
                EQUIV           = SIM > threshold;
                n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
            print( similarities[i], threshold, len(labels), max(labels)+1 ),
            labellings.append(labels);
    return labellings;
