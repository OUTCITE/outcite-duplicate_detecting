#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from scipy.optimize import linear_sum_assignment as LSA
from difflib import SequenceMatcher as SM
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# THE REFERENCE INDEX TO UPDATE THE REFERENCES IN
_index = sys.argv[1];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size      =  _configs['chunk_size_duplicates'];
_request_timeout =  _configs['request_timeout_duplicates'];

# FEATURE PARAMETERS FOR SIMILARITY COMPUTATION
_ngrams_n   = _configs['ngrams_n'];
_dateweight = _configs['dateweight'];

# THRESHOLDS FOR PAIRWISE DUPLICATE DECISIONS
_threshold      = _configs['threshold'];
_max_title_diff = _configs['max_title_diff'];
_thr_prec       = _configs['thr_prec'];
_min_title_sim  = _configs['min_title_sim'];
_min_author_sim = _configs['min_author_sim'];

# REGEXES FOR DETECTING GARBAGE, NAME SEPARATORS AND YEARS
GARBAGE   = re.compile(_configs['regex_garbage'])#re.compile(r'[\x00-\x1f\x7f-\x9f]|(-\s+)');
NAMESEP   = re.compile(_configs['regex_namesep']);
YEAR      = re.compile(_configs['regex_year']); #1500--2023

# WHICH TARGET COLLECTIONS TO USE FOR IDENTIFIER MATCHING
_target_collections = _configs['targets'];

# HOW TO TURN EACH INPUT FIELD INTO FEATURES WITH NONE MEANING NOT USED
_featypes = {   'refstring':    'ngrams',  #words #wordgrams #None
                'sowiportID':   False,
                'crossrefID':   False,
                'dnbID':        False,
                'openalexID':   False,
                'issue':        None,
                'volume':       None,
                'year':         None,
                'source':       'ngrams',
                'title':        'ngrams',
                'a1sur':        'ngrams',
                'a1init':       None,
                'a1first':      'ngrams',
                'a2sur':        'ngrams',
                'a2init':       None,
                'a2first':      'ngrams',
                'a3sur':        'ngrams',
                'a3init':       None,
                'a3first':      'ngrams',
                'a4sur':        'ngrams',
                'a4init':       None,
                'a4first':      'ngrams',
                'e1sur':        'ngrams',
                'e1init':       None,
                'e1first':      'ngrams',
                'publisher1':   'ngrams' }

# MAPPING THE INPUT TO THE COMPARISON OBJECTS
_transformap = [ ('reference', "source['reference']"),
                 ('year',      "source['year']"),
                 ('authors',   "source['authors']"),
                 ('title',     "source['title']"),
                 ('doi',       "source['doi']"),
                 ('publisher', "source['publisher']"),
                 ('editor',    "source['editor']"),
                 ('start',     "source['start']"),
                 ('end',       "source['end']"),
                 ('place',     "source['place']"),
                 ('source',    "source['source']"),
                 ('volume',    "source['volume']"),
                 ('issue',     "source['issue']") ];
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# CREATE DUPLICATE GROUPS AS TRANSITIVE CLOSURE OF PAIRWISE CLASSIFIER DECISIONS
def get_duplicates(M,refs,featsOf,configs):
    labellings = [];
    for config in configs:
        EQUIV           = pairwise_classifier(M,refs,featsOf,config);
        n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
        labellings.append(labels);
    return labellings, [];

# WRAPPER FUNCTION AROUND PAIRWISE DUPLICATE CLASSIFIER
def pairwise_classifier(M,refs,featsOf,config):
    # Given sparse matrix M with docIndex->featIndices,
    # create sparse Boolean M.shape[0]*M.shape[0] docIndex->docIndex matrix
    # such that any cell with True corresponds to a classifier decision of duplicate pair
    # if wanted, M and featsOf can be used as well, here we assume simply refobjects are compared
    rows_out,cols_out = [],[];
    for row in range(M.shape[0]):
        for col in range(M.shape[0]):
            if is_equivalent(refs[row],refs[col],config):
                rows_out.append(row);
                cols_out.append(col);
    N = csr((np.ones(len(rows_out),dtype=bool),(rows_out,cols_out)),dtype=bool,shape=(M.shape[0],M.shape[0]));
    return N;

# TRANSLATING INPUT ACCORDING TO TRANSFORMATION MAPPING TO BE ABLE TO COMPARE
def transform(source,transformap):
    target = dict();
    for target_key, source_str in transformap:
        source_val = None;
        try:
            source_val = eval(source_str,{'source':source},{'source':source});
        except Exception as e:
            pass;
        if source_val:
            target[target_key] = source_val;
    return target;

# DISTANCE FUNCTIONS
def distance(a,b):
    a,b        = a.lower(), b.lower();
    s          = SM(None,a,b);
    overlap    = sum([block.size for block in s.get_matching_blocks()]);
    return 1-(overlap / max([len(a),len(b)]));

def distance_2(a,b):
    a,b      = a.lower(), b.lower();
    s        = SM(None,a,b);
    overlap  = sum([block.size for block in s.get_matching_blocks()]);
    dist     = max([len(a),len(b)]) - overlap;
    return dist;

def distance_3(a,b):
    a,b        = '_'+re.sub(GARBAGE,'',a.lower()),'_'+re.sub(GARBAGE,'',b.lower());
    s          = SM(None,a,b);
    overlap    = sum([block.size**1 for block in s.get_matching_blocks() if block.size>=2]);
    dist       = min([len(a),len(b)])**1-overlap;
    return dist;

# HELPER FUNCTION TO FLATTEN A DICTIONARY
def flatten(d, parent_key='', sep='_'):
    items = [];
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k;
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items());
        else:
            items.append((new_key, v));
    return dict(items);

# HELPER FUNCTION TO GET ATTRIBUTE VALUE PAIRS FROM A NESTED DICTIONARY
def pairfy(d, parent_key='', sep='_'): # To be applied after flatten!
    for key in d:
        if isinstance(d[key],list):
            for el in d[key]:
                if isinstance(el,dict):
                    for a,b in pairfy(el,key,sep):
                        yield (a,str(b),);
                else:
                    yield (parent_key+sep+key,str(el),);
        else:
            yield (parent_key+sep+key,str(d[key]),);

# HELPER FUNCTION TO CREATE A DICTIONARY FROM KEY VALUE PAIRS WHERE THE SAME KEY CAN EXIST MULTIPLE TIMES AND THEN THE VALUES ARE APPENDED IN A LIST
def dictfy(pairs):
    d = dict();
    for attr,val in pairs:
        if not attr in d:
            d[attr] = [];
        d[attr].append(val);
    return d;

# FIND THE BEST MAPPING BETWEEN TWO LISTS OF STRINGS BASED ON THE USED DISTANCE
def assign(A,B): # Two lists of strings
    M          = np.array([[distance_3(a,b) if isinstance(a,str) and isinstance(b,str) else a!=b for b in B] for a in A]);
    rows, cols = LSA(M);
    mapping    = [pair for pair in zip(rows,cols)];
    costs      = [M[assignment] for assignment in mapping];
    return mapping,costs;

# CHECK IF TWO VALUES ARE SIMILAR ENOUGH TO BE CONSIDERED EQUIVALENT
def similar_enough(a,b,cost,threshold):
    if isinstance(a,str) and isinstance(b,str):
        if YEAR.fullmatch(a) and YEAR.fullmatch(b):
            y1, y2 = int(a), int(b);
            return abs(y1-y2) <= 1; # A one year difference between years is accepted
        return cost / min([len(a),len(b)])**1 < threshold;#max and not **1
    return a == b;

# COMPARE TWO REFERENCE STRINGS WITH CODE FROM EVALUATION
def compare_refstrings(P_strings,T_strings,threshold): # Two lists of strings
    mapping,costs = assign(P_strings,T_strings);
    pairs         = [(P_strings[i],T_strings[j],) for i,j in mapping];
    matches       = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if     similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    mismatches    = [(P_strings[mapping[i][0]],T_strings[mapping[i][1]],) for i in range(len(mapping)) if not similar_enough(P_strings[mapping[i][0]],T_strings[mapping[i][1]],costs[i],threshold)];
    precision     = len(matches) / len(P_strings);
    recall        = len(matches) / len(T_strings);
    return precision, recall, len(matches), len(P_strings), len(T_strings), matches, mismatches, mapping, costs;

# COMPARE TWO REFERENCE OBJECTS WITH CODE FROM EVALUATION
def compare_refobject(P_dict,T_dict,threshold):                       # Two dicts that have already been matched based on refstring attribute
    P_pairs     = pairfy(flatten(P_dict));                            # All attribute-value pairs from the output dict
    T_pairs     = pairfy(flatten(T_dict));                            # All attribute-value pairs from the gold   dict
    P_pair_dict = dictfy(P_pairs);                                    # Output values grouped by attributes in a dict
    T_pair_dict = dictfy(T_pairs);                                    # Gold   values grouped by attributes in a dict
    P_keys      = set(P_pair_dict.keys());                            # Output attributes
    T_keys      = set(T_pair_dict.keys());                            # Gold attributes
    TP_keys     = P_keys & T_keys;                                    # Attributes present in output and gold
    P           = sum([len(P_pair_dict[P_key]) for P_key in P_keys]); # Number of attribute-value pairs in output
    T           = sum([len(T_pair_dict[T_key]) for T_key in T_keys]); # Number of attribute-value pairs in gold object
    TP          = 0;                                                  # Number of attribute-value pairs in output and gold
    matches     = [];
    mismatches  = [];
    mapping     = [];
    costs       = [];
    for TP_key in TP_keys:
        prec, rec, TP_, P_, T_, matches_, mismatches_, mapping_, costs_ = compare_refstrings(P_pair_dict[TP_key],T_pair_dict[TP_key],threshold);
        TP                                                             += TP_;
        matches                                                        += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in matches_    ];
        mismatches                                                     += [(TP_key,str(match_0),str(match_1),) for match_0,      match_1      in mismatches_ ];
        mapping                                                        += [(TP_key,assignment_0,assignment_1,) for assignment_0, assignment_1 in mapping_    ];
        costs                                                          += [(TP_key,cost_,)                     for cost_                      in costs_      ];
    return TP/P, TP/T, TP, P, T, matches, mismatches, mapping, costs;

# PAIRWISE RULE-BASED DUPLICATE CLASSIFIER
def is_equivalent(ref1,ref2,config): # I think the idea is that I can give config instead of None, but at this point it is not used at all
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    for target_collection in _target_collections:
        if target_collection+'_id' in ref1 and target_collection+'_id' in ref2 and ref1[target_collection+'_id']==ref2[target_collection+'_id']:
            return True;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    ref1,ref2 = transform(ref1,_transformap), transform(ref2,_transformap);
    matchobj_ = {key:ref1[key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in ref1['authors'] if 'author_string' in author and author['author_string']] for key in ref1 if ref1[key] not in [None,'None',' ',''] };
    refobj_   = {key:ref2[key] if key!='authors' else [{'author_string':[part for part in NAMESEP.split(author['author_string']) if part]} for author in ref2['authors'] if 'author_string' in author and author['author_string']] for key in ref2 if ref2[key] not in [None,'None',' ',''] };
    prec, rec, tp, p, t, matches, mismatches, mapping, costs = compare_refobject(matchobj_,refobj_,_threshold);
    matchprec                                                = sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])/(sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])+sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in mismatches])) if sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in matches])+sum([min(len(a),len(b)) if key!='_year' else _dateweight for key,a,b in mismatches]) > 0 else 0;
    if len(matches)+len(mismatches) > 0:
        print('Matchprec:',matchprec,'Precision:',prec,'Recall:',rec,'\n___________________________________');
        print('Matches:   ',matches);
        print('Mismatches:',mismatches,'\n___________________________________');
    title1 = ref1['title'][0] if 'title' in ref1 and isinstance(ref1['title'],list) and len(ref1['title'])>0 else '' if 'title' in ref1 and isinstance(ref1['title'],list) else ref1['title'] if 'title' in ref1 else None;
    title2 = ref2['title'][0] if 'title' in ref2 and isinstance(ref2['title'],list) and len(ref2['title'])>0 else '' if 'title' in ref2 and isinstance(ref2['title'],list) else ref2['title'] if 'title' in ref2 else None;
    if title1 and title2 and distance(title1,title2) < _max_title_diff:
        if matchprec >= _thr_prec and len(matches)>1:
            print('DID MATCH:',matchprec,'>=',_thr_prec,'and #matches =',len(matches));
            return True;
        print('DID NOT MATCH.');
    if (not title1) or not title2:
        print('FAILED:',title1,title2);
    else:
        print('distance',distance(title1,title2),'>=',_max_title_diff,'and/or did not match');
    return False;

# DELETE
def is_equivalent_(ref1,ref2,config):
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    for target_collection in _target_collections:
        if target_collection+'_id' in ref1 and target_collection+'_id' in ref2 and ref1[target_collection+'_id']==ref2[target_collection+'_id']:
            return True;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'year' in ref1 and 'year' in ref2 and isinstance(ref1['year'],int) and isinstance(ref2['year'],int) and abs(ref1['year']-ref2['year'])>1:
        print('year too different:',ref1['year'],' | vs | ',ref2['year']);
        return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'title' in ref1 and 'title' in ref2 and isinstance(ref1['title'],str) and isinstance(ref2['title'],str) and ref1['title'] and ref2['title']:
        ngrams1, ngrams2 = set(get_ngrams(ref1['title'],3)), set(get_ngrams(ref2['title'],3));
        overlap          = len(ngrams1&ngrams2) / min([len(ngrams1),len(ngrams2)]); # if one is subset of the other then sim=1
        if overlap < _min_title_sim:
            print('title too different:',ref1['title'],' | vs | ',ref2['title']);
            return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    if 'authors' in ref1 and 'authors' in ref2 and isinstance(ref1['authors'],list) and isinstance(ref2['authors'],list) and ref1['authors'] and ref2['authors']:
        authors1 = ' '.join( [ author['author_string'] for author in ref1['authors'] if 'author_string' in author and isinstance(author['author_string'],str) and author['author_string'] ] ).replace(',',' ');
        authors2 = ' '.join( [ author['author_string'] for author in ref2['authors'] if 'author_string' in author and isinstance(author['author_string'],str) and author['author_string'] ] ).replace(',',' ');
        ngrams1, ngrams2   = set([]),set([]);
        for author_str in authors1.split():
            ngrams1 |= set(get_ngrams(author_str,3));
        for author_str in authors2.split():
            ngrams2 |= set(get_ngrams(author_str,3));
        overlap = len(ngrams1&ngrams2) / min([len(ngrams1),len(ngrams2)]) if min([len(ngrams1),len(ngrams2)])>0 else 0; # if one is subset of the other then sim=1
        if overlap < _min_author_sim:
            print('authors too different:',authors1.split(),' | vs | ',authors2.split());
            return False;
    #-----------------------------------------------------------------------------------------------------------------------------------------------
    return True;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS INDEX WITH THE DUPLICATE IDS
i = 0;
for success, info in bulk(_client,update_references(_index,'cluster_id','duplicate_id',get_duplicates,_featypes,_ngrams_n,[[None]],True),chunk_size=_chunk_size, request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
