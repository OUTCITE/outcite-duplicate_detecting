#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from tabulate import tabulate
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_label            = sys.argv[1];
_dbfile           = 'resources/mention_labels.db';

_index            = 'references'; #'geocite' #'ssoar'

_featyp   = 'ngrams'; #words #wordgrams #None
_ngrams_n = 3;

_configs = [None]; #TODO: Define here different models to be evaluated

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def get_duplicates(M,refs,featsOf,configs):
    labellings = [];
    for config in configs:
        EQUIV           = pairwise_classifier(M,refs,featsOf,config);
        n_comps, labels = components(csgraph=EQUIV, directed=False, return_labels=True);
        labellings.append(labels);
    return labellings;

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

def is_equivalent(ref1,ref2,config): # <==================================================#TODO: Modify here!
    #TODO: Use the features in ref1 vs. ref2 to decide duplicate or not
    #      can be made dependend on the config to allow for different models
    if True:
        return True;
    return False;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_thrs_gold    = [''];

M, refs, featsOf = get_matrix(_index,'cluster_id',_label,_featyp,_ngrams_n);

labellings            = get_duplicates(M,refs,featsOf,_configs);
first, mention2goldID = True,None;
table                 = [];
for i in range(len(_configs)):
    auto_labelling, gold_labelling, mention2goldID = goldlabels([ref['id'] for ref in refs],labellings[i],[mention2goldID,_dbfile][first]);
    table                                         += [evaluate(auto_labelling,gold_labelling,_thrs_gold,['',_configs[i]])]; first = False;
    print(tabulate(table,[_configs[i],'thr']+[attr for pair in zip(['P'+str(thr) for thr in _thrs_gold],['R'+str(thr) for thr in _thrs_gold]) for attr in pair]+['avgP','avgR','avgF1'],tablefmt="grid"));
'''
    print(_configs[i], '#mentions:', len(auto_labelling), '#auto clusters:', max(auto_labelling)+1, '#gold clusters:', len(set(gold_labelling)) );
    try:
        display(refs,featsOf,labellings[sum([len(L) for L in _thresholds[:i]])+j],1);
    except KeyboardInterrupt:
        pass;
'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
