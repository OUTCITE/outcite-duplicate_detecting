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

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_thresholds = [ #[0.001+x*0.001 for x in range(100)],
                [0.75 +x*0.001 for x in range(250)],#[0.75 +x*0.001 for x in range(250)],
                [0.75 +x*0.001 for x in range(250)],#[0.75 +x*0.001 for x in range(250)],
                [0.75 +x*0.001 for x in range(250)],#[0.75 +x*0.001 for x in range(250)],
                #[0.75 +x*0.001 for x in range(250)]
              ];

_DBSCAN       = False;
_type_combs   = [(None,None,None)];#[('PROB','PROB','PROB'),('PROB','PROB_thr','PROB'),(None,None,None)];
_similarities = ['cosim','jaccard','f1'];#['probability','cosim','jaccard','f1','overlap'];
_thrs_gold    = [10,25,50,100,250,500,1000,''];

M, refs, featsOf = get_matrix(_index,'duplicate_cluster',_label,_featyp,_ngrams_n);

for XF_type,FF_type,FX_type in _type_combs:
    labellings            = get_clusters(M,refs,featsOf,_similarities,_thresholds,XF_type,FF_type,FX_type);
    first, mention2goldID = True,None;    print(XF_type,FF_type,FX_type);
    for i in range(len(_similarities)):
        table = [];
        for j in range(len(_thresholds[i])):
            auto_labelling, gold_labelling, mention2goldID = goldlabels([ref['id'] for ref in refs],labellings[sum([len(L) for L in _thresholds[:i]])+j],[mention2goldID,_dbfile][first]);
            table                                         += [evaluate(auto_labelling,gold_labelling,_thrs_gold,['',_thresholds[i][j]])]; first = False;
        print(tabulate(table,[_similarities[i],'thr']+[attr for pair in zip(['P'+str(thr) for thr in _thrs_gold],['R'+str(thr) for thr in _thrs_gold]) for attr in pair]+['avgP','avgR','avgF1'],tablefmt="grid"));
'''
        for j in range(len(_thresholds[i])):
            print(_similarities[i], _thresholds[i][j], '#mentions:', len(auto_labelling), '#auto clusters:', max(auto_labelling)+1, '#gold clusters:', len(set(gold_labelling)) );
            try:
                display(refs,featsOf,labellings[sum([len(L) for L in _thresholds[:i]])+j],1);
            except KeyboardInterrupt:
                pass;
'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
