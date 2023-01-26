#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
from tabulate import tabulate
from common import *
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_label            = sys.argv[1];
_dbfile           = 'resources/mention_labels.db';
_samplefile       = 'resources/training/labels_'+str(_label);

_index            = 'references'; #'geocite' #'ssoar'

_featypes = {   'refstring':    'ngrams', #ngrams #words #wordgrams #None
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

_ftype = { 'refstring':    'refstring',
           'sowiportID':   'matchID',
           'crossrefID':   'matchID',
           'dnbID':        'matchID',
           'openalexID':   'matchID',
           'issue':        'pubnumber',
           'volume':       'pubnumber',
           'year':         'pubnumber',
           'source':       'source',
           'title':        'title',
           'a1sur':        'surname',
           'a1init':       'init',
           'a1first':      'first',
           'a2sur':        'surname',
           'a2init':       'init',
           'a2first':      'first',
           'a3sur':        'surname',
           'a3init':       'init',
           'a3first':      'first',
           'a4sur':        'surname',
           'a4init':       'init',
           'a4first':      'first',
           'e1sur':        'editor',
           'e1init':       'editor',
           'e1first':      'editor',
           'publisher1':   'publisher' }

_fweight = { 'refstring': 0.72939795,
             'matchID':   0.0,
             'pubnumber': 0.36280852,
             'source':    0.56819319,
             'title':     7.3914298,
             'surname':  -1.53487169,
             'init':      1.21194258,
             'first':     0.36810172,
             'editor':   -0.67533247,
             'publisher': 0.15776101 }

_bias = -5.55875478

_ngrams_n = 3;

_show_clusters = True;
_show_feats    = False;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_thresholds = [ #[0.001+x*0.001 for x in range(100)],
                #[0.9 +x*0.0001 for x in range(1000)],#[0.75 +x*0.001 for x in range(250)],
                #[0.9 +x*0.0001 for x in range(1000)],#[0.75 +x*0.001 for x in range(250)],
                #[0.9 +x*0.0001 for x in range(1000)],#[0.75 +x*0.001 for x in range(250)],
                [0.1]#[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.99,0.999,0.9999]#[0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95],[0.7,0.75,0.8,0.85,0.9,0.95,0.96,0.97,0.98,0.99]#[0.75 +x*0.001 for x in range(250)]#,[0.75 +x*0.001 for x in range(250)],[0.75 +x*0.001 for x in range(250)]
              ];

_DBSCAN       = False;
_type_combs   = [(None,None,None)];#[('PROB','PROB','PROB'),('PROB','PROB_thr','PROB'),(None,None,None)];
_similarities = ['jaccard']#['cosim','jaccard','f1'];#['probability','cosim','jaccard','f1','overlap'];
_thrs_gold    = ['']#[10,25,50,100,250,500,1000,''];

M, refs, featsOf, index2feat, feat2index = get_matrix(_index,'block_id',_label,_featypes,_ngrams_n);
samples                                  = None;

for XF_type,FF_type,FX_type in _type_combs:
    gold_labelling, mention2goldID = goldlabels([ref['id'] for ref in refs],_dbfile);
    labellings, samples            = get_clusters(M,refs,index2feat,_similarities,_thresholds,XF_type,FF_type,FX_type,_ftype,_fweight,_bias,gold_labelling); print(XF_type,FF_type,FX_type);
    OUT = open(_samplefile+'_'+str(XF_type)+'_'+str(FF_type)+'_'+str(FX_type)+'.txt','w');
    OUT.write('\n'.join([' '.join([str(similarity['all'])]+[str(similarity[ftype]) if ftype in similarity else '0' for ftype in _fweight]+[str(int(equiv))]) for m1,m2,equiv,similarity in samples]));
    OUT.close();
    print(len([sample for sample in samples if sample[2]]),'positive samples and',len([sample for sample in samples if not sample[2]]),'negative samples');
    for i in range(len(_similarities)):
        table = [];
        for j in range(len(_thresholds[i])):
            auto_labelling = autolabels([ref['id'] for ref in refs],mention2goldID,labellings[sum([len(L) for L in _thresholds[:i]])+j]);
            table         += [evaluate(auto_labelling,gold_labelling,_thrs_gold,['',_thresholds[i][j]])];
        print(tabulate(table,[_similarities[i],'thr']+[attr for pair in zip(['P'+str(thr) for thr in _thrs_gold],['R'+str(thr) for thr in _thrs_gold]) for attr in pair]+['avgP','avgR','avgF1'],tablefmt="grid"));
        if _show_clusters:
            for j in range(len(_thresholds[i])):
                print(_similarities[i], _thresholds[i][j], '#mentions:', len(auto_labelling), '#auto clusters:', max(auto_labelling)+1, '#gold clusters:', len(set(gold_labelling)) );
                try:
                    display(refs,featsOf,labellings[sum([len(L) for L in _thresholds[:i]])+j],gold_labelling,0,_show_feats);
                except KeyboardInterrupt:
                    pass;


#-------------------------------------------------------------------------------------------------------------------------------------------------
