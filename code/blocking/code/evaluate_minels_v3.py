#--------------------------------------------------------------------------------------------------------------------------------------------------
#-IMPORTS------------------------------------------------------------------------------------------------------------------------------------------
import sys
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from collections import Counter, defaultdict
import random
#--------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS------------------------------------------------------------------------------------------------------------------------------------------
_minel_DB = sys.argv[1];#"components_authors.db"; #"institutions_components.db"                         #sys.argv[1];
_repre_DB = sys.argv[2];#"/data_ssd/backests/Repositories/pubdedup/representations_authors/representations_v2.db"; #"representations_institutions_v2/representations.db" #sys.argv[2];
_feats_DB = sys.argv[3];#"/data_ssd/backests/Repositories/pubdedup/representations_authors/features.db"; #"representations_institutions_v2/features.db"        #sys.argv[3];
_batch    = 1000;
_fn_num   = 3;
_fp_num   = 2;
#--------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS----------------------------------------------------------------------------------------------------------------------------------------
def transitive_reduction(M,max_depth):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    reduction = edges.copy();
    num, i    = 1,2;
    while num > 0 and i <= max_depth+1: #TODO: the smaller should not be required but it seems that sometimes there are cycles in the graph
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        reduction  = reduction > new;
        #print('...',i,':',num;
        i += 1;
        reduction.eliminate_zeros();
        if reduction.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(reduction,csr(M.diagonal()[:,None])).astype(bool);

def set_diagonal(matrix,new): #WARNING: new is expected to be sparse csr matrix (as opposed to what is expected in set_new)
    matrix.eliminate_zeros(); new.eliminate_zeros();
    rows, cols         = matrix.nonzero();
    data               = matrix.data;
    old                = rows!=cols;
    rows_old, cols_old = rows[old], cols[old];
    data_old           = data[old];
    rows_cols_new      = new.nonzero()[0];
    data_new           = new.data;
    cols_, rows_       = np.concatenate([cols_old,rows_cols_new],0), np.concatenate([rows_old,rows_cols_new],0);
    data_              = np.concatenate([data_old,data_new],0);
    return csr((data_,(rows_,cols_)),shape=matrix.shape);
#--------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT-------------------------------------------------------------------------------------------------------------------------------------------
_con_minel, _con_repre, _con_feats = sqlite3.connect(_minel_DB), sqlite3.connect(_repre_DB), sqlite3.connect(_feats_DB);
_cur_minel, _cur_repre, _cur_feats = _con_minel.cursor(), _con_repre.cursor(), _con_feats.cursor();
#------------------------------------------------------------------------------------------------------------------------
#-PRINT THE MOST FREQUENT MINELS-----------------------------------------------------------------------------------------
_cur_minel.execute("select minel,freq from (select minel,count(*) as freq from repIDIndex2minel group by minel) order by freq DESC limit 20");
for minel,freq in _cur_minel:
    print('-----------'+str(freq)+'------------');
    features = [row_[0] for row_ in _cur_feats.execute('SELECT feat||":"||featGroup FROM index2feat WHERE featIndex in (SELECT featIndex FROM features WHERE repIDIndex=?)',(minel,)).fetchall()];
    print(','.join(features));
    print('-----------------------------------');
#------------------------------------------------------------------------------------------------------------------------
minel_combs       = dict();
minel2bfdID       = dict();
comb2repIDIndeces = defaultdict(set);
repIDIndex2bfdID  = dict();
non_singletons    = set();
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL REPRESENTATIONS THAT ARE UNDER A MINEL IN THE MAPPING----------------------------------------------------------
_cur_minel.execute("SELECT repIDIndex,GROUP_CONCAT(minel) FROM repIDIndex2minel GROUP BY repIDIndex");

for repIDIndex,minel_str in _cur_minel:
    comb            = ','.join(sorted(minel_str.split(',')));
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,goldID FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0];
    #-----------------------------------------------------------
    #comb_ = []; # This is an experiment where we ignore all minels with only one feature and all reps that have only such minels
    #for minel in comb.split(','): # The ignored ones should be counted as singletons below
    #    minID = _cur_feats.execute("SELECT repID FROM index2repID WHERE repIDIndex=?",(minel,)).fetchall()[0][0];
    #    if minID.count('+++')-minID.count('None')>0:
    #        comb_.append(minel);
    #comb = ','.join(comb_);
    #if len(comb) == 0:
    #    continue;
    #-----------------------------------------------------------
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
    minel_combs[bfdID][comb] += rep_freq;
    for minel in comb.split(','):
        if not minel in minel2bfdID:
            minel2bfdID[minel] = Counter();
        minel2bfdID[minel][bfdID] += rep_freq;
    non_singletons.add(repID);
    comb2repIDIndeces[comb].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE MINELS AS REPRESENTATIONS UNDER THEMSELVES-----------------------------------------------------------------
_cur_minel.execute("SELECT DISTINCT minel FROM repIDIndex2minel");

for row in _cur_minel:
    comb,repIDIndex = str(row[0]),row[0];
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,goldID FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0]; #TODO: Representations have no goldID, only mentions!
    #-----------------------------------------------------------
    # This is an experiment where we ignore all minels with only one feature and all reps that have only such minels
    #if repID.count('+++')-repID.count('None')>0: # The ignored ones should be counted as singletons below
    #    continue;
    #-----------------------------------------------------------
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
    minel_combs[bfdID][comb] += rep_freq;
    for minel in comb.split(','):
        if not minel in minel2bfdID:
            minel2bfdID[minel] = Counter();
        minel2bfdID[minel][bfdID] += rep_freq;
    non_singletons.add(repID);
    comb2repIDIndeces[comb].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE REPRESENTATIONS THAT ARE NEITHER LISTED UNDER A MINEL IN THE MAPPING NOR MINELS THEMSELVES AS SINGLETONS---
_cur_feats.execute("SELECT repID,repIDIndex FROM index2repID");

for repID,repIDIndex in _cur_feats:
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,goldID FROM representations WHERE repID=?",(repID,)).fetchall()[0];
    if not repID in non_singletons:  # minels or supersets already observed
        if not bfdID in minel_combs:
            minel_combs[bfdID] = Counter();
        minel_combs[bfdID][str(repIDIndex)] += rep_freq;
        for minel in comb.split(','):
            if not minel in minel2bfdID:
                minel2bfdID[minel] = Counter();
            minel2bfdID[minel][bfdID] += rep_freq;
        comb2repIDIndeces[str(repIDIndex)].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
print('Done preprocessing.');
#------------------------------------------------------------------------------------------------------------------------
all_combs = Counter();
for bfdID in minel_combs:
    for comb in minel_combs[bfdID]:
        all_combs[comb] += minel_combs[bfdID][comb];

index2comb = list(all_combs.keys());
comb2index = {index2comb[i]:i for i in range(len(index2comb))};

minel2combs = defaultdict(set);
for comb in index2comb:
    for minel in comb.split(','):
        minel2combs[minel].add(comb);

comb2supersets = defaultdict(set);
for comb in comb2index:
    supersets            = set.intersection(*[minel2combs[minel] for minel in comb.split(',')]);
    comb2supersets[comb] = supersets;

freqs     = np.array([all_combs[index2comb[i]] for i in range(len(index2comb))],dtype=int);
edges     = [(comb2index[comb],comb2index[superset],) for comb in comb2supersets for superset in comb2supersets[comb]];
rows,cols = zip(*edges);
size      = max(rows+cols)+1;
sub2sup   = csr((np.ones(len(edges),dtype=bool),(rows,cols,)),shape=(size,size,));
#------------------------------------------------------------------------------------------------------------------------
TPs, Ts, Rs, FNs = 0, 0, 0, dict();

for bfdID in minel_combs:
    combi   = [comb2index[comb] for comb in minel_combs[bfdID]];
    freqs_  = np.array([minel_combs[bfdID][index2comb[i]] for i in range(len(index2comb))],dtype=int); #The freq of this comb in the current bfdID
    reach   = (sub2sup.T[combi,:].dot(sub2sup[:,combi]));
    TP      = reach.multiply(freqs_[combi][:,None]).dot(freqs_[combi][:,None]).sum();
    T       = freqs_[combi].sum()**2;
    R       = TP/T if T >0 else 0;
    TPs    += TP;
    Ts     += T;
    Rs     += R;
    FNs[bfdID] = set([ (index2comb[combi[i]],index2comb[combi[j]],) for i in range(min(_fn_num,len(combi))) for j in range(len(combi)-1,max(-1,len(combi)-(1+_fn_num)),-1) if not reach[i,j] ]);
    print('bfdID:',bfdID,'| combs:',len(combi),'| ments:',freqs_[combi].sum(),'| rec:',int(100*R),'| TP:',TP,'| T:',T);
#------------------------------------------------------------------------------------------------------------------------
print('Overall micro average recall:',int(100*TPs/Ts));
print('Overall macro average recall:',int(100*Rs/len(minel_combs)));
print('TP:',TPs,'T:',Ts);
#------------------------------------------------------------------------------------------------------------------------
Ps, FPs, i = 0, set(), 0;

for combi in [np.array(range(j,min(j+_batch,len(all_combs)))) for j in range(0,len(all_combs),_batch)]:
    reach   = (sub2sup.T[combi,:].dot(sub2sup)); reach.eliminate_zeros();
    P       = reach.multiply(freqs[combi][:,None]).dot(freqs[:,None]).sum();
    Ps     += P;
    #-------------------------------------------------------- #TODO: Make faster!
    num = 0;
    for x,y in zip(*reach.nonzero()):
        comb_x, comb_y             = index2comb[combi[x]], index2comb[y];
        repIDIndex_x, repIDIndex_y = random.choice(list(comb2repIDIndeces[comb_x])), random.choice(list(comb2repIDIndeces[comb_y]));
        bfdID_x, bfdID_y           = repIDIndex2bfdID[repIDIndex_x], repIDIndex2bfdID[repIDIndex_y];
        if bfdID_x != bfdID_y and comb_x != comb_y: # The second part is not required, just to give more interesting examples.
            FPs.add((repIDIndex_x,repIDIndex_y,comb_x,comb_y,bfdID_x,bfdID_y,));
            num += 1;
            if num > _fp_num:
                break;
    #--------------------------------------------------------
    i += 1;
    if i % 10 == 0:
        print(100*_batch*i/len(all_combs),'% (',Ps,')',end='\r');


print('combs:',len(index2comb),'| minels:',len(minel2combs),'| ments:',freqs.sum(),'| rec:',int(100*TPs/Ts),'| pre:',int(100*TPs/Ps),'| TP:',TPs,'| T:',Ts,'| P:',Ps);
#------------------------------------------------------------------------------------------------------------------------
for bfdID in FNs:
    input(bfdID+'...');
    for comb1,comb2 in FNs[bfdID]:
        feats1 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb1.split(',')];
        feats2 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb2.split(',')];
        print('============================================================'+comb1);
        for featlist in feats1:
            print(','.join([attr+':'+val for val,attr in featlist]));
        print('--------------------------VS--------------------------------');
        for featlist in feats2:
            print(','.join([attr+':'+val for val,attr in featlist]));
        print('============================================================'+comb2);
#------------------------------------------------------------------------------------------------------------------------
for repIDIndex1,repIDIndex2,comb1,comb2,bfdID1,bfdID2 in FPs:
    input(comb1+'->'+str(repIDIndex1)+': '+bfdID1+'\n'+comb2+'->'+str(repIDIndex2)+': '+bfdID2+'...');
    feats1 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb1.split(',')];
    feats2 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb2.split(',')];
    print('============================================================'+str(repIDIndex1));
    for featlist in feats1:
        print(','.join([attr+':'+val for val,attr in featlist]));
    print('--------------------------VS--------------------------------');
    for featlist in feats2:
        print(','.join([attr+':'+val for val,attr in featlist]));
    print('============================================================'+str(repIDIndex2));
#------------------------------------------------------------------------------------------------------------------------
