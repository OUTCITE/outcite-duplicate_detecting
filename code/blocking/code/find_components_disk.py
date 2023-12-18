#-IMPORTS-------------------------------------------------------------------------------------------------------------------------------------------
import sqlite3
import sys
import os, psutil
import numpy as np
from collections import defaultdict, Counter
import time
import random
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
from copy import deepcopy as copy
#---------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS-------------------------------------------------------------------------------------------------------------------------------------------

_inDB         = sys.argv[1];
_reportDB     = sys.argv[2];
_componentsDB = sys.argv[3];

_workers   = 8;
_batchsize = 100000;
_patchsize = 50000000;

_minel_multi_thr = 1000;

_process = psutil.Process(os.getpid());
#---------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-----------------------------------------------------------------------------------------------------------------------------------------

def get_featsOf(cur,start=None,end=None):
    if start == None:
        #cur.execute("SELECT * FROM features ORDER BY repIDIndex DESC"); #TODO: Why is this DESC?? I can only assume that I forgot to change this? But it is not used at the moment.
        print('start==None.')
        cur.execute("SELECT * FROM features");
        rows = sorted([(int(repIDIndex),int(featIndex),) for repIDIndex,featIndex in cur]);
    else:
        cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,)); #TODO: Why do I order in the DB instead of afterwards? This is actually faster.
        #cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ?",(start,end,));
        #rows = sorted([(int(repIDIndex),int(featIndex),) for repIDIndex,featIndex in cur]);
    first   = cur.fetchone();#rows[0];#
    featsOf = [[first[1]]];
    current = first[0];
    for repIDIndex, featIndex in cur:#rows[1:]:#
        if repIDIndex != current:
            current     = repIDIndex;
            featsOf[-1] = np.array(featsOf[-1]);
            featsOf.append([]);
        featsOf[-1].append(featIndex);
    featsOf[-1] = np.array(featsOf[-1]);
    featsOf = np.array(featsOf,dtype=object);
    return featsOf;

def get_structOf(cur,start,end):
    print('STRUCT: Building help structure for repIDIndices between',start,'and',end); t = time.time();
    struct = defaultdict(set);
    cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,)); #TODO: Why do I order in the DB instead of afterwards? This is actually faster.
    #cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ?",(start,end,));
    #rows = sorted([(int(repIDIndex),int(featIndex),) for repIDIndex,featIndex in cur]);
    print('SELECT: Took',time.time()-t,'seconds.');
    for repIDIndex, featIndex in cur:#rows:#
        struct[featIndex].add(repIDIndex);
    print('STRUCT: Took',time.time()-t,'seconds.');
    return struct;

def put(value,queue,sleeptime=0.1,max_trytime=1):
    start_time = time.time();
    try_time   = 0;
    while True:
        try:
            queue.put(value,block=False);
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                return 1;
            time.sleep(sleeptime);

def get(queue,sleeptime=0.02,max_trytime=0.1):
    start_time = time.time();
    try_time   = 0;
    value      = None;
    while True:
        try:
            value = queue.get(block=False);
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                break;
            time.sleep(sleeptime);
    return value;

def find_subsup(start,end,search_size):
    t         = time.time();
    con       = sqlite3.connect(_inDB);
    cur       = con.cursor();
    C         = get_featsOf(cur,start,end); con.close(); # Because of the appending, this assumes that there are no gaps in the repIDIndex!
    subsets   = [];                                      # However it seems that there are indeed no gaps.
    supersets = [];
    for index1 in range(start,end+1): # What is the reason for the +1? I guess because the input range assume inclusive interval
        if len(C[index1-start]) >= search_size:
            break;
        feats      = [(feat,freq,) for freq,feat in sorted([(len(S[feat]),feat,) for feat in C[index1-start]])];
        if feats[0][1] == 0:
            continue; # If the rarest feature does not occur in the index, then there is no superset, unnecessary if the below is implemented as explicit loop with breaks
        overlaps   = (S[feat] if feat in S else set() for feat,freq in feats);
        supersets_ = set.intersection(*[overlap for overlap in overlaps]); #Could write this explicitely going through the feat-sets and breaking when having found as many supersets as frequency of first in struct
        if len(supersets_) > 0:
            subsets.append(index1);
            supersets.append(supersets_);
    print('SUBSUP: Found',len(subsets),' subsets from '+str(start)+' to '+str(end)+' for superset-size '+str(search_size)+' in ',round(time.time()-t,2),'s',end='\r');
    return subsets, supersets;

def work(Q,R):
    while True:
        job = get(Q);
        if job != None:
            start1, end1, size = job;
            subsets, supersets = find_subsup(start1,end1,size);
            put((subsets,supersets,),R);
        else:
            break;

def size_boundaries(cur):
    cur.execute("SELECT freq, COUNT(*) FROM (SELECT COUNT(*) AS freq FROM features GROUP BY repIDIndex) GROUP BY freq");
    size_freq  = {size:freq for size,freq in cur};
    sizes      = sorted(list(size_freq.keys()));
    boundaries = [[None,None,] for i in range(max(sizes)+1)];
    acc = 0;
    for size in sizes:
        boundaries[size][0] = acc;
        acc                += size_freq[size];
        boundaries[size][1] = acc-1;
    return sizes,boundaries;

def queue2list(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

def join(workers):
    to_join = set(range(len(workers)));
    while len(to_join) > 0:
        i = random.sample(to_join,1)[0];
        workers[i].join(0.1);
        if not workers[i].is_alive():
            to_join.remove(i); print(len(to_join),'workers left to join.',end='\r');
        else:
            time.sleep(0.2);

def start(workers,batches,Q,size):
    for batch_start,batch_end in batches:
        put((batch_start,batch_end,size,),Q);
    for worker in workers:
        worker.start();

def update_results(supersets,min_el_map,R):
    results               = queue2list(R);
    subsetss, supersetsss = zip(*results) if len(results) > 0 else ([],[]);
    current_subsets       = set([subset   for subsets    in subsetss    for subset     in subsets]);
    current_supersets     = set([superset for supersetss in supersetsss for supersets_ in supersetss for superset in supersets_]);
    #current_supersets     = set().union(*[supersets_ for supersetss in supersetsss for supersets_ in supersetss]); # Should be same as above
    supersets            |= current_supersets;
    current_min_els       = current_subsets - supersets;
    for subsets_, supersets_ in results:
        for i in range(len(subsets_)):
            if subsets_[i] in current_min_els:
                min_el_map[subsets_[i]] |= supersets_[i];
    return supersets, min_el_map;

def make_tasks(batchsize,patchsize,sizes,boundaries):
    tasks = [];
    for k in range(1,len(sizes)):
        sub_start, sub_end = boundaries[sizes[0]][0], boundaries[sizes[k-1]][1];
        sup_start, sup_end = boundaries[sizes[k]][0], boundaries[sizes[k  ]][1];
        batches            = [(i,min(i+batchsize,sub_end)) for i in range(sub_start,sub_end,batchsize)];
        patches            = [(i,min(i+patchsize,sup_end)) for i in range(sup_start,sup_end,patchsize)];
        tasks             += [(patch,batches,sizes[k],) for patch in patches];
    return tasks;

def get_min_el_map(tasks,cur,sizes,boundaries,num_workers=16): #Assuming that sizes is smallest first!
    global S, cur_out, con_out; #additional values for row: (1) size of S (2) process memory
    min_el_map, supersets, manager = defaultdict(set), set(), MP.Manager();
    patch, batches, size           = tasks[0];
    S                              = get_structOf(cur,patch[0],patch[1]);
    for i in range(1,len(tasks)+1): #TODO: Improve the granularity of what objects use how much of ram, in particular the results!
        row     = [size,patch[0],patch[1],round(sys.getsizeof(S)/1024**2),round(_process.memory_info().rss/1024**2),time.time()];
        Q, R    = manager.Queue(), manager.Queue();
        workers = [MP.Process(target=work,args=(Q,R,)) for x in range(num_workers)];            row += [(time.time()-row[5])];
        start(workers,batches,Q,size);                                                          row += [(time.time()-row[5])];
        join(workers);                                                                          row += [(time.time()-row[5])];
        supersets, min_el_map = update_results(supersets,min_el_map,R);                         row += [(time.time()-row[5])];
        del S;
        patch, batches, size = tasks[i]                            if i < len(tasks) else (None,None,None);
        S                    = get_structOf(cur,patch[0],patch[1]) if i < len(tasks) else None; row += [(time.time()-row[5])];
        cur_out.execute("INSERT INTO processing VALUES("+','.join(('?' for val in row))+")",row); con_out.commit();
    return min_el_map, supersets;

#-SCRIPT--------------------------------------------------------------------------------------------------------------------------------------------

#-PREPARE INPUT AND OUTPUT--------------------------------------------------------------------------------------------------------
con     = sqlite3.connect(_inDB);
cur     = con.cursor();
con_out = sqlite3.connect(_reportDB);
cur_out = con_out.cursor();
cur_out.execute("DROP   TABLE IF EXISTS processing");
cur_out.execute("CREATE TABLE processing(superset_size INT, patch_start INT, patch_end INT, MB_S INT, MB_all INT, t0 REAL, t1 REAL, t2 REAL, t3 REAL, t4 REAL, t5 REAL)");
#---------------------------------------------------------------------------------------------------------------------------------

#-MAKE THE TASK BATCHES AND PATCHES-----------------------------------------------------------------------------------------------
sizes, boundaries     = size_boundaries(cur);
tasks                 = make_tasks(_batchsize,_patchsize,sizes,boundaries);
#---------------------------------------------------------------------------------------------------------------------------------

#-BULDING THE REDUCED GRAPH INSTEAD OF THE SUBSET PARTIAL ORDER BY RELATING MINIMAL ELEMENTS AND THEIR SUPERSETS------------------
min_el_map, supersets = get_min_el_map(tasks,cur,sizes,boundaries,_workers); #TODO: Are these supersets or >proper< supersets?
#---------------------------------------------------------------------------------------------------------------------------------

#-FINDING FOR EACH MINEL THE NUMBER OF MULTI-MINEL SPECIFICATIONS-----------------------------------------------------------------
num_minels = Counter();
for minel in min_el_map:
    for superset in min_el_map[minel]:
        num_minels[superset] += 1;
num_any_minels = Counter();
for minel in min_el_map:
    num_any = sum([num_minels[superset]>0 for superset in min_el_map[minel]]);
    if num_any > 0:
        num_any_minels[minel] = num_any;
num_multi_minels = Counter();
for minel in min_el_map:
    num_multi = sum([num_minels[superset]>1 for superset in min_el_map[minel]]);
    if num_multi > 0:
        num_multi_minels[minel] = num_multi;
num_single_minels = Counter();
for minel in min_el_map:
    num_single = sum([num_minels[superset]==1 for superset in min_el_map[minel]]);
    if num_single > 0:
        num_single_minels[minel] = num_single;
prob_multi_minels = Counter();
for minel in min_el_map:
    num_any   = sum([num_minels[superset]>0 for superset in min_el_map[minel]]);
    num_multi = sum([num_minels[superset]>1 for superset in min_el_map[minel]]);
    prob      = num_multi/num_any;
    if prob > 0:
        prob_multi_minels[minel] = prob;
#---------------------------------------------------------------------------------------------------------------------------------
print('any   :', sorted([(num_any_minels[minel],prob_multi_minels[minel],minel,)   for minel in num_any_minels  ],reverse=True)[:30]);
print('multi :', sorted([(num_multi_minels[minel],prob_multi_minels[minel],minel,) for minel in num_multi_minels],reverse=True)[:30]);
print('single:', sorted([(num_single_minels[minel],prob_multi_minels[minel],minel,) for minel in num_single_minels],reverse=True)[:30]);
#input('Enter to continue...');
#-REMOVING EDGES THAT ARE OVER-CONNECTING-----------------------------------------------------------------------------------------
'''
min_el_map_original = copy(min_el_map);
for minel in num_multi_minels:
    if num_multi_minels[minel] > _minel_multi_thr:
        for superset in min_el_map_original[minel]:
            if num_minels[superset] > 1:
                min_el_map[minel].remove(superset); # Cut the connection between overly connected minels and their 'ambiguous' supersets
        if len(min_el_map[minel]) == 0:
            del min_el_map[minel];
'''
#---------------------------------------------------------------------------------------------------------------------------------

#-FINDING THE CONNECTED COMPONENTS IN THE REDUCED GRAPH---------------------------------------------------------------------------
rows, cols    = zip(*[(fro,to,) for fro in min_el_map for to in min_el_map[fro]]);
max_index     = boundaries[-1][1];#max(max(min_el_map.keys()),max(supersets));
min_el_matrix = csr((np.ones(len(rows),dtype=bool),(rows,cols,)), dtype=bool, shape=(max_index+1,max_index+1,) );
num_c, labels = connected_components(min_el_matrix,directed=False);
components    = [set() for i in range(num_c)];
for repIndex in range(len(labels)):
    components[labels[repIndex]].add(repIndex);
#singletons    = set(range(len(labels)))-(set(min_el_map.keys())|set((repIndex for minel in min_el_map for repIndex in min_el_map[minel]))); #TODO: Comment in to change
#components = [component for component in components if len(component) > 1]; # Dropping the singletons
#---------------------------------------------------------------------------------------------------------------------------------

#-OUTPUT THE RESULTS--------------------------------------------------------------------------------------------------------------
con_out = sqlite3.connect(_componentsDB);
cur_out = con_out.cursor();
cur_out.execute("DROP TABLE IF EXISTS components");
cur_out.execute("DROP INDEX IF EXISTS label_index");
cur_out.execute("CREATE TABLE components(label INT, repIDIndex INT)");
cur_out.executemany("INSERT INTO components VALUES(?,?)",((label,repIndex,) for label in range(len(components)) for repIndex in components[label] if len(components[label]) > 0)); #TODO: Can be changed
cur_out.execute("CREATE INDEX label_index on components(label)");
cur_out.execute("CREATE INDEX repIDIndex_index on components(repIDIndex)");
con_out.commit();

cur_out.execute("DROP TABLE IF EXISTS repIDIndex2minel");
cur_out.execute("DROP INDEX IF EXISTS repIDIndex2minel_repIDIndex_index");
cur_out.execute("DROP INDEX IF EXISTS repIDIndex2minel_minel_index");
cur_out.execute("CREATE TABLE repIDIndex2minel(repIDIndex INT, minel INT)");
cur_out.executemany("INSERT INTO repIDIndex2minel VALUES(?,?)",((repIDIndex,minel,) for minel in min_el_map for repIDIndex in min_el_map[minel])); #TODO: Can be changed |set([minel]
#cur_out.executemany("INSERT INTO repIDIndex2minel VALUES(?,?)",((repIDIndex,repIDIndex,) for repIDIndex in singletons)); #TODO: Comment in to change
cur_out.execute("CREATE INDEX repIDIndex2minel_repIDIndex_index on repIDIndex2minel(repIDIndex)");
cur_out.execute("CREATE INDEX repIDIndex2minel_minel_index on repIDIndex2minel(minel)");
con_out.commit();

cur_out.execute("DROP TABLE IF EXISTS minel2label");
cur_out.execute("DROP INDEX IF EXISTS minel2label_label_index");
cur_out.execute("CREATE TABLE minel2label(minel INTEGER PRIMARY KEY, label INT)");
cur_out.executemany("INSERT INTO minel2label VALUES(?,?)",((minel,int(labels[next(iter(min_el_map[minel]))]),) for minel in min_el_map));
cur_out.execute("CREATE INDEX minel2label_label_index on minel2label(label)");
con_out.commit();

cur_out.execute("DROP TABLE IF EXISTS repIDIndex2numinels");
cur_out.execute("CREATE TABLE repIDIndex2numinels(repIDIndex INTEGER PRIMARY KEY, numinels INT)");
cur_out.executemany("INSERT INTO repIDIndex2numinels VALUES(?,?)",((superset,num_minels[superset],) for superset in num_minels));
con_out.commit();

cur_out.execute("DROP TABLE IF EXISTS minel2crossfreq");
cur_out.execute("CREATE TABLE minel2crossfreq(minel INTEGER PRIMARY KEY, freq INT)");
cur_out.executemany("INSERT INTO minel2crossfreq VALUES(?,?)",((minel,num_multi_minels[minel],) for minel in num_multi_minels));
con_out.commit();

con_out.close();
#---------------------------------------------------------------------------------------------------------------------------------
#cfreq = np.array([(i,len(components[i])) for i in range(len(components))]);
#csort = np.argsort(cfreq[:,1]);
#mfreq = np.array([(min_el,len(min_el_map[min_el])) for min_el in min_el_map]);
#msort = np.argsort(mfreq[:,1]);
#------------------------------------------------------------
#min_els     = set([min_el for min_el in min_el_map if len(min_el_map[min_el])>0]);
#min_reduce  = [(cfreq[csort[i]][1],set([mention2repIndex[mentionIndex] for mentionIndex in components[csort[i]]])&min_els,csort[i]) for i in range(len(csort))];
#min_reduce_ = [(f1,len(f2),f3,) for f1,f2,f3 in min_reduce if len(f2)>0];
#min_reduced = [(f1,f2,f3,) for f1,f2,f3 in min_reduce if len(f2)>0];
#------------------------------------------------------------
#OUT = open('component_minels_disk.txt','w');
#for compsize,minels,compindex in min_reduced:
#    OUT.write(' '.join([str(next(iter(rep2mentionIndex[repIndex]))) for repIndex in minels])+'\n');
#OUT.close();
#------------------------------------------------------------
