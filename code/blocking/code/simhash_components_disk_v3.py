#-IMPORTS-------------------------------------------------------------------------------------------------------------------------------------------
import sqlite3
import sys
import os, psutil
import numpy as np
from collections import defaultdict, Counter
import time
import random
import gmpy2
import itertools
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
from scipy.sparse import vstack as csr_vstack
from scipy.sparse import hstack as csr_hstack
from copy import deepcopy as copy
from functools import partial
import asciidammit as dammit
#---------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS-------------------------------------------------------------------------------------------------------------------------------------------

_inDB         = sys.argv[1];
_reportDB     = sys.argv[2]; # Not used
_componentsDB = sys.argv[3];

#_workers   = 8;
_batchsize = 500000;
_patchsize = 1;

_num_workers   = 64;
_tasks_per_job = 2;

_k = 7;

#---------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-----------------------------------------------------------------------------------------------------------------------------------------

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

def queue2list(Q,sleeptime=0.02,max_trytime=1):
    L = [];
    while True:
        #print(Q.qsize(),'left to pop...');
        element = get(Q,sleeptime,max_trytime);
        if element == None:
            break;
        L.append(element);
    return L;

def join(workers):
    to_join = set(range(len(workers)));
    print(len(to_join),'workers left to join.',end='\r');
    while len(to_join) > 0:
        i = random.sample(to_join,1)[0];
        workers[i].join(0.1);
        if not workers[i].is_alive():
            to_join.remove(i); print(len(to_join),'workers left to join.',end='\r');
        else:
            time.sleep(0.2);

def start(workers,jobs,Q):
    for job in jobs:
        put(job,Q);
    for worker in workers:
        worker.start();

def get_featsOf(cur,start=None,end=None):
    if start == None:
        print('start==None.')
        cur.execute("SELECT * FROM features");
        rows = sorted([(int(repIDIndex),int(featIndex),) for repIDIndex,featIndex in cur]);
    else:
        cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,));
    first   = cur.fetchone();
    featsOf = [[first[1]]];
    current = first[0];
    for repIDIndex, featIndex in cur:
        if repIDIndex != current:
            current     = repIDIndex;
            featsOf[-1] = np.array(featsOf[-1]);
            featsOf.append([]);
        featsOf[-1].append(featIndex);
    featsOf[-1] = np.array(featsOf[-1]);
    featsOf     = np.array(featsOf,dtype=object);
    return featsOf;

def make_matrix(featlists,d):
    rows = [featIndex for featlist in featlists             for featIndex in featlist];
    cols = [i         for i        in range(len(featlists)) for j         in range(len(featlists[i]))];
    data = np.ones(len(rows),dtype=bool);
    D    = csr((data,(rows,cols)),dtype=bool,shape=(d,len(featlists),));
    return D;

def randomvector(d,ignore,p):
    rng        = np.random.default_rng(seed=p);
    components = rng.standard_normal(d);
    r          = np.sqrt(np.square(components).sum());
    v          = components / r;
    v[ignore]  = 0;
    v          = csr(v,shape=(1,d));
    return v;

def bits2int(bits):
  m,n = bits.shape;
  a   = 2**np.arange(n,dtype=np.uint64)[::-1];
  return bits @ a;

def make_tasks(batchsize,n):
    tasks = [(i,min(i+batchsize-1, n  ),) for i in range(0, n,batchsize)]; # sqlite range including the last integer
    return tasks;

def get_hashes(tasks,d,cur): #Assuming that sizes is smallest first!
    for batch in tasks:
        featlists = get_featsOf(cur,batch[0],batch[1]);
        featstrss = get_featStr(featlists,cur);
        integers  = np.array([feats2docint(featstrs) for featstrs in featstrss],dtype=np.uint64);
        yield integers;

def work(Q,R,cur,d):
    tasks = get(Q);
    while tasks != None:
        first_repIDIndex = tasks[0][0]; # First task -> start
        #-------------------------------------------------------------------------------------------------------------------------------------
        labels = np.array([],dtype=np.uint64);
        for new_labels in get_hashes(tasks,d,cur):
            labels = np.concatenate((labels,new_labels));
        #-------------------------------------------------------------------------------------------------------------------------------------
        put((first_repIDIndex,labels.copy(),),R);
        print('Put',len(labels),'labels into results queue. Size:',R.qsize());
        tasks = get(Q);

def stream_bits(strings): #TODO: Why does asciiDammit not produce an ascii string?
    stream = [np.frombuffer(string.encode(),dtype=np.uint8) for string in strings]; #bytes(dammit.asciiDammit(string),'ascii')
    maxlen = max((len(L) for L in stream));
    for i in range(maxlen):
        indices = [];
        barray  = [];
        for j in range(len(stream)):
            if i < len(stream[j]):
                indices.append(j);
                barray.append(stream[j][i]);
        yield np.array(indices), np.array(barray);

def fnv1a(features): # This seems to be correct, although the similarity of similar features is not reflected
    p = np.array([1099511628211],dtype=np.uint64); # This is an array to avoid overflow checks and warning
    h = np.repeat(14695981039346656037,len(features));
    for indices,Bytes in stream_bits(features):
        h[indices] ^= Bytes;#np.left_shift(np.right_shift(h[indices],8),8) + Bytes;
        h[indices] *= p;
    return h;

def dif(hashes,i,j):
    return gmpy2.popcount(int(hashes[i]^hashes[j]));

def feats2docint(features):
    hashes   = fnv1a(features);
    booles   = np.unpackbits(hashes.view(np.uint8),bitorder='little').reshape(len(hashes),64)[:,::-1];
    avg_bool = booles.sum(0)/len(booles)>=0.5;
    avg_int  = np.packbits(avg_bool.reshape(8, 8)[::-1]).view(np.uint64)[0];
    return avg_int;

def get_featStr(featlists,cur):
    return [[row[0] for row in cur.execute('SELECT featGroup||":"||feat FROM index2feat WHERE featIndex IN ('+','.join([str(el) for el in featlist])+')').fetchall()] for featlist in featlists];

def get_feats(repIDIndex,cur):
    return cur.execute('SELECT featGroup||":"||feat FROM index2feat WHERE featIndex IN (SELECT featIndex FROM features WHERE repIDIndex='+str(repIDIndex)+')').fetchall();

#---------------------------------------------------------------------------------------------------------------------------------

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

def transitive_closure(M):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    closure   = edges.copy();
    num, i    = 1,2;
    while num > 0:
        #print('...',i,':',num);
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        closure    = closure + new;
        i         += 1;
        closure.eliminate_zeros();
        if closure.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(closure,csr(M.diagonal()[:,None])).astype(bool);

def get_closure(tups):
    index2id = list(set([tup[0] for tup in tups]) | set([tup[1] for tup in tups]));
    id2index = {index2id[i]:i for i in range(len(index2id))};
    tups_re  = tups #+ [(index2id[i],index2id[i],) for i in range(len(index2id))]; # Unfortunately you have to make the relation reflexive first - you could also add the diagonal to M
    M        = csr( ([True for tup in tups_re],([id2index[tup[0]] for tup in tups_re],[id2index[tup[1]] for tup in tups_re])),shape=(len(index2id),len(index2id)),dtype=bool);
    M_       = transitive_closure(M); # n is maximum path length of your relation
    temp     = M_.nonzero();
    return [(index2id[temp[0][i]],index2id[temp[1][i]],) for i in range(len(temp[0]))];

def make_blockints_(bitstrings):
    integerss  = np.array([split_interpret(bitstring) for bitstring in bitstrings],dtype=np.uint8);
    return integerss;

def split_interpret(bitstring): # assume 64 / 8
    parts = [bitstring[i:i+8] for i in range(0,64,8)];
    ints  = [int(part,2) for part in parts];
    return ints;

def make_blockints_(integers):
    integerss  = np.array([split_interpret(make_bitstring(integer)) for integer in integers],dtype=np.uint8);
    return integerss;

def make_blockints(integers):
    return np.packbits(np.unpackbits(integers.view(np.uint8)))[::-1].reshape(len(integers),8)[::-1];

def make_bitstrings(integers):
    bitstrings = np.vectorize(np.binary_repr)(integers,64);
    return bitstrings;

def make_bitstring(integer):
    bitstring = np.binary_repr(integer,64);
    return bitstring;

def simhash(ordering,T,N,k):
    times = []; t = time.process_time();
    #print('-----------------------------');
    T_        = T[:,ordering]; times.append(time.process_time()-t); t = time.process_time();
    sorting   = np.lexsort(T_[:,::-1].T[:8-k]); times.append(time.process_time()-t); t = time.process_time(); # This takes the longest, around 1 sec for 1M rows
    T__       = T[sorting,:]; times.append(time.process_time()-t); t = time.process_time();
    equals    = np.where((T__[:-1,:8-k] == T__[1:,:8-k]).sum(axis=1)==8-k)[0]; times.append(time.process_time()-t); t = time.process_time();
    equivs    = [(sorting[index],sorting[index+1],) for index in equals]; times.append(time.process_time()-t); t = time.process_time(); # Tuples of indices in the unsorted table which have identical prefix for this ordering
    equivs    = get_closure(equivs); times.append(time.process_time()-t); t = time.process_time();
    #print(len(equivs),'satisfying necessary condition for this table');
    checked   = [(i1,i2,) for i1,i2 in equivs if gmpy2.popcount(int(N[i1]^N[i2])) <= k]; times.append(time.process_time()-t); t = time.process_time();
    #print(checked); print(times);
    return set(checked);

def find_similar(N,k):
    orderings = itertools.combinations(range(8),8-k);
    orderings = [list(ordering)+[index for index in range(8) if not index in ordering] for ordering in orderings];
    T         = make_blockints(N);
    with MP.Pool(56) as pool:
        similarss = pool.map(partial(simhash,T=T,N=N,k=k),orderings);
    print(len(similarss));
    similars  = set().union(*similarss);
    return similars;

#-SCRIPT--------------------------------------------------------------------------------------------------------------------------------------------

#-PREPARE INPUT AND OUTPUT--------------------------------------------------------------------------------------------------------
con = sqlite3.connect(_inDB);
cur = con.cursor();
#---------------------------------------------------------------------------------------------------------------------------------

#-MAKE THE TASK BATCHES AND PATCHES-----------------------------------------------------------------------------------------------
n     = cur.execute("SELECT MAX(repIDIndex) FROM index2repID").fetchall()[0][0]+1; # Since 0 is an index as well, we have to add one
d     = cur.execute("SELECT MAX(featIndex)  FROM index2feat" ).fetchall()[0][0]+1; # Since 0 is an index as well, we have to add one
tasks = make_tasks(_batchsize,n);
jobs  = [tasks[i:min(i+_tasks_per_job,len(tasks))] for i in range(0,len(tasks),_tasks_per_job)];
#---------------------------------------------------------------------------------------------------------------------------------

#-COMPUTING THE SIMHASHES---------------------------------------------------------------------------------------------------------
print(n,'representations to hash...', len(jobs),'jobs to work...');

cons = [sqlite3.connect(_inDB) for x in range(_num_workers)];
curs = [cons[x].cursor()       for x in range(_num_workers)];

manager = MP.Manager();
Q, R    = manager.Queue(), manager.Queue();
workers = [MP.Process(target=work,args=(Q,R,curs[x],d,)) for x in range(_num_workers)];
start(workers,jobs,Q);
time.sleep(60);
join(workers);

for i in range(len(cons)):
    cons[i].close();
    print(i,'connections left to close.',end='\r');
#---------------------------------------------------------------------------------------------------------------------------------

#-FINDING THE K-SIMILAR HASHES----------------------------------------------------------------------------------------------------
print('Getting results from queue...');
results = queue2list(R);
print('Done getting results from queue.');

length = sum([len(labels) for offset,labels in results]);

labelling = np.zeros(length,dtype=np.uint64);
for offset,labels in results:
    labelling[np.arange(len(labels))+offset] = labels;

print('Starting k-similar simhashing...');
similars = find_similar(labelling,_k);
#---------------------------------------------------------------------------------------------------------------------------------

#-FINDING THE CONNECTED COMPONENTS IN THE SIMILAR HASHES--------------------------------------------------------------------------
rows, cols    = zip(*similars);
sim_matrix    = csr((np.ones(len(rows),dtype=bool),(rows,cols,)), dtype=bool, shape=(length,length,) );
num_c, labels = connected_components(sim_matrix,directed=False);
components    = [set() for i in range(num_c)];
for repIndex in range(len(labels)):
    components[labels[repIndex]].add(repIndex);
#---------------------------------------------------------------------------------------------------------------------------------

#-OUTPUT THE RESULTS--------------------------------------------------------------------------------------------------------------
con.close();
print('Writing out...');
con_out = sqlite3.connect(_componentsDB);
cur_out = con_out.cursor();
cur_out.execute("DROP TABLE IF EXISTS components");
cur_out.execute("DROP INDEX IF EXISTS label_index");
cur_out.execute("CREATE TABLE components(label INT, repIDIndex INT)");
cur_out.executemany("INSERT INTO components VALUES(?,?)",((label,repIndex,) for label in range(len(components)) for repIndex in components[label] if len(components[label]) > 0)); #TODO: Can be changed
cur_out.execute("CREATE INDEX label_index on components(label)");
cur_out.execute("CREATE INDEX repIDIndex_index on components(repIDIndex)");
con_out.commit();
con_out.close();
print('Done writing out.');
