import sys
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from common import *

_indb  = sys.argv[1]; # The refmetas DB
_outdb = sys.argv[2]; # The features DB

con = sqlite3.connect(_indb);
cur = con.cursor();

_id_fields = ['sowiport','crossref','dnb','openalex','arxiv','ssoar','research_data','gesis_bib'];

print("Getting rows...");
rows = cur.execute("SELECT linkID,"+','.join([id_field+'ID' for id_field in _id_fields])+" FROM refmetas WHERE "+' OR '.join([id_field+'ID IS NOT NULL' for id_field in _id_fields])).fetchall();
rows = [[el for el in row if el] for row in rows];
con.close();

print("Indexing IDs...");
id2index = dict();
index2id = [];
for row in rows:
    for el in row:
        if not el in id2index:
            id2index[el] = len(index2id);
            index2id.append(el);

print("Creating sparse matrix...");
pairs      = [(id2index[row[0]],id2index[el],) for row in rows for el in row[1:]];
rows, cols = zip(*pairs);
L = csr((np.ones(len(rows)),(rows,cols)),shape=(len(id2index),len(id2index)),dtype=bool);

print("Detecting components...");
n, labels = components(L, directed=False);

print("Creating label to index mapping...");
label2indices = [];
for i in range(len(labels)):
    if labels[i] >= len(label2indices):
        label2indices.append([]);
    label2indices[labels[i]].append(i);

print("Storing labels...");
con = sqlite3.connect(_outdb);
cur = con.cursor();
cur.executemany("UPDATE mentions SET goldID=? WHERE originalID=?",((l,index2id[i],) for l in range(len(label2indices)) for i in label2indices[l]));
con.commit();
con.close();
