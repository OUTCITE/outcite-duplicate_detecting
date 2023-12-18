#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import sqlite3
import json
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from common import *
from pathlib import Path
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS-----------------------------------------------------------------------------------------------------------------------------------------

# THE DATABASE WHICH CONTAINES THE UNPROCESSED FEATURES
_indb  = sys.argv[1];
# THE DATABASE WHERE THE PROCESSED FEATURES ARE WRITTEN TO
_outdb = sys.argv[2];

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# THE TARGET MATCHES TO BE USED AS GOOD (SILVER) IDENTIFIERS
_id_fields = _configs['targets'];
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

# CURSOR TO DATABASE WITH UNPROCESSED FEATURES
con = sqlite3.connect(_indb);
cur = con.cursor();

# GETTING ROWS
print("Getting rows...");
rows = cur.execute("SELECT linkID,"+','.join([id_field+'ID' for id_field in _id_fields])+" FROM refmetas WHERE "+' OR '.join([id_field+'ID IS NOT NULL' for id_field in _id_fields])).fetchall();
rows = [[el for el in row if el] for row in rows];
con.close();

# INDEXING IDS
print("Indexing IDs...");
id2index = dict();
index2id = [];
for row in rows:
    for el in row:
        if not el in id2index:
            id2index[el] = len(index2id);
            index2id.append(el);

# CREATING SPARSE MATRIX
print("Creating sparse matrix...");
pairs      = [(id2index[row[0]],id2index[el],) for row in rows for el in row[1:]];
rows, cols = zip(*pairs);
L = csr((np.ones(len(rows)),(rows,cols)),shape=(len(id2index),len(id2index)),dtype=bool);

# DETECTING COMPONENTS
print("Detecting components...");
n, labels = components(L, directed=False);

# CREATING LABEL TO INDEX MAPPING
print("Creating label to index mapping...");
label2indices = [];
for i in range(len(labels)):
    if labels[i] >= len(label2indices):
        label2indices.append([]);
    label2indices[labels[i]].append(i);

# STORING LABELS
print("Storing labels...");
con = sqlite3.connect(_outdb);
cur = con.cursor();
cur.executemany("UPDATE mentions SET goldID=? WHERE originalID=?",((l,index2id[i],) for l in range(len(label2indices)) for i in label2indices[l]));
con.commit();
con.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
