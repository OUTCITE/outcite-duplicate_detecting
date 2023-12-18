import sys
import json
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components
from collections import defaultdict
import sqlite3

_infile          = sys.argv[1]; 
_mentions        = sys.argv[2]; #'/data_ssds/disk01/backests/pubdedup/representations_publications/mentions.db';
_representations = sys.argv[3]; #'/data_ssds/disk'+_disk+'/backests/pubdedup/representations_publications/'+_mapping+'/representations.db';
_features        = sys.argv[4]; #'/data_ssds/disk'+_disk+'/backests/pubdedup/representations_publications/'+_mapping+'/features.db';
_components      = sys.argv[5]; #'/data_ssds/disk'+_disk+'/backests/pubdedup/representations_publications/'+_mapping+'/components.db';
_outDB           = sys.argv[6]; #'/data_ssds/disk'+_disk+'/backests/pubdedup/representations_publications/'+_mapping+'/labelled_mentions.db';

_batch = 1000;


def insert_gold_rows(index2coreID,label2doi,index2label,cur_in,cur_out):
    for database in [(_mentions,'mentions',),(_representations,'representations',),(_features,'features',),(_components,'components',)]:
        cur_in.execute("ATTACH DATABASE ? AS ?",database);
    for start in range(0,len(index2coreID),_batch):
        #input('Press Enter...');
        end                  = min(start+_batch,len(index2coreID)); print(start,end)
        originalIDs          = [str(index2coreID[index]) for index in range(start,end)];
        originalID2goldID    = {originalIDs[index-start]:label2doi[index2label[index]] for index in range(start,end)};
        mentionID_rows       = cur_in.execute("SELECT originalID,mentionID FROM mentions.mentions WHERE originalID IN ("+','.join(originalIDs)+")").fetchall();
        originalID2mentionID = {pair[0]:pair[1] for pair in mentionID_rows};
        mentionIDs           = [str(row[1]) for row in mentionID_rows];
        mentionIDIndex_rows  = cur_in.execute("SELECT mentionID,mentionIDIndex from representations.index2mentionID WHERE mentionID IN ("+','.join(mentionIDs)+")").fetchall();
        mentionID2index      = {pair[0]:pair[1] for pair in mentionIDIndex_rows};
        mentionIDIndices     = [str(row[1]) for row in mentionIDIndex_rows];
        repID_rows           = cur_in.execute("SELECT mentionIDIndex,repID FROM representations.mention2repID WHERE mentionIDIndex IN ("+','.join(mentionIDIndices)+")").fetchall();
        mentionIDIndex2repID = {pair[0]:pair[1] for pair in repID_rows};
        repIDs               = [str(row[1]) for row in repID_rows];
        repIDIndex_rows      = cur_in.execute('SELECT repID,repIDIndex FROM features.index2repID WHERE repID IN ("'+'","'.join(repIDs)+'")').fetchall();
        repID2index          = {pair[0]:pair[1] for pair in repIDIndex_rows};
        repIDIndices         = [str(row[1]) for row in repIDIndex_rows];
        label_rows           = cur_in.execute("SELECT repIDIndex,label FROM components.components WHERE repIDIndex IN ("+','.join(repIDIndices)+")").fetchall();
        repIDIndex2label     = {pair[0]:pair[1] for pair in label_rows};
        labels               = [int(row[1]) for row in label_rows];
        rows                 = [];
        for originalID in originalIDs:
            print('--------------------------------------');
            goldID = originalID2goldID[originalID];
            if not originalID in originalID2mentionID:
                print("No mentionID found for coreID",originalID);
            else:
                mentionID = originalID2mentionID[originalID];
                #print('originalID',originalID,':',mentionID);
                if not mentionID in mentionID2index:
                    print("No mentionIDIndex found for mentionID",mentionID);
                else:
                    mentionIDIndex = mentionID2index[mentionID];
                    #print('mentionID',mentionID,':',mentionIDIndex);
                    if not mentionIDIndex in mentionIDIndex2repID:
                        print("No repID found for mentionIDIndex",mentionIDIndex);
                    else:
                        repID = mentionIDIndex2repID[mentionIDIndex];
                        #print('mentionIDIndex',mentionIDIndex,':',repID);
                        if not repID in repID2index:
                            print("No repIDIndex found for repID",repID);
                        else:
                            repIDIndex = repID2index[repID];
                            #print('repID',repID,':',repIDIndex);
                            if not repIDIndex in repIDIndex2label:
                                print("No label found for repIDIndex",repIDIndex);
                                label              = 1000000000+repIDIndex;
                            else:
                                label = repIDIndex2label[repIDIndex];
                                #print('repIDIndex',repIDIndex,':',label);
                            #yield (mentionID,repID,goldID,label,);
                            rows.append((mentionID,repID,goldID,label,));
        cur_out.executemany("INSERT INTO mentions_core VALUES(?,?,?,?)",rows);
        con_out.commit();


data         = dict();
relation     = set([]);
index2coreID = [];
coreID2index = dict();

IN = open(_infile);
for line in IN:
    obj          = json.loads(line);
    coreID       = int(obj['core_id']);
    data[coreID] = obj;
    relation.add((coreID,coreID,));
    for duplicate_id in obj['labelled_duplicates']:
        dupID = int(duplicate_id);
        relation.add((coreID,dupID,));
    if coreID not in coreID2index:
        coreID2index[coreID] = len(index2coreID);
        index2coreID.append(coreID);
IN.close();

relation       = [(coreID2index[pair[0]],coreID2index[pair[1]],) for pair in relation];
M              = csr((np.ones(len(relation),dtype=bool),([pair[0] for pair in relation],[pair[1] for pair in relation])),dtype=bool);
n, index2label = components(M);

label2indeces = [[] for i in range(len(set(index2label)))];

for i in range(len(index2label)):
    label2indeces[index2label[i]].append(i);

histogram = defaultdict(int);

for length in [len(label2indeces[label]) for label in range(len(label2indeces))]:
    histogram[length] += 1;

larges    = [label for label in range(len(label2indeces)) if len(label2indeces[label]) > 13];# Length-14 (largest) components: [536, 932]
label2doi = [set([data[coreID]['doi'] for coreID in [index2coreID[index] for index in label2indeces[label]]]).pop() for label in range(len(label2indeces))]; # Have checked they are unique

con_in = sqlite3.connect(_outDB);
cur_in = con_in.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

cur_out.execute("DROP TABLE IF EXISTS mentions_core");
cur_out.execute("CREATE TABLE mentions_core(mentionID INT, repID TEXT, goldID TEXT, label INT)");

insert_gold_rows(index2coreID,label2doi,index2label,cur_in,cur_out);

cur_out.execute("CREATE INDEX mentionID_index_core ON mentions_core(mentionID)");
cur_out.execute("CREATE INDEX repID_index_core     ON mentions_core(repID    )");
cur_out.execute("CREATE INDEX goldID_index_core    ON mentions_core(goldID   )");
cur_out.execute("CREATE INDEX label_index_core     ON mentions_core(label    )");

con_in.close();
con_out.close();
