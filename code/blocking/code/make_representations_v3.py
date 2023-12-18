import sqlite3
import sys
import time
import hashlib
from tabulate import tabulate

_indb      = sys.argv[1];
_outdb     = sys.argv[2];
_types     = sys.argv[3];

_batchsize = 1000000;

_max_complen = 4;

con_in  = sqlite3.connect(_indb);
cur_in  = con_in.cursor();

_target_fields = dict();
TYPES   = open(_types);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _target_fields[feature] = rest.split(' ');
TYPES.close();
_featOf        = {field:feature for feature in _target_fields for field in _target_fields[feature]};
_columns       = set(_featOf.keys());
_features      = [feature for field in _target_fields for feature in _target_fields[field]];
_feat2index    = {field:index for index,field,_  ,_,_,_ in cur_in.execute("PRAGMA table_info(mentions)").fetchall()};
_feat2type     = {field:typ   for _    ,field,typ,_,_,_ in cur_in.execute("PRAGMA table_info(mentions)").fetchall()};
#_selection     = sorted([_feat2index[feature] for field in _target_fields for feature in _target_fields[field]]); #TODO: The alphbetical order of the features contradicts the order in _features
print(_features); print(_feat2index);
_selection     = [_feat2index[feature] for feature in _features];
_columns       = ['repID','freq']+_features;
_types         = ['TEXT PRIMARY KEY','INT']+[_feat2type[feature] for feature in _features];
_questionmarks = ','.join(['?' for i in range(len(_columns))]);
_featstring    = ','.join(_features);
_groups        = [[_feat2index[feature] for feature in _target_fields[field]] for field in _target_fields];

def bundle(row): #So what does this do after all? I think sorting and putting the none at the end to get from sets to sequences for hashing!
    row_   = [cell for cell in row];
    for group in _groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def make_repID(row):
    #return '+++'.join((str(row_r[x]) for x in _selection));
    return hashlib.sha1('#+*'.join([str(x)+'*+'+str(row[x]) for x in _selection]).encode("utf-8")).hexdigest();

def mentions2representations(rows_m):
    rows_r = [bundle(row_m) for row_m in rows_m];
    repIDs = [make_repID(row_r) for row_r in rows_r];#['+++'.join((str(row_r[x]) for x in _selectedfield)) for row_r in rows_r];
    freqs  = [row_r[3] for row_r in rows_r];
    rows_r = [tuple([repIDs[i],freqs[i]]+[rows_m[i][x] for x in _selection]) for i in range(len(rows_m))];
    return rows_r;

def initialize_primary_keys(cur_in,cur_out,con_out):
    mention        = cur_in.execute("SELECT * FROM mentions ORDER BY mentionID LIMIT 1").fetchall()[0];
    representation = mentions2representations([mention])[0];
    cur_out.execute("INSERT INTO mention2repID   VALUES(?,?)",(-1, None,));
    cur_out.execute("INSERT INTO index2mentionID VALUES(?,?)",( 0, mention[0],));
    cur_out.execute("INSERT INTO index2repID     VALUES(?,?)",( 0, representation[0],));
    con_out.commit();

def insert_representations(Q,cur_in,cur_out,con_out):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        mentions        = cur_in.execute("SELECT * FROM mentions ORDER BY mentionID LIMIT ?,?",(start,size,)).fetchall();
        representations = mentions2representations(mentions); print(time.time()-t,'s for getting representations.'); t=time.time();
        print(tabulate([representations[0][                        :int(len(_columns)*(1/3))]], headers=_columns[                        :int(len(_columns)*(1/3))]),'\n');
        print(tabulate([representations[0][int(len(_columns)*(1/3)):int(len(_columns)*(2/3))]], headers=_columns[int(len(_columns)*(1/3)):int(len(_columns)*(2/3))]),'\n');
        print(tabulate([representations[0][int(len(_columns)*(2/3)):                        ]], headers=_columns[int(len(_columns)*(2/3)):                        ]),'\n');
        #------------------------------------------------------------------------------------------------------------------------------------
        cur_out.executemany("INSERT INTO representations(repID,freq,"+_featstring+") VALUES("+_questionmarks+") ON CONFLICT(repID) DO UPDATE SET freq=freq+?",(list(rep)+[rep[1]] for rep in representations));
        print(time.time()-t,'s for inserting representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        cur_out.executemany("INSERT INTO mention2repID  (repID)     VALUES(?)",((representation[0],) for representation in representations));
        cur_out.executemany("INSERT INTO index2mentionID(mentionID) VALUES(?)",((mention[0]       ,) for mention        in mentions       ));
        cur_out.executemany("INSERT INTO index2repID    (repID)     VALUES(?)",((representation[0],) for representation in representations));
        con_out.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;

con_out = sqlite3.connect(_outdb);
cur_out = con_out.cursor();

cur_out.execute("DROP TABLE IF EXISTS representations");
cur_out.execute("DROP TABLE IF EXISTS mention2repID");
cur_out.execute("DROP TABLE IF EXISTS index2mentionID");
cur_out.execute("DROP TABLE IF EXISTS index2repID");
cur_out.execute("CREATE TABLE representations("+', '.join([_columns[i]+' '+_types[i] for i in range(len(_columns))])+")");
cur_out.execute("CREATE TABLE mention2repID  (mentionIDIndex INTEGER PRIMARY KEY, repID     TEXT)");
cur_out.execute("CREATE TABLE index2mentionID(mentionIDIndex INTEGER PRIMARY KEY, mentionID INT  UNIQUE ON CONFLICT IGNORE)");
cur_out.execute("CREATE TABLE index2repID    (repIDIndex     INTEGER PRIMARY KEY, repID     TEXT UNIQUE ON CONFLICT IGNORE)");

num_rows = cur_in.execute("SELECT count(*) FROM mentions").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

initialize_primary_keys(cur_in,cur_out,con_out);
insert_representations(Q,cur_in,cur_out,con_out);

cur_out.execute("CREATE INDEX repID_on_mention_index ON mention2repID(repID)");

con_in.close();
con_out.close();
#-----------------------------------------------------------------------------------------------------------------------
