import sqlite3
import sys
import time

_inDB  = sys.argv[1];
_outDB = sys.argv[2];
_types = sys.argv[3];

_batch  = 1000;

_fields = dict();
TYPES   = open(_types);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();
_featOf  = {field:feature for feature in _fields for field in _fields[feature]};
_columns = set(_featOf.keys());
__fields = [field for feature in _fields for field in _fields[feature]];

#TODO: For some reason we run into a problem at find_components_disk.py when we accept all representations, but like this we drop too much
#TODO: Seems like this only happens with the empty representation...
#_constraint = "university1 IS NOT NULL OR agency1 IS NOT NULL OR association1 IS NOT NULL OR academy1 IS NOT NULL OR factory1 IS NOT NULL OR college1 IS NOT NULL OR clinic1 IS NOT NULL OR company1 IS NOT NULL OR faculty1 IS NOT NULL OR center1 IS NOT NULL OR site1 IS NOT NULL OR field1 IS NOT NULL OR lab1 IS NOT NULL OR collection1 IS NOT NULL OR institute1 IS NOT NULL OR subfield1 IS NOT NULL OR subject1 IS NOT NULL OR community1 IS NOT NULL OR chair1 IS NOT NULL OR other1 IS NOT NULL";
_constraint = " OR ".join([field+" IS NOT NULL" for field in __fields]);
#"id IS NOT NULL OR (community1 IS NOT NULL OR division1 IS NOT NULL OR none1 IS NOT NULL OR city1 IS NOT NULL OR center1 IS NOT NULL OR faculty1 IS NOT NULL OR area1 IS NOT NULL OR institute1 IS NOT NULL OR academy1 IS NOT NULL OR university1 IS NOT NULL OR agency1 IS NOT NULL OR factory1 IS NOT NULL OR collection1 IS NOT NULL OR site1 IS NOT NULL OR clinic1 IS NOT NULL OR college1 IS NOT NULL OR lab1 IS NOT NULL OR polytechnic1 IS NOT NULL OR chair1 IS NOT NULL OR company1 IS NOT NULL OR association1 IS NOT NULL)";
#"rid IS NOT NULL OR (l IS NOT NULL AND l_ IS NOT NULL)"; #"l IS NOT NULL AND l_ IS NOT NULL"; #"a1sur IS NOT NULL AND (term1 IS NOT NULL OR term1gen IS NOT NULL)";


con   = sqlite3.connect(_inDB);
cur   = con.cursor();
con_w = sqlite3.connect(_outDB);
cur_w = con_w.cursor();

cur_w.execute("DROP   TABLE IF EXISTS features");
cur_w.execute("DROP   TABLE IF EXISTS index2feat");
cur_w.execute("DROP   TABLE IF EXISTS index2repID");
cur_w.execute("CREATE TABLE           features   (repIDIndex INT,                 featIndex INT)");
cur_w.execute("CREATE TABLE           index2feat (featIndex  INTEGER PRIMARY KEY, featGroup TEXT, feat TEXT)");
cur_w.execute("CREATE TABLE           index2repID(repIDIndex INTEGER PRIMARY KEY, repID     INT)");

repID2index = dict();
grp_expression  = ','.join([featname for group in _fields for featname in _fields[group]]);
sum_expression  = '('+')+('.join([featname+' IS NULL' for group in _fields for featname in _fields[group]])+')'; print(sum_expression);
cur.execute("SELECT repID FROM representations WHERE "+_constraint+" ORDER BY "+sum_expression+" DESC"); #DESC in number of NULLs -> ASC in size!
for row in cur:
    length              = len(repID2index);
    repID2index[row[0]] = length;
    if length % 1000000 == 0: print(length);

print('writing index2repID...');
cur_w.executemany("INSERT INTO index2repID VALUES(?,?)"  ,((repID2index[repID], repID,) for repID in repID2index));
con_w.commit();

offset = 0;
for group in _fields:
    print(group);
    feat2index = dict();
    length     = 0;
    for featname in _fields[group]:
        print(featname);
        cur.execute("SELECT "+featname+" FROM representations WHERE "+featname+" IS NOT NULL AND "+_constraint);
        for row in cur:
            feat = row[0];
            if not feat in feat2index:
                feat2index[feat] = offset + length;
                length          += 1;
                if length % 100000 == 0: print(length);
    offset += length;
    cur.execute("SELECT "+','.join(['repID']+_fields[group])+" FROM representations WHERE "+_constraint);
    print('writing',group,'...');
    cur_w.executemany("INSERT INTO features        VALUES(?,?)"  ,((repID2index[row[0]],feat2index[feat],) for row in cur for feat in row[1:] if feat != None));
    print('writing index2feat...');
    cur_w.executemany("INSERT INTO index2feat      VALUES(?,?,?)",((feat2index[feat],   group,feat,      ) for feat in feat2index));
    con_w.commit();

con.close();

print('creating index on repIDIndex...');
cur_w.execute("CREATE INDEX repIDindex_index ON features(repIDIndex)");
print('creating index on featIndex...');
cur_w.execute("CREATE INDEX featIndex_index ON features(featIndex)");
print('creating index on repID...');
cur_w.execute("CREATE INDEX repID_index ON index2repID(repID)");

con_w.close();
