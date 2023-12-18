import sqlite3
import sys
import time
import math

_db        = sys.argv[1];

_max_feats = 2; #TODO: The scoring with length-2 feature lists is not very good right now
_threshold = 1;

con   = sqlite3.connect(_db);
cur   = con.cursor();
con_  = sqlite3.connect(_db);
cur_  = con_.cursor();

columns = [row[1] for row in cur.execute("PRAGMA table_info(representations)").fetchall()];

num_feats = { row[0]: sum([cell != None for cell in row[2:]]) for row in cur.execute("SELECT * FROM representations") };

def selector(num_feats,cur):
    for repID in num_feats:
        if num_feats[repID] <= _max_feats:
            row    = cur.execute('SELECT * FROM representations WHERE repID=?',(repID,)).fetchall()[0];
            freq   = int(row[1]);
            #counts = [];
            values = [];
            #TODO: GET AND-COMBINED COUNT
            for i in range(2,len(columns)):
                if row[i] != None:
                    #count = cur.execute('SELECT COUNT(*) FROM representations WHERE '+' OR '.join([columns[i][:-1]+str(j)+'="'+row[i]+'"' for j in range(1,5)])).fetchall()[0][0];
                    #counts.append(count);
                    values.append((columns[i],row[i],));
                if len(values) == _max_feats:
                    break;
            if len(values) == 0: # If there is actually no feature at all
                yield (columns[2],repID,);
                continue;
            ors   = [' OR '.join([column[:-1]+str(j)+'="'+value+'"' for j in range(1,5)]) for column,value in values];
            query = 'SELECT COUNT(*) FROM representations WHERE ('+') AND ('.join(ors)+')';
            count = cur.execute(query).fetchall()[0][0];
            score = freq / count;
            if score < _threshold:
                print(' '.join([v1+':'+v2 for v1,v2 in values]),'<--------',score,'|',freq,count);
                yield (values[-1][0][:-1]+str(len(values)),repID,);
            else:
                print(' '.join([v1+':'+v2 for v1,v2 in values]),'xxxxxxxxx',score,'|',freq,count);

specifications = list(selector(num_feats,cur));

for attribute,repID in specifications:
    print(       'UPDATE representations SET '+attribute+'="'+repID+'" WHERE repID="'+repID+'"');
    cur_.execute('UPDATE representations SET '+attribute+'="'+repID+'" WHERE repID="'+repID+'"');
    con_.commit();

con.close();
con_.close();
