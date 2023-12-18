import sqlite3
import sys
import time
import hashlib
from itertools import product, combinations

_db              = sys.argv[1];
_types           = sys.argv[2];
_rule_file       = sys.argv[3]; #Describes one or more lists of features where for each list, at least one feature must be not NULL
_generalize_file = sys.argv[4];

_batchsize = 100000;

IN = open(_rule_file);
_restrictions = [line.rstrip().split() for line in IN.readlines()];
IN.close();

_ftype2column = dict();
TYPES   = open(_types);
for line in TYPES:
    ftype, rest = line.rstrip().split(':');
    _ftype2column[ftype] = rest.split(' ');
TYPES.close();
_column2ftype = {column:ftype for ftype in _ftype2column for column in _ftype2column[ftype]};

con = sqlite3.connect(_db);
cur = con.cursor();

_columns = [row[1] for row in cur.execute("PRAGMA table_info(representations)")];# if not row[1] in set(['repID','freq'])];

__fields      = [field for feature in _ftype2column for field in _ftype2column[feature]];
_column2index = {_columns[i]:i for i in range(len(_columns))};

_additionals    = [];
_selection      = list(range(2,2+len(__fields)+len(_additionals)));
_questionmarks  = ','.join(['?' for i in range(len(_columns))]);
_groups         = sorted([[_column2index[field] for field in _ftype2column[key]] for key in _ftype2column])
_requiredftypes = [set([_column2ftype[column] for column in restriction]) for restriction in _restrictions]

def is_ok(rhs):
    rhstypes = set([typ for freq,typ in rhs if freq>0]); # the ftypes in the generalization result
    for disjunction in _requiredftypes:
        if len(disjunction & rhstypes) == 0: # no overlap
            return False;
    return True;

_ftypes   = [];
_generals = dict();
IN = open(_generalize_file);
for line in IN.readlines():
    lhs, rhss                                    = line.rstrip().split(' --> ');
    lhs, rhss                                    = lhs.split(), [rhs.split() for rhs in rhss.split(' | ')];
    lhs                                          = [(int(lhs[i]),lhs[i+1],) for i in range(0,len(lhs),2)];
    rhss                                         = [[(int(rhs[i]),rhs[i+1],) for i in range(0,len(rhs),2)] for rhs in rhss];
    rhss                                         = [rhs for rhs in rhss if is_ok(rhs)];
    _ftypes                                      = [typ for freq,typ in lhs];
    _generals[tuple([freq for freq,typ in lhs])] = [tuple([freq for freq,typ in rhs]) for rhs in rhss];
IN.close();

_ftypeindex2columnindeces = [[_column2index[column] for column in _ftype2column[_ftypes[i]]] for i in range(len(_ftypes))];

print('_ftype2column:',_ftype2column); print('_column2index:',_column2index); #print('_generals:',_generals);  print('_ftypes:',_ftypes); print('_generals:',_generals);

def get_intype(row):
    return tuple([sum([row[_column2index[field]]!=None for field in _ftype2column[conditional]]) for conditional in _ftypes]);

def is_valid_(row):
    if row[2] == None and row[5] == None and row[8] == None and row[11] == None: # no surname
        return False;
    if row[14] == None and row[15] == None and row[16] == None and row[17] == None: # no term
        return False;
    return True;

def is_valid(row):
    for restriction in _restrictions:
        underspecified = True;
        for field in restriction:
            if row[_column2index[field]]:
                underspecified = False;
                break;
        if underspecified:
            return False;
    return True;

def generalize_(row):
    year_drops      = [set([1]),set([2])];
    name_drops      = [set([3,4,5]),set([6,7,8]),set([9,10,11]),set([12,13,14])];
    term_drops      = [set([15,16,17,18]),set([15,19]),set([16,20]),set([17,21]),set([18,22])];
    year_drops      = [set([drop for drop in year_drop if row[drop] != None]) for year_drop in year_drops];
    name_drops      = [set([drop for drop in name_drop if row[drop] != None]) for name_drop in name_drops];
    term_drops      = [set([drop for drop in term_drop if row[drop] != None]) for term_drop in term_drops];
    year_drops      = [year_drop for year_drop in year_drops if len(year_drop)>0];
    name_drops      = [name_drop for name_drop in name_drops if len(name_drop)>0];
    term_drops      = [term_drop for term_drop in term_drops if len(term_drop)>0];
    yeneralizations = [[row[i] if not i in year_drop else None for i in range(len(row))] for year_drop in year_drops];
    neneralizations = [[row[i] if not i in name_drop else None for i in range(len(row))] for name_drop in name_drops];
    teneralizations = [[row[i] if not i in term_drop else None for i in range(len(row))] for term_drop in term_drops];
    generalizations = [generalization for generalization in yeneralizations+neneralizations+teneralizations if is_valid(generalization)];
    return generalizations;

def pick(rfreq,lfreq,i,row): # Return all possible combination of picking rfreq out of lfreq columns for ftypeindex i
    non_null_indeces = [i for i in _ftypeindex2columnindeces[i] if row[i]];
    return list(combinations(non_null_indeces,lfreq-rfreq));

def generalize(row):
    drop_always = set([]);#set([_column2index[field] for ftype in ['title'] for field in _ftype2column[ftype]]);
    lhs         = get_intype(row); #print(row,intype)
    rhss        = _generals[lhs] if lhs in _generals else [];
    if len(rhss) == 0:
        if lhs[0] > 0: print('illegal lhs',lhs);
        return [];
    drops    = [set([el for tup in tups for el in tup]) for rhs in rhss for tups in product(*[pick(rhs[i],lhs[i],i,row) for i in range(len(_ftypes))])];
    generals = [[row[i] if row[i] and not i in drop and not i in drop_always else None for i in range(len(row))] for drop in drops];
    #print(row);print(drops);print(generals);print('----------------------------------------------------');
    return generals;

def bundle(row):
    groups = _groups#[[3,6,9,12],[4,7,10,13],[5,8,11,14],[15,16,17,18],[19,20,21,22]]; #TODO: the last group and the second to last need remain in sync!
    row_   = [cell for cell in row];
    for group in groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def make_repID(row):
    #return '+++'.join((str(row_r[x]) for x in _selection));
    return hashlib.sha1('#+*'.join([str(x)+'*+'+str(row[x]) for x in _selection]).encode("utf-8")).hexdigest();

def representations2generalizations(rows_r):
    rows_g = [generalization for row_r in rows_r for generalization in generalize(row_r)];
    print(rows_r[-1]); print(rows_g[-1]);
    print(len(rows_g))
    rows_r = [bundle(row_g) for row_g in rows_g];
    print(len(rows_r))
    repIDs = [make_repID(row_r) for row_r in rows_r];#['+++'.join((str(row_r[x]) for x in _selection)) for row_r in rows_r];
    print(len(repIDs))
    rows_r = [tuple([repIDs[i],0]+[rows_r[i][x] for x in _selection]) for i in range(len(repIDs))];
    print(len(rows_r))
    return rows_r;

def insert_representations(Q,cur,con):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        representations = cur.execute("SELECT * FROM representations ORDER BY rowid LIMIT ?,?",(start,size,)).fetchall();
        print(time.time()-t,'s for getting',len(representations),'representations.'); t=time.time();
        generalizations = representations2generalizations(representations);
        print(time.time()-t,'s for getting',len(generalizations),'generalizations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        #print(("INSERT INTO representations VALUES ("+_questionmarks+") ON CONFLICT(repID) DO NOTHING",generalizations,))
        cur.executemany("INSERT INTO representations VALUES ("+_questionmarks+") ON CONFLICT(repID) DO NOTHING",generalizations);
        #cur.executemany("INSERT INTO representations(repID,"+_ftypes+") VALUES(?,"+_questionmarks+")",generalizations);
        print(time.time()-t,'s for inserting',len(generalizations),'representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        #print(("INSERT INTO index2repID(repID) VALUES(?)",((representation[0],) for representation in generalizations),))
        cur.executemany("INSERT INTO index2repID(repID) VALUES(?)",((representation[0],) for representation in generalizations));
        con.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;

#input('Enter to continue')

num_rows = cur.execute("SELECT count(*) FROM representations").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

#cur.execute("DROP INDEX IF EXISTS repID_index");

insert_representations(Q,cur,con);

#cur.execute("CREATE INDEX repID_index ON index2repID(repID)");

con.close();

# A type of query that can help understand how to generalize:
# select goldID,count(*),a1sur,a2sur,a3sur,a4sur from mentions where goldID in (select id from (select id,count(*) as freq from mentions where goldID is not NULL group by goldID) where freq > 10 limit 0,10) group by goldID,a1sur,a2sur,a3sur,a4sur;
