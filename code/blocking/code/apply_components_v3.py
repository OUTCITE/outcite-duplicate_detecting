import sqlite3
import sys
from collections import defaultdict

_mentDB = sys.argv[1];
_compDB = sys.argv[2];
_featDB = sys.argv[3];
_reprDB = sys.argv[4];
_outDB  = sys.argv[5];

_ONLY_LABELLED = False if len(sys.argv)>6 and sys.argv[6]=='0' else True; # Set to False to export all labels even if no goldID is available -- required for OUTCITE duplicate detection!
_INTEGER_ID    = False if len(sys.argv)>7 and sys.argv[7]=='0' else True; # Set to False to use originalID's instead of mentionID's          -- required for OUTCITE duplicate detection!


def get_rows(cur,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID,mentionID2id):
    i = 0;
    for label, repIDIndex in cur:
        repID = repIDIndex2repID[repIDIndex];
        if repID:
            for mentionIDIndex in repID2mentionIDIndeces[repID]:
                mentionID = mentionIDIndex2mentionID[mentionIDIndex];
                if mentionID and mentionID in mentionID2id:
                    i += 1;
                    if i % 10000 == 0:
                        print(i,end='\r');
                    yield (mentionID,repID,mentionID2id[mentionID],label,);

def get_additionals(max_label,missing_mentionIDs):
    missing_repIDs = 0;
    i              = 0;
    for mentionID in missing_mentionIDs:
        goldID          = mentionID2id[mentionID];
        mentionIDIndex  = mentionID2mentionIDIndex[mentionID];
        repID           = mentionIDIndex2repID[mentionIDIndex];
        repIDIndex      = repID2repIDIndex[repID] if repID in repID2repIDIndex else None; # TODO: How can it happen that repID is not in repID2repIDIndex???
        label           = max_label+1+repIDIndex if repIDIndex != None else repID;
        missing_repIDs += not repID in repID2repIDIndex;
        i              += 1;
        if i % 10000 == 0:
            print(i,missing_repIDs,end='\r');
        yield (mentionID,repID,goldID,label,)

print('Creating mapping mentionID --> goldID...'); # Only mentionIDs with an id not NULL
con = sqlite3.connect(_mentDB);
cur = con.cursor();
cur.execute("SELECT "+["originalID","mentionID"][_INTEGER_ID]+", goldID FROM mentions"+[""," WHERE goldID IS NOT NULL"][_ONLY_LABELLED]);
mentionID2id = {mentionID:ID for mentionID, ID in cur};
if not _INTEGER_ID:
    print('Creating mapping mentionID --> originalID...');
    cur.execute("SELECT mentionID,originalID FROM mentions"+[""," WHERE goldID IS NOT NULL"][_ONLY_LABELLED]);
mentionID2originalID = {mentionID:originalID for mentionID, originalID in cur};
con.close();

print('Creating mapping mentionIDIndex --> mentionID...'); # Only mentionIDIndeces with an mentionID with an id not NULL
con = sqlite3.connect(_reprDB);
cur = con.cursor();
max_index = cur.execute("SELECT max(mentionIDIndex) FROM index2mentionID").fetchall()[0][0];
mentionIDIndex2mentionID = [None for i in range(max_index+1)];
mentionID2mentionIDIndex = dict();
#all_mentionIDs           = set([]);
cur.execute("SELECT mentionIDIndex, mentionID FROM index2mentionID");
for mentionIDIndex, mentionID in cur:
    #all_mentionIDs.add(mentionID);
    mentionID      = int(mentionID) if _INTEGER_ID else mentionID2originalID[mentionID]; #Pending further correction in the make_representations step
    mentionIDIndex = int(mentionIDIndex);
    if mentionID in mentionID2id:
        mentionIDIndex2mentionID[mentionIDIndex] = mentionID;
        mentionID2mentionIDIndex[mentionID]      = mentionIDIndex;

print('Creating mapping repID --> mentionIDIndeces...'); # Only repIDs with a mentionIDIndex with an mentionID with an id not NULL
cur.execute("SELECT repID, mentionIDIndex FROM mention2repID");
repID2mentionIDIndeces = defaultdict(set);
mentionIDIndex2repID   = dict();
for repID, mentionIDIndex in cur:
    mentionIDIndex                       = int(mentionIDIndex);
    mentionIDIndex2repID[mentionIDIndex] = repID
    if mentionIDIndex2mentionID[mentionIDIndex]:
        repID2mentionIDIndeces[repID].add(mentionIDIndex);
con.close();

print('Creating mapping repIDIndex --> repID...'); # Only repIDIndices with a repID with an mentionIDIndex with an mentionID with an id not NULL
con       = sqlite3.connect(_featDB);
cur       = con.cursor();
max_index = cur.execute("SELECT max(repIDIndex) FROM index2repID").fetchall()[0][0];
cur.execute("SELECT repIDIndex, repID FROM index2repID");
repIDIndex2repID = [None for i in range(max_index+1)];
repID2repIDIndex = dict();
for repIDIndex, repID in cur:
    repIDIndex              = int(repIDIndex);
    repID2repIDIndex[repID] = repIDIndex;
    if repID in repID2mentionIDIndeces:
        repIDIndex2repID[repIDIndex] = repID;
con.close();

con     = sqlite3.connect(_compDB);
cur     = con.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

print('Creating target  mentions...');
cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute("CREATE TABLE mentions(mentionID PRIMARY KEY, repID TEXT, goldID INT, label INT)");
cur.execute("SELECT label, repIDIndex FROM components");
cur_out.executemany("INSERT INTO mentions VALUES(?,?,?,?)",get_rows(cur,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID,mentionID2id));
con_out.commit();

print('Adding singleton component mentions...');
lab_gold_mentionIDs = set([row[0] for row in cur_out.execute("SELECT mentionID FROM mentions").fetchall()]);
gold_mentionIDs     = set(mentionID2id.keys());
missing_mentionIDs  = sorted(list(gold_mentionIDs - lab_gold_mentionIDs));
max_label           = cur_out.execute("SELECT max(label) FROM mentions").fetchall()[0][0];
#max_label           = 0 if isinstance(max_label,str) or max_label==None else max_label;
#TODO: Below is a mistake: The label should be based on the repID, not the mentionID!
#cur_out.executemany("INSERT INTO mentions VALUES(?,?,?,?)",((missing_mentionIDs[i],None,mentionID2id[missing_mentionIDs[i]],max_label+1+i,) for i in range(len(missing_mentionIDs))));
cur_out.executemany("INSERT INTO mentions VALUES(?,?,?,?)",get_additionals(max_label,missing_mentionIDs));
con_out.commit();

print('Creating target  indices...');
cur_out.execute("CREATE INDEX mentions_repID_index on mentions(repID)");
cur_out.execute("CREATE INDEX mentions_id_index    on mentions(goldID)");
cur_out.execute("CREATE INDEX mentions_label_index on mentions(label)");

con.close();
con_out.close();
