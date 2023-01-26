import sqlite3
import sys
import re

_indb  = sys.argv[1];
_outdb = sys.argv[2];

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'anystyle_references_from_pdftotext_fulltext',
                'cermine_references_from_cermine_xml',   #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml',
                'exparser_references_from_cermine_layout' ];

DOCID = re.compile('|'.join(['((?<='+refobj+').+_ref_)' for refobj in _refobjs]));

def get_blocks(cur):
    row_num      = 0;
    index2docID  = [];
    docID2index  = dict();
    num_rows     = cur.execute("SELECT COUNT(*) FROM refmetas").fetchall()[0][0];
    cur.execute("SELECT linkID FROM refmetas");
    for row in cur:
        row_num += 1;
        linkID   = row[0];
        if row_num % 1000 == 0:
            print(round(100.0*row_num/num_rows,2),'%  ',end='\r');
        docID = DOCID.search(linkID).group(0)[1:-5];
        if not docID in docID2index:
            docID2index[docID] = len(index2docID);
            index2docID.append(docID);
        yield (linkID,docID,None,docID2index[docID],);

#-----------------------------------------------------------------------------------------------------------------------

con     = sqlite3.connect(_indb);
cur     = con.cursor();
con_out = sqlite3.connect(_outdb);
cur_out = con_out.cursor();

cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute('CREATE TABLE mentions(mentionID PRIMARY KEY, repID TEXT, goldID INT, label INT)');

cur_out.executemany("INSERT INTO mentions VALUES("+','.join(['?' for i in range(4)])+")",get_blocks(cur));
con_out.commit();

cur_out.execute("CREATE INDEX IF NOT EXISTS repID_index ON mentions(repID)");
cur_out.execute("CREATE INDEX IF NOT EXISTS label_index ON mentions(label)");

con.close();
con_out.close();
