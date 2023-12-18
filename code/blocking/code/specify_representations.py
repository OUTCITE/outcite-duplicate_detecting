import sqlite3
import sys
import time

_db        = sys.argv[1];
_rule_file = sys.argv[2]; # Describes one or more lists of features where for each list, at least one feature must be not NULL.
                          # In other words, specify combinations that are illegal if ALL of the features are NULL.
con = sqlite3.connect(_db);
cur = con.cursor();

IN = open(_rule_file);
_restrictions = [line.rstrip().split() for line in IN.readlines()];
IN.close();

selectors = [ "("+' AND '.join([feature+" IS NULL" for feature in _restrictions[i]])+") AS tmp"+str(i) for i in range(len(_restrictions))];

print(      "CREATE TEMPORARY TABLE underspecified AS SELECT repID AS repID, "+', '.join(selectors)+" FROM representations"); #TODO: Should this be SELECT DISTINCT repID?
cur.execute("CREATE TEMPORARY TABLE underspecified AS SELECT repID AS repID, "+', '.join(selectors)+" FROM representations"); #TODO: Should this be SELECT DISTINCT repID?

for i in range(len(_restrictions)):
    print(      "UPDATE representations SET "+_restrictions[i][0]+"=repID WHERE repID IN (SELECT repID FROM underspecified WHERE tmp"+str(i)+"=1"+['',' AND '][i>0]+" AND ".join(['tmp'+str(j)+'=0' for j in range(0,i)])+")");
    cur.execute("UPDATE representations SET "+_restrictions[i][0]+"=repID WHERE repID IN (SELECT repID FROM underspecified WHERE tmp"+str(i)+"=1"+['',' AND '][i>0]+" AND ".join(['tmp'+str(j)+'=0' for j in range(0,i)])+")");
    con.commit();

con.close();
