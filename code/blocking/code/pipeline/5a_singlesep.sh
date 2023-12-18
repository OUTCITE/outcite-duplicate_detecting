features=$1
report=$2
components=$3

echo $folder

sqlite3 ${components} << EOF
ATTACH DATABASE "${features}" AS feats;
DROP TABLE IF EXISTS main.components;
CREATE TABLE main.components(label INT, repIDIndex INT);
INSERT INTO main.components SELECT featIndex,repIDIndex FROM feats.features;
CREATE INDEX main.label_index ON components(label);
CREATE INDEX main.repIDIndex_index ON components(repIDIndex);
EOF
