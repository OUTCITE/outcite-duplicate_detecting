#for min_gold_size in 1 2; do for max_gold_size in 10 25 50 100 1000 10000; do for max_label_size in 10 100 1000 10000 100000 1000000; do bash code/pipeline/8_evaluate_components.sh publications 04 restrictions_publications_stricter 4261_3021_4441_0_20 $min_gold_size $max_gold_size $max_label_size  pair "authors, year, title words" poset; done; done; done

arg=$1
disk=$2
spechem=$3
scheme=$4
min_gold_size=$5
max_gold_size=$6
max_label_size=$7
mode=$8
representation=$9 #"authors, year, title words", ...
method=${10}

restrictor=goldID; #label; #goldID

table=mentions
components=code/blocking/representations_${arg}/${spechem}/${scheme}/components_${method}.db;
file=code/blocking/representations_${arg}/${spechem}/${scheme}/labelled_mentions_${method}.db;
result_db=code/blocking/representations_${arg}/${spechem}/${scheme}/results_${method}.db;

if [ $mode == 'core' ]; then
    table=mentions_core;
fi

max_size_data=`sqlite3  ${components} "SELECT freq FROM (select count(*) AS freq,label FROM components GROUP BY label) GROUP BY freq ORDER BY freq DESC LIMIT 1"`;
avg_size_data=`sqlite3  ${components} "SELECT COUNT(*)/CAST(COUNT(DISTINCT label) as REAL) FROM components"`;
max_size_gold=`sqlite3  ${file} "SELECT freq FROM (select count(*) AS freq,goldID FROM ${table} GROUP BY goldID) GROUP BY freq ORDER BY freq DESC LIMIT 1"`;
avg_size_gold=`sqlite3  ${file} "SELECT COUNT(*)/CAST(COUNT(DISTINCT goldID) AS REAL) FROM ${table}"`;

# Precompute the illegal labels and goldIDs as table to select from
sqlite3 ${file} "DROP TABLE IF EXISTS illegal_labels";
sqlite3 ${file} "DROP TABLE IF EXISTS illegal_goldIDs";
sqlite3 ${file} "ATTACH DATABASE '${components}' as comps; CREATE TABLE main.illegal_labels  AS SELECT label FROM (SELECT label ,COUNT(*) AS freq FROM comps.components GROUP BY label ) WHERE freq>${max_label_size}";
sqlite3 ${file} "CREATE TABLE illegal_goldIDs AS SELECT goldID FROM (SELECT goldID,COUNT(*) AS freq FROM ${table} GROUP BY goldID) WHERE freq>${max_gold_size} OR freq<${min_gold_size}";

# The below is isolating all mentions that are in oversize components by assigning them individual labels each, kind of modified method and is ignoring all labelled mentions for which there is either too great a gold size or too great a component size, kind of cheating
T=`sqlite3    ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE                                                     goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY goldID      )"`;
Pa=`sqlite3   ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE label NOT IN (SELECT label FROM illegal_labels) AND goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY        label)"`;
Pbr=`sqlite3  ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE label     IN (SELECT label FROM illegal_labels) AND goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY        repID)"`;
if [ -z "$Pbr" ]; then
    Pbr=0;
fi
#Pbm=`sqlite3  ${file} "SELECT SUM(num   ) FROM (SELECT COUNT(distinct mentionID)                           AS num    FROM ${table} WHERE label     IN (SELECT label FROM illegal_labels) AND goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY        label)"`;
TPa=`sqlite3  ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE label NOT IN (SELECT label FROM illegal_labels) AND goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY goldID,label)"`;
TPbr=`sqlite3 ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE label     IN (SELECT label FROM illegal_labels) AND goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY goldID,repID)"`;
if [ -z "$TPbr" ]; then
    TPbr=0;
fi
#TPbm=$Pb_s;
used_ments=`sqlite3 ${file} "SELECT COUNT(*) FROM ${table} WHERE goldID NOT IN (SELECT goldID FROM illegal_goldIDs)"`;

echo Number of illegal labels: `sqlite3 ${file} "SELECT COUNT(*) FROM ${table} WHERE label NOT IN (SELECT label FROM illegal_labels)"`;

# The below is ignoring all labelled mentions for which there is either too great a gold size or too great a component size, kind of cheating
#Tr=`sqlite3  ${file} "SELECT SUM(square) FROM (SELECT COUNT(distinct mentionID)*COUNT(distinct mentionID) AS square FROM ${table} WHERE goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY goldID      )"`;
Pr=`sqlite3  ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY        repID)"`;
TPr=`sqlite3 ${file} "SELECT SUM(square) FROM (SELECT COUNT(*)*COUNT(*) AS square FROM ${table} WHERE goldID NOT IN (SELECT goldID FROM illegal_goldIDs) GROUP BY goldID,repID)"`;

sqlite3 $result_db "CREATE TABLE IF NOT EXISTS results(type TEXT,representation TEXT,specification TEXT,generalization TEXT,eval_mode TEXT,dataset TEXT,min_gold_size INT,max_gold_size INT,max_label_size INT,max_size_data INT,avg_size_data REAL,max_size_gold INT,avg_size_gold REAL,label_prec REAL,label_rec REAL,rep_prec REAL,rep_rec REAL,base_prec REAL,base_rec REAL,label_tp INT,label_p INT,t INT,label_tn INT,label_n INT,rep_tp INT,rep_p INT,rep_n INT,rep_tn INT,used_ments INT)"

# USED TO ADDRESS THE CURRENT ROW IN RESULTS: type, representation, specification, generalization, eval_mode, min_gold_size, max_gold_size
val_type=$arg
val_representation=$representation
val_specification=$spechem
val_generalization=$scheme
val_eval_mode="pair"
val_dataset=$table
val_min_gold_size=$min_gold_size
val_max_gold_size=$max_gold_size
val_max_label_size=$max_label_size
val_max_size_data=$max_size_data
val_avg_size_data=$avg_size_data
val_max_size_gold=$max_size_gold
val_avg_size_gold=$avg_size_gold
label_prec=`echo "scale=2;100*((${TPa}+${TPbr}))/((${Pa}+${Pbr}))" | bc`
label_rec=`echo "scale=2;100*((${TPa}+${TPbr}))/${T}" | bc`
rep_prec=`echo "scale=2;100*${TPr}/${Pr}" | bc`
rep_rec=`echo "scale=2;100*${TPr}/${T}" | bc`
base_rec=`echo "scale=2;100*${used_ments}/${T}" | bc`
base_prec=100
label_tp=`echo "((${TPa}+${TPbr}))" | bc`
label_p=`echo "((${Pa}+${Pbr}))" | bc`
label_t=${T}
label_tn="NULL"
label_n="NULL"
rep_tp=${TPr}
rep_p=${Pr}
rep_tn="NULL"
rep_n="NULL"

echo temporary directory: ${SQLITE_TMPDIR};

echo "INSERT INTO results VALUES(\"${val_type}\",\"${val_representation}\",\"${val_specification}\",\"${val_generalization}\",\"${val_eval_mode}\",\"${val_dataset}\",${val_min_gold_size},${val_max_gold_size},${val_max_label_size},${val_max_size_data},${val_avg_size_data},${val_max_size_gold},${val_avg_size_gold},${label_prec},${label_rec},${rep_prec},${rep_rec},${base_prec},${base_rec},${label_tp},${label_p},${label_t},${label_tn},${label_n},${rep_tp},${rep_p},${rep_n},${rep_tn},${used_ments})"
sqlite3 $result_db "INSERT INTO results VALUES(\"${val_type}\",\"${val_representation}\",\"${val_specification}\",\"${val_generalization}\",\"${val_eval_mode}\",\"${val_dataset}\",${val_min_gold_size},${val_max_gold_size},${val_max_label_size},${val_max_size_data},${val_avg_size_data},${val_max_size_gold},${val_avg_size_gold},${label_prec},${label_rec},${rep_prec},${rep_rec},${base_prec},${base_rec},${label_tp},${label_p},${label_t},${label_tn},${label_n},${rep_tp},${rep_p},${rep_n},${rep_tn},${used_ments})"

echo --------------------------------------------------------------------------------------------------
echo PAIRWISE label-performance including identical representations
echo --------------------------------------------------------------------------------------------------
echo TP $label_tp
echo P  $label_p
echo T  $label_t
echo --------------------------------------------------------------------------------------------------
echo Precision $label_prec
echo Recall    $label_rec
echo --------------------------------------------------------------------------------------------------
#echo --------------------------------------------------------------------------------------------------
#echo representation-based performance as in different representations that should be and are connected
#echo --------------------------------------------------------------------------------------------------
#echo WARNING: THIS EVALUATION IS A BIT SUSPICIOUS AS YOU CAN HAVE THE SAME REPRESENTATION ACROSS GOLD-IDS!
#echo HENCE, YOU WILL COUNT THE SAME REPRESENTATION MULTIPLE TIMES DEPENDING ON THEIR MENTIONS LABELS.
#echo --------------------------------------------------------------------------------------------------
#echo TP label ${TPs[1]}
#echo TP rep   ${TPrr}
#echo P  ${Ps[1]}
#echo T        ${Ts[1]}
#echo --------------------------------------------------------------------------------------------------
#echo Precision `echo "scale=2;100*${TPs[1]}/${Ps[1]}" | bc`
#echo Recall label    `echo "scale=2;100*${TPs[1]}/${Ts[1]}" | bc`
#echo Recall rep      `echo "scale=2;100*${TPrr}/${Ts[1]}" | bc`
#echo --------------------------------------------------------------------------------------------------
echo --------------------------------------------------------------------------------------------------
echo PAIRWISE baseline using repIDs as labels to show how many mentions share the same repIDs
echo --------------------------------------------------------------------------------------------------
echo TP $TPr
echo P  $Pr
echo T  $T
echo --------------------------------------------------------------------------------------------------
echo Precision $rep_prec
echo Recall    $rep_rec
echo --------------------------------------------------------------------------------------------------
echo --------------------------------------------------------------------------------------------------

if [ $mode != 'core' ]; then
    exit 0
fi

# WEIRD EVALUATION FROM THE CORE PAPER

sqlite3 $file <<EOF
CREATE INDEX IF NOT EXISTS ${table}_repID_index on ${table}(repID);

DROP TABLE IF EXISTS gold_duplicates;
DROP TABLE IF EXISTS label_duplicates;
DROP TABLE IF EXISTS repID_duplicates;
DROP TABLE IF EXISTS label_gold_unions;
DROP TABLE IF EXISTS label_union_sizes;
DROP TABLE IF EXISTS repID_gold_unions;
DROP TABLE IF EXISTS label_sizes;
DROP TABLE IF EXISTS repID_union_sizes;
DROP TABLE IF EXISTS repID_sizes;

CREATE TABLE gold_duplicates  AS SELECT m.mentionID AS source, e.mentionID AS target FROM ${table} e INNER JOIN ${table} m ON m.goldID=e.goldID AND NOT m.mentionID=e.mentionID;
CREATE TABLE label_duplicates AS SELECT m.mentionID AS source, e.mentionID AS target FROM ${table} e INNER JOIN ${table} m ON m.label=e.label   AND NOT m.mentionID=e.mentionID;
CREATE TABLE repID_duplicates AS SELECT m.mentionID AS source, e.mentionID AS target FROM ${table} e INNER JOIN ${table} m ON m.repID=e.repID   AND NOT m.mentionID=e.mentionID;

CREATE TABLE label_gold_unions(source INT, target INT, UNIQUE(source,target));
INSERT or IGNORE INTO  label_gold_unions SELECT * FROM label_duplicates;
INSERT or IGNORE INTO  label_gold_unions SELECT * FROM gold_duplicates;

CREATE TABLE           repID_gold_unions(source INT, target INT, UNIQUE(source,target));
INSERT or IGNORE INTO  repID_gold_unions SELECT * FROM repID_duplicates;
INSERT or IGNORE INTO  repID_gold_unions SELECT * FROM gold_duplicates;

CREATE TABLE label_union_sizes AS SELECT source,COUNT(DISTINCT target) as size FROM label_gold_unions group by source;
CREATE TABLE label_sizes       AS SELECT source,COUNT(DISTINCT target) as size FROM label_duplicates group by source;

CREATE TABLE repID_union_sizes AS SELECT source,COUNT(DISTINCT target) as size FROM repID_gold_unions group by source;
CREATE TABLE repID_sizes       AS SELECT source,COUNT(DISTINCT target) as size FROM repID_duplicates group by source;
EOF
wait;

TP_core_lab=`sqlite3 $file "SELECT COUNT(*) FROM label_sizes INNER JOIN label_union_sizes ON label_sizes.source=label_union_sizes.source AND label_sizes.size=label_union_sizes.size"`;
FP_core_lab=`sqlite3 $file "SELECT COUNT(*) FROM label_sizes INNER JOIN label_union_sizes ON label_sizes.source=label_union_sizes.source AND label_sizes.size<label_union_sizes.size"`;
TN_core_lab=`sqlite3 $file "SELECT COUNT(*) FROM (SELECT DISTINCT mentionID FROM ${table} EXCEPT SELECT DISTINCT source FROM label_duplicates EXCEPT    SELECT DISTINCT source FROM gold_duplicates)"`;
FN_core_lab=`sqlite3 $file "SELECT COUNT(*) FROM (SELECT DISTINCT mentionID FROM ${table} EXCEPT SELECT DISTINCT source FROM label_duplicates INTERSECT SELECT DISTINCT source FROM gold_duplicates)"`;

TP_core_rep=`sqlite3 $file "SELECT COUNT(*) FROM repID_sizes INNER JOIN repID_union_sizes ON repID_sizes.source=repID_union_sizes.source AND repID_sizes.size=repID_union_sizes.size"`;
FP_core_rep=`sqlite3 $file "SELECT COUNT(*) FROM repID_sizes INNER JOIN repID_union_sizes ON repID_sizes.source=repID_union_sizes.source AND repID_sizes.size<repID_union_sizes.size"`;
TN_core_rep=`sqlite3 $file "SELECT COUNT(*) FROM (SELECT DISTINCT mentionID FROM ${table} EXCEPT SELECT DISTINCT source FROM repID_duplicates EXCEPT    SELECT DISTINCT source FROM gold_duplicates)"`;
FN_core_rep=`sqlite3 $file "SELECT COUNT(*) FROM (SELECT DISTINCT mentionID FROM ${table} EXCEPT SELECT DISTINCT source FROM repID_duplicates INTERSECT SELECT DISTINCT source FROM gold_duplicates)"`;

val_type=$arg
val_representation=$representation
val_specification=$spechem
val_generalization=$scheme
val_eval_mode="core"
val_dataset=$table
val_min_gold_size=$min_gold_size
val_max_gold_size=$max_gold_size
val_max_label_size=$max_label_size
val_max_size_data=$max_size_data
val_avg_size_data=$avg_size_data
val_max_size_gold=$max_size_gold
val_avg_size_gold=$avg_size_gold
label_prec=`echo "scale=2;100*${TP_core_lab}/$((TP_core_lab+FP_core_lab))" | bc`
label_rec=`echo "scale=2;100*${TP_core_lab}/$((TP_core_lab+FN_core_lab))" | bc`
rep_prec=`echo "scale=2;100*${TP_core_rep}/$((TP_core_rep+FP_core_rep))" | bc`
rep_rec=`echo "scale=2;100*${TP_core_rep}/$((TP_core_rep+FN_core_rep))" | bc`
base_prec="NULL"
base_rec="NULL"
label_tp=${TP_core_lab}
label_p=$((TP_core_lab+FP_core_lab))
label_t=$((TP_core_lab+FN_core_lab))
label_tn="NULL"
label_n="NULL"
rep_tp=${TP_core_rep}
rep_p=$((TP_core_rep+FP_core_rep))
rep_t=$((TP_core_rep+FN_core_rep))
rep_tn="NULL"
rep_n="NULL"
used_ments="NULL"

sqlite3 $result_db "INSERT INTO results VALUES(\"${val_type}\",\"${val_representation}\",\"${val_specification}\",\"${val_generalization}\",\"${val_eval_mode}\",\"${val_dataset}\",${val_min_gold_size},${val_max_gold_size},${val_max_label_size},${val_max_size_data},${val_avg_size_data},${val_max_size_gold},${val_avg_size_gold},${label_prec},${label_rec},${rep_prec},${rep_rec},${base_prec},${base_rec},${label_tp},${label_p},${label_t},${label_tn},${label_n},${rep_tp},${rep_p},${rep_n},${rep_tn},${used_ments})"

echo --------------------------------------------------------------------------------------------------
echo --------------------------------------------------------------------------------------------------
echo MAPPING-BASED mention-based performance including identical representations
echo --------------------------------------------------------------------------------------------------
echo TP ${TP_core_lab}
echo P  $((TP_core_lab+FP_core_lab))
echo T  $((TP_core_lab+FN_core_lab))
echo --------------------------------------------------------------------------------------------------
echo Precision `echo "scale=2;100*${TP_core_lab}/$((TP_core_lab+FP_core_lab))" | bc`
echo Recall    `echo "scale=2;100*${TP_core_lab}/$((TP_core_lab+FN_core_lab))" | bc`
echo --------------------------------------------------------------------------------------------------
echo --------------------------------------------------------------------------------------------------
echo MAPPING-BASED baseline using repIDs as labels to show how many mentions share the same repIDs
echo --------------------------------------------------------------------------------------------------
echo TP ${TP_core_rep}
echo P  $((TP_core_rep+FP_core_rep))
echo T  $((TP_core_rep+FN_core_rep))
echo --------------------------------------------------------------------------------------------------
echo Precision `echo "scale=2;100*${TP_core_rep}/$((TP_core_rep+FP_core_rep))" | bc`
echo Recall    `echo "scale=2;100*${TP_core_rep}/$((TP_core_rep+FN_core_rep))" | bc`
echo --------------------------------------------------------------------------------------------------

val_type=$arg
val_representation=$representation
val_specification=$spechem
val_generalization=$scheme
val_eval_mode="core_neg"
val_dataset=$table
val_min_gold_size=$min_gold_size
val_max_gold_size=$max_gold_size
val_max_label_size=$max_label_size
val_max_size_data=$max_size_data
val_avg_size_data=$avg_size_data
val_max_size_gold=$max_size_gold
val_avg_size_gold=$avg_size_gold
label_prec=`echo "scale=2;100*${TN_core_lab}/$((TN_core_lab+FN_core_lab))" | bc`
label_rec=`echo "scale=2;100*${TN_core_lab}/$((TN_core_lab+FP_core_lab))" | bc`
rep_prec=`echo "scale=2;100*${TN_core_rep}/$((TN_core_rep+FN_core_rep))" | bc`
rep_rec=`echo "scale=2;100*${TN_core_rep}/$((TN_core_rep+FP_core_rep))" | bc`
base_prec="NULL"
base_rec="NULL"
label_tp="NULL"
label_p="NULL"
label_t=$((TN_core_lab+FP_core_lab))
label_tn=${TN_core_lab}
label_n=$((TN_core_lab+FN_core_lab))
rep_tp="NULL"
rep_p="NULL"
rep_t=$((TN_core_rep+FP_core_rep))
rep_tn=${TN_core_rep}
rep_n=$((TN_core_rep+FN_core_rep))
used_ments="NULL"

sqlite3 $result_db "INSERT INTO results VALUES(\"${val_type}\",\"${val_representation}\",\"${val_specification}\",\"${val_generalization}\",\"${val_eval_mode}\",\"${val_dataset}\",${val_min_gold_size},${val_max_gold_size},${val_max_label_size},${val_max_size_data},${val_avg_size_data},${val_max_size_gold},${val_avg_size_gold},${label_prec},${label_rec},${rep_prec},${rep_rec},${base_prec},${base_rec},${label_tp},${label_p},${label_t},${label_tn},${label_n},${rep_tp},${rep_p},${rep_n},${rep_tn},${used_ments})"

actual_negatives=`sqlite3 $file "SELECT negatives FROM (SELECT COUNT(*) AS negatives,freq FROM (SELECT COUNT(*) AS freq,goldID FROM mentions_core GROUP BY goldID) GROUP BY freq) WHERE freq==1"`;
old_actual_negatives=$((TN_core_rep+FP_core_rep));
system_negatives_lab=`sqlite3 $file "SELECT negatives FROM (SELECT COUNT(*) AS negatives,freq FROM (SELECT COUNT(*) AS freq,label  FROM mentions_core GROUP BY label ) GROUP BY freq) WHERE freq==1"`;
old_system_negatives_lab=$((TN_core_lab+FN_core_lab));
system_negatives_rep=`sqlite3 $file "SELECT negatives FROM (SELECT COUNT(*) AS negatives,freq FROM (SELECT COUNT(*) AS freq,repID  FROM mentions_core GROUP BY repID ) GROUP BY freq) WHERE freq==1"`;
old_system_negatives_rep=$((TN_core_rep+FN_core_rep));

#echo oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo
#echo actual negatives $actual_negatives $old_actual_negatives
#echo system negatives label $system_negatives_lab $old_system_negatives_lab
#echo system negatives repID $system_negatives_rep $old_system_negatives_rep
#echo oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo

echo --------------------------------------------------------------------------------------------------
echo MAPPING-BASED NONDUPLICATE mention-based performance including identical representations
echo --------------------------------------------------------------------------------------------------
echo TN ${TN_core_lab}
echo N  $((TN_core_lab+FN_core_lab))
echo T  $((TN_core_lab+FP_core_lab))
echo --------------------------------------------------------------------------------------------------
echo Precision `echo "scale=2;100*${TN_core_lab}/${system_negatives_lab}" | bc`
echo Recall    `echo "scale=2;100*${TN_core_lab}/${actual_negatives}" | bc` #TODO: Need to change the denominator to get the cases where N, i.e. the gold dups are empty
echo --------------------------------------------------------------------------------------------------
echo --------------------------------------------------------------------------------------------------
echo MAPPING-BASED NONDUPLICATE baseline using repIDs as labels to show how many mentions share the same repIDs
echo --------------------------------------------------------------------------------------------------
echo TN ${TN_core_rep}
echo N  $((TN_core_rep+FN_core_rep))
echo T  $((TN_core_rep+FP_core_rep))
echo --------------------------------------------------------------------------------------------------
echo Precision `echo "scale=2;100*${TN_core_rep}/${system_negatives_rep}" | bc`
echo Recall    `echo "scale=2;100*${TN_core_rep}/${actual_negatives}" | bc` #TODO: Need to change the denominator to get the cases where N, i.e. the gold dups are empty
echo --------------------------------------------------------------------------------------------------

