dir1=code/blocking/;

arg=$1
disk=$2
spechem=$3
scheme=$4
method=$5 #simhash title poset
size=$6
offset=$7

dir2=code/blocking/;

code=${dir1}code/disambiguate_v3.py;

query=database;

echo temporary directory: ${SQLITE_TMPDIR};

#label=`sqlite3 $components "SELECT label FROM (SELECT label,COUNT(*) as freq FROM components GROUP BY label) WHERE freq=${size} LIMIT ${offset},1"`
label=$offset

case $arg in

publications)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/$spechem.json;
    ;;
authors)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/test_authors.json;
    ;;
institutions)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/test_institutions_wos.json;
    ;;
institutions_bfd)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/test_institutions_bfd.json;
    ;;
outcite)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/$spechem.json;
    ;;
methods)
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    pid=${arg}_test;
    configs=${dir1}configs/$spechem.json;
    query=minel; #TODO: Comment out for components instead of minels, but that does not usually make a lot of sense
    ;;
esac

key=${components}'+'${representations}'+'${features}'+'${label}

python $code $query $key $pid $configs
