dir1=code/blocking/;

arg=$1
disk=$2
spechem=$3
scheme=$4
method=$5 #poset simhash single

dir2=code/blocking/;

if [ "$method" == "simhash" ]; then
    code=${dir1}code/simhash_components_disk_v3.py
elif [ "$method" == "single" ]; then
    code=${dir1}code/pipeline/5a_singlesep.sh
elif [ "$method" == "poset" ]; then
    code=${dir1}code/find_components_disk.py
fi

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${scheme}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
    ;;
authors)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${scheme}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
    ;;
institutions)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
    ;;
institutions_bfd)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
    ;;
outcite)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
    ;;
methods)
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db;
    report=${dir1}report_${arg}_${spechem}_${scheme}.db
    components=${dir2}representations_${arg}/${spechem}/${scheme}/components_${method}.db
esac

if [ "$method" == "single" ]; then
    nice -n 1 bash $code $features $report $components;
else
    nice -n 1 python $code $features $report $components;
fi

if [ "$method" == "poset" ]; then
    python ${dir1}code/analyse_report.py $report;
fi
