dir1=code/blocking/;
dir2=code/blocking/;

script=${dir1}code/apply_components_v3.py;
script2=${dir1}code/core_goldstandard.py;

arg=$1
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams
        #fields parts ngrams
specmode=$6
method=$7 #poset simhash single

only_labelled=1
integer_id=1

dir3=code/blocking/;

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    folder=representations_${arg}/;
    core_gold=${dir1}data/deduplication_dataset_2020/Ground_Truth_data.jsonl;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db
    fi
    representations=${dir3}${folder}${spechem}/${scheme}/representations.db;
    features=${dir3}${folder}${spechem}/${scheme}/features.db;
    components=${dir3}${folder}${spechem}/${scheme}/components_${method}.db;
    output=${dir3}${folder}${spechem}/${scheme}/labelled_mentions_${method}.db;
    python $script2 $core_gold $mentions $representations $features $components $output;
    ;;
authors)
    folder=representations_${arg}/;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db
    fi
    representations=${dir3}${folder}${spechem}/${scheme}/representations.db;
    features=${dir3}${folder}${spechem}/${scheme}/features.db;
    components=${dir3}${folder}${spechem}/${scheme}/components_${method}.db;
    output=${dir3}${folder}${spechem}/${scheme}/labelled_mentions_${method}.db;
    ;;
institutions) #TODO: Test
    folder=representations_${arg}/;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}${folder}${spechem}/mentions.db
    else
        mentions=${dir2}${folder}mentions_${mode}.db
    fi
    representations=${dir3}${folder}${spechem}/${scheme}/representations.db;
    features=${dir3}${folder}${spechem}/${scheme}/features.db;
    components=${dir3}${folder}${spechem}/${scheme}/components_${method}.db;
    output=${dir3}${folder}${spechem}/${scheme}/labelled_mentions_${method}.db;
    ;;
institutions_bfd) #TODO: Test
    folder=representations_${arg}/;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}${folder}${spechem}/mentions.db
    else
        mentions=${dir2}${folder}mentions_${mode}.db
    fi
    representations=${dir3}${folder}${spechem}/${scheme}/representations.db;
    features=${dir3}${folder}${spechem}/${scheme}/features.db;
    components=${dir3}${folder}${spechem}/${scheme}/components_${method}.db;
    output=${dir3}${folder}${spechem}/${scheme}/labelled_mentions_${method}.db;
    ;;
outcite)
    folder=representations_${arg}/;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db
    fi
    representations=${dir3}${folder}${spechem}/${scheme}/representations.db;
    features=${dir3}${folder}${spechem}/${scheme}/features.db;
    components=${dir3}${folder}${spechem}/${scheme}/components_${method}.db;
    output=${dir3}${folder}${spechem}/${scheme}/labelled_mentions_${method}.db;
    only_labelled=0
    integer_id=0
    ;;
esac

python $script $mentions $components $features $representations $output $only_labelled $integer_id;
