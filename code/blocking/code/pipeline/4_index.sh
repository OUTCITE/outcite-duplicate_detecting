dir1=code/blocking/;

arg=$1
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams title
        #fields parts ngrams

dir2=code/blocking/;

code=${dir1}code/index_representations.py

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    types=${dir1}mappings/types/${arg}_${mode}.txt
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db
    ;;
authors)
    types=${dir1}mappings/types/${arg}_${mode}.txt
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db
    ;;
institutions) #TODO: Test
    types=${dir1}mappings/types/institutions_${mode}.txt
    representations=${dir2}representations_institutions_wos/${spechem}/${scheme}/representations.db
    features=${dir2}representations_institutions_wos/${spechem}/${scheme}/features.db
    ;;
institutions_bfd) #TODO: Test
    types=${dir1}mappings/types/institutions_${mode}.txt
    representations=${dir2}representations_institutions_bfd/${spechem}/${scheme}/representations.db
    features=${dir2}representations_institutions_bfd/${spechem}/${scheme}/features.db
    ;;
outcite)
    types=${dir1}mappings/types/${arg}_${mode}.txt
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db
    ;;
methods)
    types=${dir1}mappings/types/${arg}_${mode}.txt
    representations=${dir2}representations_${arg}/${spechem}/${scheme}/representations.db
    features=${dir2}representations_${arg}/${spechem}/${scheme}/features.db
    ;;
esac


nice -n 1 python $code $representations $features $types;
