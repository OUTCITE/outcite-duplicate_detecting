dir1=code/blocking/;
dir2=code/blocking/;

arg=$1
spechem=$2
mode=$3 #words bigrams ngrams title
        #fields parts ngrams
specmode=$4

echo temporary directory: ${SQLITE_TMPDIR};

#TODO: Need to create a feature permutation based on alphabetical order of equivalent columns as defined in the type definition files, then hash and insert into representations

case $arg in

publications)
    code=${dir1}code/make_representations_v3.py
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
        representations=${dir2}representations_${arg}/${spechem}/representations.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db #for title only use bigrams or words or another existing mentions db
        representations=${dir2}representations_${arg}/representations_${mode}.db
    fi
    types=${dir1}mappings/types/${arg}_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
authors)
    code=${dir1}code/make_representations_v3.py
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
        representations=${dir2}representations_${arg}/${spechem}/representations.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db
        representations=${dir2}representations_${arg}/representations_${mode}.db
    fi
    types=${dir1}mappings/types/${arg}_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
institutions) #TODO: Test
    nice -n 1 python $code $mentions $representations $types;
    #code=${dir1}code/make_representations_v3.py
    #;;
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_institutions_wos/${spechem}/mentions.db
        representations=${dir2}representations_institutions_wos/${spechem}/representations.db
    else
        mentions=${dir2}representations_institutions_wos/mentions_${mode}.db
        representations=${dir2}representations_institutions_wos/representations_${mode}.db
    fi
    types=${dir1}mappings/types/institutions_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
institutions_bfd) #TODO: Test
    #nice -n 1 python $code $mentions $representations $types;
    #;;
    code=${dir1}code/make_representations_v3.py
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_institutions_bfd/${spechem}/mentions.db
        representations=${dir2}representations_institutions_bfd/${spechem}/representations.db
    else
        mentions=${dir2}representations_institutions_bfd/mentions_${mode}.db
        representations=${dir2}representations_institutions_bfd/representations_${mode}.db
    fi
    types=${dir1}mappings/types/institutions_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
outcite)
    code=${dir1}code/make_representations_v3.py
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
        representations=${dir2}representations_${arg}/${spechem}/representations.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db #for title only use bigrams or words or another existing mentions db
        representations=${dir2}representations_${arg}/representations_${mode}.db
    fi
    types=${dir1}mappings/types/${arg}_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
methods)
    code=${dir1}code/make_representations_v3.py
    if [ "$specmode" == "mentions" ]; then
        mentions=${dir2}representations_${arg}/${spechem}/mentions.db
        representations=${dir2}representations_${arg}/${spechem}/representations.db
    else
        mentions=${dir2}representations_${arg}/mentions_${mode}.db #for title only use bigrams or words or another existing mentions db
        representations=${dir2}representations_${arg}/representations_${mode}.db
    fi
    types=${dir1}mappings/types/${arg}_${mode}.txt
    nice -n 1 python $code $mentions $representations $types;
    ;;
esac
