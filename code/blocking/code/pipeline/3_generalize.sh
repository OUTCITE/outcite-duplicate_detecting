dir1=code/blocking/;

arg=$1
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams title
        #fields parts ngrams

dir2=code/blocking/;
dir3=code/blocking/;

code=${dir1}code/generalize_representations.py

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    representations_before=${dir3}representations_${arg}/${spechem}/representations.db
    representation_folder=${dir2}representations_${arg}/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_${arg}/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
authors)
    representations_before=${dir3}representations_${arg}/${spechem}/representations.db
    representation_folder=${dir2}representations_${arg}/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_${arg}/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
institutions) #TODO: Test
    representations_before=${dir3}representations_institutions_wos/${spechem}/representations.db
    representation_folder=${dir2}representations_institutions_wos/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_institutions/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
institutions_bfd) #TODO: Test
    representations_before=${dir3}representations_institutions_bfd/${spechem}/representations.db
    representation_folder=${dir2}representations_institutions_bfd/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_institutions/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
outcite)
    representations_before=${dir3}representations_${arg}/${spechem}/representations.db
    representation_folder=${dir2}representations_${arg}/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_${arg}/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
methods)
    representations_before=${dir3}representations_${arg}/${spechem}/representations.db
    representation_folder=${dir2}representations_${arg}/${spechem}/${scheme}/
    representations=${representation_folder}representations.db
    types=${dir1}mappings/types/${arg}_${mode}.txt
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    generalizations=${dir1}mappings/generalization_schemes_${arg}/${scheme}.txt
    mkdir -p $representation_folder
    cp $representations_before $representations
    if [ "$scheme" != "no_generalization" ]; then
        nice -n 1 python $code $representations $types $restrictions $generalizations;
    fi
    ;;
esac
