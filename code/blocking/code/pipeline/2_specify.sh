dir1=code/blocking/;

arg=$1
spechem=$2
mode=$3 #words bigrams ngrams title
        #fields parts ngrams

dir2=code/blocking/;
code=${dir1}code/specify_representations.py

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    specification_folder=${dir2}representations_${arg}/${spechem}/
    representations_before=${dir2}representations_${arg}/representations_${mode}.db
    representations=${specification_folder}representations.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python $code $representations $restrictions;
    ;;
authors)
    specification_folder=${dir2}representations_${arg}/${spechem}/
    representations_before=${dir2}representations_${arg}/representations_${mode}.db
    representations=${dir2}representations_${arg}/${spechem}/representations.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python $code $representations $restrictions;
    ;;
institutions) #TODO: Test
    specification_folder=${dir2}representations_institutions_wos/${spechem}/
    representations_before=${dir2}representations_institutions_wos/representations_${mode}.db
    representations=${dir2}representations_institutions_wos/${spechem}/representations.db
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    if [ "$spechem" = "restrictions_institutions_fields_threshold" ]; then
        code=${dir1}code/specify_representations_institutions.py
    fi
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python $code $representations $restrictions;
    ;;
institutions_bfd) #TODO: Test
    specification_folder=${dir2}representations_institutions_bfd/${spechem}/
    representations_before=${dir2}representations_institutions_bfd/representations_${mode}.db
    representations=${dir2}representations_institutions_bfd/${spechem}/representations.db
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    if [ "$spechem" = "restrictions_institutions_fields_threshold" ]; then
        code=${dir1}code/specify_representations_institutions.py
    fi
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python -i $code $representations $restrictions;
    ;;
outcite)
    specification_folder=${dir2}representations_${arg}/${spechem}/
    representations_before=${dir2}representations_${arg}/representations_${mode}.db
    representations=${specification_folder}representations.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python $code $representations $restrictions;
    ;;
methods)
    specification_folder=${dir2}representations_${arg}/${spechem}/
    representations_before=${dir2}representations_${arg}/representations_${mode}.db
    representations=${specification_folder}representations.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $representations_before $representations;
    nice -n 1 python $code $representations $restrictions;
    ;;
esac
