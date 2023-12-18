dir1=code/blocking/;
dir2=code/blocking/;

code=${dir1}code/evaluate_minels_v3.py

arg=$1;

case $arg in

publications)
    components=${dir1}components_publications.db
    representations=${dir2}representations_publications/representations.db;
    features=${dir2}representations_publications/features.db;
    ;;
authors)
    components=${dir1}components_authors.db
    representations=${dir2}representations_authors/representations.db;
    features=${dir2}representations_authors/features.db;
    ;;
institutions_bfd)
    components=${dir2}representations_institutions_bfd/restrictions_institutions/generalization_scheme_1/components.db
    representations=${dir2}representations_institutions_bfd/restrictions_institutions/generalization_scheme_1/representations.db;
    features=${dir2}representations_institutions_bfd/restrictions_institutions/generalization_scheme_1/features.db;
    ;;
esac

python $code $components $representations $features;
