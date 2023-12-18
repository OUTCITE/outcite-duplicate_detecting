feat_file=$1
dup_file=$2

from_path=/home/outcite/duplicate_detecting/resources/
input_folder=code/blocking/representations_outcite/
rep_folder=code/blocking/representations_outcite/
code_folder=code/blocking/code/pipeline/

arg=outcite
disk=02
specmode=mentions
spechem=restrictions_outcite_words_goldIDs #restrictions_outcite_words
mode=words_goldIDs #words # bigrams ngrams title
scheme=only_matchID #no_generalization # 8281_3021_4441_0_20
method=poset

echo rsync-ing features...
rsync -c ${from_path}${feat_file} ${input_folder}/mentions_${mode}.db

if [ "$specmode" == "mentions" ]; then
    echo ${code_folder}1a_specify_mentions.sh $arg $spechem $mode
    bash ${code_folder}1a_specify_mentions.sh $arg $spechem $mode
fi

wait;

echo ${code_folder}1_represent.sh $arg $spechem $mode $specmode
bash ${code_folder}1_represent.sh $arg $spechem $mode $specmode

wait;

if [ "$specmode" == "mentions" ]; then
    echo 'Have already specified mentions'
else
    echo ${code_folder}2_specify.sh $arg $spechem $mode
    bash ${code_folder}2_specify.sh $arg $spechem $mode
fi

wait;

echo ${code_folder}3_generalize.sh $arg $disk $spechem $scheme $mode
bash ${code_folder}3_generalize.sh $arg $disk $spechem $scheme $mode

wait;

echo ${code_folder}4_index.sh $arg $disk $spechem $scheme $mode
bash ${code_folder}4_index.sh $arg $disk $spechem $scheme $mode

wait;

echo ${code_folder}5_separate.sh $arg $disk $spechem $scheme $method
bash ${code_folder}5_separate.sh $arg $disk $spechem $scheme $method

wait;

echo ${code_folder}7_prepare_evaluation.sh $arg $disk $spechem $scheme $mode $specmode $method
bash ${code_folder}7_prepare_evaluation.sh $arg $disk $spechem $scheme $mode $specmode $method

wait;

echo ${code_folder}8a_evaluate_all.sh $arg $disk $spechem $scheme $mode $method
bash ${code_folder}8a_evaluate_all.sh $arg $disk $spechem $scheme $mode $method

wait;

echo scp-ing result...
scp ${rep_folder}${spechem}/${scheme}/labelled_mentions_${method}.db /home/outcite/duplicate_detecting/resources/${dup_file}
