arg=$1 #institutions_bfd institutions_wos
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams title
        #fields parts ngrams
method=$6 #poset simhash single

script=code/blocking/code/pipeline/8_evaluate_components_v2.sh

if [ "$arg" == "publications" ]; then

    for min_gold_size in 1 2; do
        for max_gold_size in 10 25 50 100 1000 10000; do
            for max_label_size in 10 100 1000 10000 100000 1000000; do
                bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
            done;
        done;
    done
    bash $script $arg $disk $spechem $scheme 1 10000 1000000 core $mode $method;

else

    for min_gold_size in 1 2; do
        for max_gold_size in 10000000; do
            for max_label_size in 100000 1000000 10000000 100000000; do
                bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
            done;
        done;
    done

fi
