arg=$1 #institutions_bfd institutions_wos
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams title
        #fields parts ngrams
method=$6 #poset simhash single

script=code/blocking/code/pipeline/8_evaluate_components_v2.sh

if [ "$arg" == "publications" ]; then

    for min_gold_size in 1 2 3 4 5 6 7 8 9 10; do
        for max_gold_size in 3; do
            for max_label_size in 1 2 4 8 16 27 40 52 64 125 216 343 512 729 1000; do
                echo                                     $min_gold_size $min_gold_size $max_label_size;
                bash $script $arg $disk $spechem $scheme $min_gold_size $min_gold_size $max_label_size pair $mode $method;
            done;
        done;
    done
    #bash $script $arg $disk $spechem $scheme 1 10000 1000000 core $mode $method;

elif [ "$arg" == "authors" ]; then

    for min_gold_size in 1 2 3 4 5 6 7 8 9 10; do
        for max_gold_size in 3; do
            for max_label_size in 20000 40000 80000 160000 270000 400000 520000 640000 1250000 2160000 3430000 5120000 7290000 10000000; do #1 2 4 8 16 27 40 52 64 125 216 343 512 729 1000 2000 3000 4000 5000 7500 10000; do
                echo                                     $min_gold_size $min_gold_size $max_label_size;
                bash $script $arg $disk $spechem $scheme $min_gold_size $min_gold_size $max_label_size pair $mode $method;
            done;
        done;
    done
    for min_gold_size in 10 20 40 80 160 320 640 1280; do
        max_gold_size=$(( 2*min_gold_size - 1 ));
        for max_label_size in 20000 40000 80000 160000 270000 400000 520000 640000 1250000 2160000 3430000 5120000 7290000 10000000; do #1 2 4 8 16 27 40 52 64 125 216 343 512 729 1000 2000 3000 4000 5000 7500 10000; do
            echo                                     $min_gold_size $max_gold_size $max_label_size;
            bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
        done;
    done

elif [ "$arg" == "institutions_bfd" ]; then

    for min_gold_size in 10 40 160 640 2560 10240 40960 163840; do
        max_gold_size=$(( 4*min_gold_size - 1 ));
        for max_label_size in 1 2 4 8 16 27 40 52 64 125 216 343 512 729 1000 2000 3000 4000 5000 7500 10000; do
            echo                                     $min_gold_size $max_gold_size $max_label_size;
            bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
        done;
    done

else

    for min_gold_size in 1 2; do
        for max_gold_size in 10000000; do
            for max_label_size in 100000 1000000 10000000 100000000; do
                bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
            done;
        done;
    done

fi
