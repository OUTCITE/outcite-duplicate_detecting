#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys
import re
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1];#'references';
_nchar = int(sys.argv[2]);

_delta = 0.001;

_field = 'reference';

_max_extract_time = 10;
_max_scroll_tries = 3;
_scroll_size      = 10;

_ids = [];

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def whitespace_context(index,field,nchar):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_query = { "ids": { "values": _ids } } if _ids else {'match_all':{}};
    #----------------------------------------------------------------------------------------------------------------------------------
    print('------------------->',scr_query);
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query,_source=[field]);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    print('------------------->',page['hits']['total']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if field in doc['_source']:
                refstring = doc['_source'][field];
                refstring = refstring.replace('\n',' ').replace('\r',' ').lower();
                words     = refstring.split();
                if len(words) > 1:
                    for i in range(1,len(words)):
                        yield True, words[i-1][-nchar:], words[i][:nchar], 0,0 #words[i-1], words[i];
                for word in words:
                    for i in range(1,len(word)):
                        yield False, word[:i][-nchar:], word[i:][:nchar], 0,0 #word[:i], word[i:];
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
    client.clear_scroll(scroll_id=sid);

def most_frequent(freq):
    for key1 in LM:
        for key2 in LM[key1][0]:
            if LM[key1][0][key2][1] > 10000:
                print(key1,key2,LM[key1][0][key2][1]); print(LM[key1][0][key2][0]); print('------------------------------------------------');

def prob_sep(word1,word2,LM1,LM2,LM3,SUM1,SUM2,nchar):
    ngram1, ngram2 = word1[-nchar:].lower(), word2[:nchar].lower();
    freq_separate = (LM2[ngram1][1]/SUM2)*(LM3[ngram2]/SUM2) if ngram1 in LM2 and ngram2 in LM3 else 0;
    #freq_follow   = LM2[ngram1][0][ngram2][1] if ngram1 in LM2 and ngram2 in LM2[ngram1][0] else 0;
    freq_together = LM1[ngram1][0][ngram2][1]/SUM1 if ngram1 in LM1 and ngram2 in LM1[ngram1][0] else 0;
    return [freq_separate/(freq_together+freq_separate) if freq_separate>0 else 0, 0.01]; #[freq_separate/(freq_separate+freq_together) if freq_separate>0 else 0, freq_together/(freq_separate+freq_together) if freq_together>0 else 0];

def check_sep(text,LM1,LM2,LM3,SUM1,SUM2,nchar):
    words = text.replace('\n',' ').replace('\r',' ').split();
    if len(words) > 1:
        for i in range(1,len(words)):
            sep,tog  = prob_sep(words[i-1].lower(),words[i].lower(),LM1,LM2,LM3,SUM1,SUM2,nchar);
            decision = 'separate' if sep>tog else 'together' if tog>sep else 'unknown';
            print(words[i-1],words[i],sep,decision)
            yield words[i-1],words[i],decision;

def correct(text,LM1,LM2,LM3,SUM1,SUM2,nchar):
    text_ = '';
    for word1, word2, decision in check_sep(text,LM1,LM2,LM3,SUM1,SUM2,nchar):
        text_ = word1 if not text_ else text_;
        if decision == 'together':
            text_ += word2;
        else:
            text_ += ' '+word2;
    return text_;

def p_wh(w,h,freq_hw,histories,words,histsum,delta):
    f_hw   = freq_hw[h][0][w][1] if h in freq_hw and w in freq_hw[h][0] else 0;
    f_h    = freq_hw[h][1]       if h in freq_hw                        else 0;
    d_w    = len(words[h])     if h in words     else 0;
    d_h    = len(histories[w]) if w in histories else 0;
    gamma  = (delta/f_h)*d_w       if f_h > 0 else 0;
    part1  = max(0,f_hw-delta)/f_h if f_h > 0 else 0;
    part2  = d_h/histsum;
    result = part1 + gamma*part2;
    return result;

def within_vs_across(w,h,within_word_hw,across_word_hw,within_word_h_,across_word_h_,within_word__w,across_word__w,within_word_histsum,across_word_histsum,delta):
    within_word_prob = p_wh(w, h, within_word_hw, within_word_h_, within_word__w, within_word_histsum, delta);
    across_word_prob = p_wh(w, h, across_word_hw, across_word_h_, across_word__w, across_word_histsum, delta);
    score_merge      = within_word_prob/across_word_prob if across_word_prob >0 else 0;
    print(score_merge>2,round(score_merge,2),h,w,within_word_prob,across_word_prob);
    return score_merge;

def contrast(text,within_word_hw,across_word_hw,within_word_h_,across_word_h_,within_word__w,across_word__w,within_word_histsum,across_word_histsum,delta):
    words = text.replace('\n',' ').replace('\r',' ').split();
    text_ = '';
    if len(words) > 1:
        for i in range(1,len(words)):
            text_       = words[i-1] if not text_ else text_;
            h, w        = words[i-1][-_nchar:].lower(), words[i][:_nchar].lower();
            score_merge = within_vs_across(w,h,within_word_hw,across_word_hw,within_word_h_,across_word_h_,within_word__w,across_word__w,within_word_histsum,across_word_histsum,delta);
            if score_merge > 2:
                text_ += words[i];
            else:
                text_ += ' '+words[i];
    return text_;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

i  = 0;
within_word_hw = dict();
across_word_hw = dict();
within_word_w  = dict();
across_word_w  = dict();
within_word_h_  = dict(); # DISTINCT HISTORIES!
across_word_h_  = dict(); # DISTINCT HISTORIES!
within_word__w  = dict(); # DISTINCT HISTORIES!
across_word__w  = dict(); # DISTINCT HISTORIES!
for ACROSS, h, w, history, word in whitespace_context(_index,_field,_nchar):
    freq_hw   = across_word_hw if ACROSS else within_word_hw;
    freq_w    = across_word_w  if ACROSS else within_word_w;
    histories = across_word_h_ if ACROSS else within_word_h_;
    words     = across_word__w if ACROSS else within_word__w;
    i += 1;
    if i%1000000==0:
        print(i,end='\r');
    if h in freq_hw:
        if w in freq_hw[h][0]:
            freq_hw[h][0][w][1] += 1;
        else:
            freq_hw[h][0][w] = [set([]),1];
    else:
        freq_hw[h] = [{w:[set([]),1]},0];
    if w in freq_w:
        freq_w[w] += 1;
    else:
        freq_w[w] = 1;
    freq_hw[h][1] += 1;
    freq_hw[h][0][w][0].add((history,word,));
    if w in histories:
        histories[w].add(h);
    else:
        histories[w] = set([h]);
    if h in words:
        words[h].add(w);
    else:
        words[h] = set([w]);

within_word_histsum = sum([len(within_word_h_[w]) for w in within_word_h_]);
across_word_histsum = sum([len(across_word_h_[w]) for w in across_word_h_]);
SUM1                = sum((within_word_hw[key][1] for key in within_word_hw));
SUM2                = sum((across_word_hw[key][1] for key in across_word_hw));

text = input();
contrast(text,within_word_hw,across_word_hw,within_word_h_,across_word_h_,within_word__w,across_word__w,within_word_histsum,across_word_histsum,_delta);

#TODO: Improve
#-------------------------------------------------------------------------------------------------------------------------------------------------
