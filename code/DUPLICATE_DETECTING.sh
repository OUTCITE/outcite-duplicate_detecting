DOCINDEX=$1
WRITEBACK=$2

REFINDEX=${DOCINDEX}_references
DUPINDEX=${DOCINDEX}_duplicates

LOGFILE=logs/${DOCINDEX}.out
ERRFILE=logs/${DOCINDEX}.err

cd /home/outcite/duplicate_detecting/

> $LOGFILE
> $ERRFILE

echo "...downloads the features for all extracted references into an SQLite database."
python code/B1_download_features.py $DOCINDEX resources/${DOCINDEX}_features.db >$LOGFILE.1 2>>$ERRFILE;

echo "...transforms the above created database into one that is conforming to the input requirements for the blocking step."
python code/B2_process_features.py resources/${DOCINDEX}_features.db resources/${DOCINDEX}_features_processed.db >$LOGFILE.2 2>>$ERRFILE;

echo "...can be used to add gold or silver identifiers to the already existing column in the above created database."
python code/B3_add_goldID.py resources/${DOCINDEX}_features.db resources/${DOCINDEX}_features_processed.db >$LOGFILE.3 2>>$ERRFILE;

echo "...runs the blocking code"
bash code/blocking/code/pipeline/OUTCITE.sh ${DOCINDEX}_features_processed.db ${DUPINDEX}.db >$LOGFILE.4 2>>$ERRFILE;

echo "...takes the duplicate block identifiers from the duplicate block mapping that has been copied to the OUTCITE server and adds them to the respective references in the OUTCITE SSOAR index."
python code/0_update_blockIDs.py $DOCINDEX resources/${DUPINDEX}.db >$LOGFILE.5 2>>$ERRFILE;

echo "...creates a new index called ‘references’ where the references will be stored as documents on the top level."
cd /home/outcite/refextract/
python code/M_create_index.py $REFINDEX >$LOGFILE.6 2>>$ERRFILE;
cd /home/outcite/duplicate_detecting/

echo "...writes all references extracted in the OUTCITE SSOAR index into the above created index."
python code/1_index_references.py $DOCINDEX $REFINDEX >$LOGFILE.7 2>>$ERRFILE;

echo "...clusters the duplicate blocks using clustering techniques. The resulting identifier is called ‘cluster_id’ and stored with the reference in the OUTCITE references index."
python code/2_update_clusterIDs.py $REFINDEX >$LOGFILE.8 2>>$ERRFILE;

echo "...Does a pairwise classification of the above created clusters to determine pairs that seem impossible to be duplicates and then applies the transitive closure of the new duplicate-relation"
echo "   Note that the closure may re-establish duplicate relationships by transitivity that have just been broken up."
echo "   The resulting identifier is called ‘duplicate_id’ and stored with the reference in the OUTCITE references index."
python code/3_update_duplicateIDs.py $REFINDEX >$LOGFILE.9 2>>$ERRFILE;

echo "...Creates a new index called ‘duplicates’ that stores information such as the canonical values used for different reference fields as well as the original values."
echo "...from the individual extracted references and the corresponding identifiers of these individual extracted references."
cd /home/outcite/refextract/
python code/M_create_index.py $DUPINDEX >$LOGFILE.10 2>>$ERRFILE;
cd /home/outcite/duplicate_detecting/

echo "...creates an entry for all references that have the same duplicate_id and stores it in the above created index."
python code/4_index_duplicates.py $REFINDEX $DUPINDEX >$LOGFILE.11 2>>$ERRFILE;

echo "...Replaces the information in the extracted references in the OUTCITE SSOAR index by the canonical values stored in the respective duplicate group’s entry in the OUTCITE duplicates index."
echo "   Also keeps the previous values in fields ending on ‘_original’."
echo "   This is not necessarily applied in practice as the export for other uses can be done directly from the duplicates or references index."
if [ $WRITEBACK == 'writeback' ]; then
    python code/5_update_references.py $DOCINDEX $DUPINDEX >$LOGFILE.12 2>>$ERRFILE;
fi
