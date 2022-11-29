> /home/outcite/duplicate_detecting/resources/training_all.txt;

for file in /home/outcite/duplicate_detecting/resources/training/*.txt; do
    (cat "${file}"; echo) >> /home/outcite/duplicate_detecting/resources/training_all.txt;
done
