for csvfile in *.csv;
do
    wc -l $csvfile
done

echo "Total Count : $(cat *.csv | wc -l)"