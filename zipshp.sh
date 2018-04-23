for i in /*; do
    cd $i 
    echo $i
    for k in $i/*; do
       	zip -m -j -r $k.zip $k
	echo $k;
	rm -r $k
    done
done

