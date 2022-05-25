#!/bin/bash

echo "Start!"
echo $(date)

# cycle through the 124 filenames in the file and downloads each one
cat geopolar_all_filelist.txt | while read line
do 
    filename="$(basename $line)"
    if [ -f "$filename" ]; then
	echo "$filename exists"
    else
	echo "Downloading $line"
	wget "$line"
    fi
done

echo "Complete"
echo $(date)
