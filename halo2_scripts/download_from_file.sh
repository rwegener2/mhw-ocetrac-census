#!/bin/bash

echo "Start!"

# cycle through the 124 filenames in the file, skipping the 2
# header rows
tail -n 124 geopolar_2015_filelist.txt | while read line
do 
    echo "Downloading $line"
    wget "$line"
done
