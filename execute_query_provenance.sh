#!/bin/bash
trap "exit" INT
i=1
num=0
max_num=10
echo $1
echo $2
echo $3
echo $4
while [ $i -ne 0 ]
do
java -Xmx30720m -jar Query_provenance.jar $1 $2 false $3 $4
i=$?
num=$[$num+1]
echo $num
if [ $num -gt $max_num ];
then
	exit 1
fi
done
