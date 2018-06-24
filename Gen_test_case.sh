#!/bin/bash
trap "exit" INT
i=2
max_view_num=50
view_num_gap=2
query_file=$1
view_file=$2
provenance_file=$3
echo $query_file
echo $view_file
echo $provenance_file
while ((i < max_view_num))
do
	java -jar test_case_agg.jar $query_file $view_file $i
	bash execute_query_provenance.sh $query_file $view_file $provenance_file
	echo view_num::$i
	java -Xmx30720m -jar Prov_reasoning.jar false false $query_file $view_file $provenance_file
	java -Xmx30720m -jar Prov_reasoning.jar true false $query_file $view_file $provenance_file
	java -Xmx30720m -jar Prov_reasoning.jar false true $query_file $view_file $provenance_file
        java -Xmx30720m -jar Prov_reasoning.jar true true $query_file $view_file $provenance_file
	java -jar TLA_agg.jar $view_file $query_file false
	java -jar TLA_agg.jar $view_file $query_file true
	echo finished
	i=$((i + view_num_gap))
done
