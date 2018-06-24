#!/bin/bash
trap "exit" INT
num=0
max_num=10
echo $1
echo $2
echo $3
echo $4
query_subgoal_num=3
query_head_var_per_subgoal_num=1
query_head_agg_var_per_subgoal_num=1
view_num=10
for i in {1..20}
do
    query_instance_size=$((i*10000))
	echo query_instance_size::$query_instance_size
	
	if (( $i > 1))
	then
		bash execute_query_provenance.sh $1 $2 $3 $4
	fi

	for j in {1..20}
	do
		view_instance_size=$((j*10000))
		echo view_instance_size::$view_instance_size
		if (( $i == 1 && $j == 1  ))
		then
			java -jar Query_view_generator.jar $3 true $query_subgoal_num $query_head_var_per_subgoal_num $query_head_agg_var_per_subgoal_num $view_num 0 $query_instance_size $view_instance_size	
			bash execute_query_provenance.sh $1 $2 $3 $4
		else
			java -jar Query_view_generator.jar $3 false $query_subgoal_num $query_head_var_per_subgoal_num $query_head_agg_var_per_subgoal_num $view_num 0 $query_instance_size $view_instance_size
		fi

		echo materialized::false
		java -Xmx30720m -jar Prov_reasoning.jar true true false $3 $1 $2 $4
		echo materialized::true
		java -Xmx30720m -jar Prov_reasoning.jar true true true $3 $1 $2 $4

	done
done
