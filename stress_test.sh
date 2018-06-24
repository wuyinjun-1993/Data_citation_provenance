#!/bin/bash
query_size=3;
query_grouping_attr_num=3;
query_agg_attr_num=3;
view_init_size=10
false_str="false";
true_str="true";
instance_size=200000;
batch_size=5;
java -Xmx30720m -jar Prov_reasoning.jar $true_str $batch_size
for view_offset in 10 15 20 25 30 35 40 45
do
	view_num=5;
	echo $view_num
	echo $batch_size
	java -jar Query_view_generator.jar $false_str $query_size $query_grouping_attr_num $query_agg_attr_num $view_num $view_offset $instance_size
	java -Xmx30720m -jar Prov_reasoning.jar $true_str $batch_size
done

