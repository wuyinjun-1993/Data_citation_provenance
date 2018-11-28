package edu.upenn.cis.citation.covering_sets_calculation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view0.Covering_set;

public class Covering_sets_no_clustering {
  
  public static HashSet<Covering_set> reasoning_single_tuple(ArrayList<Tuple>[] head_variable_view_mapping, HashSet views, HashMap<Tuple, ArrayList<Integer>> view_mapping_query_arg_ids_mappings)
  {
      
//  ArrayList<citation_view_vector> view_com = new ArrayList<citation_view_vector>();
      
      HashSet<Covering_set> all_covering_sets = new HashSet<Covering_set>();
      
      for(int i = 0; i < head_variable_view_mapping.length; i ++)
      {
        if(head_variable_view_mapping[i] == null)
          continue;
        
        
        ArrayList<Tuple> curr_tuples = (ArrayList<Tuple>) head_variable_view_mapping[i].clone();
        
        curr_tuples.retainAll(views);
        
//        System.out.println(i + "::" + curr_tuples);
        
        all_covering_sets = Join_covering_sets.join_views_curr_relation(curr_tuples, all_covering_sets, view_mapping_query_arg_ids_mappings);

      }
              
//      c_view_template.addAll(all_covering_sets);
      
      return all_covering_sets;
      
  }

}
