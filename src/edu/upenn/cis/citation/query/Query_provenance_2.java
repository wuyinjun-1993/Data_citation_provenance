package edu.upenn.cis.citation.query;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.multi_thread.Loading_base_relations;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class Query_provenance_2 {
  
  
  
  
  public static ResultSet get_query_provenance(Query query, boolean test_case, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = null;
    
    if(!test_case)
      sql = Query_converter.data2sql_with_provenance_col(query);
    else
      sql = Query_converter.data2sql_with_provenance_col_test(query);
    
    System.out.println(sql);
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    return rs;
    
  }
  
  public static ResultSet get_query_provenance(Single_view view, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = null;
    
    sql = Query_converter.data2sql_with_provenance_col(view);
    
    pst = c.prepareStatement(sql);
    
//    System.out.println(sql);
    
    ResultSet rs = pst.executeQuery();
    
    return rs;
    
  }
  
  public static ResultSet get_query_provenance_with_condition(Single_view view, String condition_string, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = null;
    
    sql = Query_converter.data2sql_with_provenance_col_with_condition_string(view, condition_string);//(view);
    
    pst = c.prepareStatement(sql);
    
//    System.out.println(sql);
    
    ResultSet rs = pst.executeQuery();
    
    return rs;
    
  }
  
  public static ResultSet get_query_provenance_materialized(Single_view view, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = null;
    
    sql = Query_converter.data2sql_with_provenance_col_materialized(view);
    
    pst = c.prepareStatement(sql);
    
//    System.out.println(sql);
    
    ResultSet rs = pst.executeQuery();
    
    return rs;
    
  }

  public static void retrieve_query_instance_provenance(ResultSet rs, Query query, HashMap<Head_strs, String[][]> group_value_prov_mappings, HashMap<Head_strs, Head_strs> query_instance, HashMap<String, HashMap<Head_strs, HashSet<Integer>>>[] query_prov_index) throws SQLException
  {
    
    while(rs.next())
    {
      Vector<String> grouping_values = new Vector<String>();
      for(int i = 0; i<query.head.args.size(); i++)
      {
        grouping_values.add(rs.getString(i+1));
      }
      
      Head_strs grouping_value_strings = new Head_strs(grouping_values);
      
      int start_pos = query.head.args.size();

      Array array = rs.getArray(start_pos + 1);
      
      
      String[][] prov_lists = (String[][]) array.getArray();
      
      group_value_prov_mappings.put(grouping_value_strings, prov_lists);

      Build_query_prov_index.build_index_for_query_prov(query, query_prov_index, grouping_value_strings, prov_lists);
      
      if(query.head.has_agg)
      {
                
        start_pos = query.head.args.size() + 1;
        
        Vector<String> agg_results = new Vector<String>(); 
        
        for(int i = start_pos; i<start_pos + query.head.agg_args.size(); i++)
        {
          agg_results.add(rs.getString(i+1));
        }
        
        Head_strs agg_res = new Head_strs(agg_results);
        
        query_instance.put(grouping_value_strings, agg_res);
      }
      else
      {
        query_instance.put(grouping_value_strings, null);
      }
    }
  }
  
  public static void retrieve_query_instance_provenance2(ResultSet rs, Query query, HashMap<Head_strs, Integer> group_value_prov_count_mappings, HashMap<Head_strs, String[][]> group_value_prov_mappings, HashMap<Head_strs, Head_strs> query_instance, HashMap<String, HashSet<Integer>>[] query_prov_index) throws SQLException
  {
    
    while(rs.next())
    {
      Vector<String> grouping_values = new Vector<String>();
      for(int i = 0; i<query.head.args.size(); i++)
      {
        grouping_values.add(rs.getString(i+1));
      }
      
      Head_strs grouping_value_strings = new Head_strs(grouping_values);
      
      int start_pos = query.head.args.size();

      Array array = rs.getArray(start_pos + 1);
      
      
      String[][] prov_lists = (String[][]) array.getArray();
      
      group_value_prov_mappings.put(grouping_value_strings, prov_lists);

      Build_query_prov_index.build_index_for_query_prov2(query, query_prov_index, grouping_value_strings, prov_lists);
      
      group_value_prov_count_mappings.put(grouping_value_strings, prov_lists.length);
      
      if(query.head.has_agg)
      {
                
        start_pos = query.head.args.size() + 1;
        
        Vector<String> agg_results = new Vector<String>(); 
        
        for(int i = start_pos; i<start_pos + query.head.agg_args.size(); i++)
        {
          agg_results.add(rs.getString(i+1));
        }
        
        Head_strs agg_res = new Head_strs(agg_results);
        
        query_instance.put(grouping_value_strings, agg_res);
        
        
      }
      else
      {
        query_instance.put(grouping_value_strings, null);
      }
    }
  }
  
  public static void retrieve_query_instance_provenance3(ResultSet rs, Query query, HashMap<String, Integer> group_value_prov_count_mappings, HashMap<String, String[][]> group_value_prov_mappings, HashMap<String, Head_strs> query_instance, HashMap<String, HashMap<String, long[]>>[] query_prov_index, HashMap<String, Integer> query_group_value_initial_count) throws SQLException
//  public static void retrieve_query_instance_provenance3(ResultSet rs, Query query, HashMap<Head_strs, Integer> group_value_prov_count_mappings, HashMap<Head_strs, String[][]> group_value_prov_mappings, HashMap<Head_strs, Head_strs> query_instance, HashMap<String, long[]>[] query_prov_index, HashMap<Head_strs, Integer> query_group_value_initial_count) throws SQLException
  {
    
    StringBuilder stringbuilder = new StringBuilder();
    
    double index_time = 0;
    
    while(rs.next())
    {
      String grouping_value_strings = new String();
//      Vector<String> grouping_values = new Vector<String>();
      for(int i = 0; i<query.head.args.size(); i++)
      {
        if(i >= 1)
          grouping_value_strings = Head_strs.concatenate_strings(stringbuilder, grouping_value_strings, rs.getString(i+1));
        else
          grouping_value_strings = rs.getString(i+1);
      }
      
//      Head_strs grouping_value_strings = new Head_strs(grouping_values);
      
      query_group_value_initial_count.put(grouping_value_strings, 0);
      
      int start_pos = query.head.args.size();

      Array array = rs.getArray(start_pos + 1);
      
      
      String[][] prov_lists = (String[][]) array.getArray();
      
      group_value_prov_mappings.put(grouping_value_strings, prov_lists);

      double t1 = System.nanoTime();
      
      Build_query_prov_index.build_index_for_query_prov3(stringbuilder, query, query_prov_index, grouping_value_strings, prov_lists);
      
      double t2 = System.nanoTime();
      
      index_time += (t2 - t1);
      
      group_value_prov_count_mappings.put(grouping_value_strings, prov_lists.length);
      
      if(query.head.has_agg)
      {
                
        start_pos = query.head.args.size() + 1;
        
        Vector<String> agg_results = new Vector<String>(); 
        
        for(int i = start_pos; i<start_pos + query.head.agg_args.size(); i++)
        {
          agg_results.add(rs.getString(i+1));
        }
        
        Head_strs agg_res = new Head_strs(agg_results);
        
        query_instance.put(grouping_value_strings, agg_res);
        
        
      }
      else
      {
        query_instance.put(grouping_value_strings, null);
      }
    }
    
    index_time = index_time * 1.0/1000000000;
    
    System.out.println(index_time);
  }

  public static void retrieve_query_instance_provenance4(ResultSet rs, Query query, HashMap<Head_strs, Integer> group_value_prov_count_mappings, HashMap<Head_strs, String[][]> group_value_prov_mappings, HashMap<Head_strs, Head_strs> query_instance, Vector<String>[] query_provs, Vector<long[]>[] indexes) throws SQLException
  {
    
    int prov_length = 0;
    
    while(rs.next())
    {
      Vector<String> grouping_values = new Vector<String>();
      for(int i = 0; i<query.head.args.size(); i++)
      {
        grouping_values.add(rs.getString(i+1));
      }
      
      Head_strs grouping_value_strings = new Head_strs(grouping_values);
      
      int start_pos = query.head.args.size();

      Array array = rs.getArray(start_pos + 1);
      
      
      String[][] prov_lists = (String[][]) array.getArray();
      
      prov_length += prov_lists.length;
      
      group_value_prov_mappings.put(grouping_value_strings, prov_lists);

      group_value_prov_count_mappings.put(grouping_value_strings, prov_lists.length);
      
      if(query.head.has_agg)
      {
                
        start_pos = query.head.args.size() + 1;
        
        Vector<String> agg_results = new Vector<String>(); 
        
        for(int i = start_pos; i<start_pos + query.head.agg_args.size(); i++)
        {
          agg_results.add(rs.getString(i+1));
        }
        
        Head_strs agg_res = new Head_strs(agg_results);
        
        query_instance.put(grouping_value_strings, agg_res);
        
        
      }
      else
      {
        query_instance.put(grouping_value_strings, null);
      }
    }
    
    System.out.println("query_done");
    
    double time1 = System.nanoTime();
    
    Build_query_prov_index.build_index_for_query_prov4(query, query_provs, indexes, group_value_prov_mappings, prov_length);
    
    double time2 = System.nanoTime();

    double time = (time2 - time1)*1.0/1000000000;
    
    System.out.println("time::" + time);
    
    System.out.println(query_provs[0].size());
    System.out.println(indexes[0].size());
  }

  
  public static void retrieve_query_instance(ResultSet rs, Query query, HashMap<String, Head_strs> query_instance) throws SQLException
  {
    
    StringBuilder stringbuilder = new StringBuilder();
    
    while(rs.next())
    {
      String grouping_value_strings = new String();
//      Vector<String> grouping_values = new Vector<String>();
      for(int i = 0; i<query.head.args.size(); i++)
      {
        if(i >= 1)
          grouping_value_strings = Head_strs.concatenate_strings(stringbuilder, grouping_value_strings, rs.getString(i+1));
        else
          grouping_value_strings = rs.getString(i+1);
        
//        grouping_values.add(rs.getString(i+1));
      }
      
//      Head_strs grouping_value_strings = new Head_strs(grouping_values);
      
      int start_pos = query.head.args.size();

//      Array array = rs.getArray(start_pos + 1);
//      
//      
//      String[][] prov_lists = (String[][]) array.getArray();
//      
//      group_value_prov_mappings.put(grouping_value_strings, prov_lists);

      
      if(query.head.has_agg)
      {
                
//        start_pos = query.head.args.size() + 1;
        
        Vector<String> agg_results = new Vector<String>(); 
        
        for(int i = start_pos; i<start_pos + query.head.agg_args.size(); i++)
        {
          agg_results.add(rs.getString(i+1));
        }
        
        Head_strs agg_res = new Head_strs(agg_results);
        
        query_instance.put(grouping_value_strings, agg_res);
      }
      else
      {
        query_instance.put(grouping_value_strings, null);
      }
    }
  }
  
  public static HashMap<String, String[]> construct_base_relation_sets(String sql, Subgoal subgoal, Set<String> prov_sets, Connection c, PreparedStatement pst) throws SQLException
  {
    pst = c.prepareStatement(sql);
    
    pst.setFetchSize(10000);
    
    
    System.out.println(sql);
    
    ResultSet rs = pst.executeQuery();
    
    HashMap<String, String[]> base_relation_content = new HashMap<String, String[]>();
    
    while(rs.next())
    {
      String prov = rs.getString(subgoal.args.size() + 1);
      
      if(prov_sets.contains(prov))
      {
        String[] tuple = new String[subgoal.args.size()];
        
        for(int i = 0; i<tuple.length; i++)
        {
          tuple[i] = rs.getString(i + 1);
        }
        
        base_relation_content.put(prov, tuple);
      }
    }
    
    return base_relation_content;
  }
  
  public static HashMap<String, String[]>[] retrieve_base_relations(String db_name, HashMap<String, HashMap<String, long[]>>[] query_prov_index, Query query, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    
    
    HashMap<String, String[]>[] base_relation_content = new HashMap[query_prov_index.length];
    
//    Vector<Loading_base_relations> threads = new Vector<Loading_base_relations>();
    
    for(int i = 0; i<query_prov_index.length; i++)
    {
//      Loading_base_relations thread = new Loading_base_relations(db_name, query_prov_index[i], query, i);
//      
//      
//      thread.start();
//      
//      threads.add(thread);
      
      StringBuilder sb = new StringBuilder();
      
      Set<String> curr_query_prov_sets = query_prov_index[i].keySet();
      
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      String sql = Query_converter.construct_query_base_relations(sb, curr_query_prov_sets, subgoal, query.subgoal_name_mapping.get(subgoal.name));
      
      base_relation_content[i] = construct_base_relation_sets(sql, subgoal, curr_query_prov_sets, c, pst);
    }
    
//    for(int i = 0; i<threads.size(); i++)
//    {
//      threads.get(i).join();
//    }
//    
//    for(int i = 0; i<threads.size(); i++)
//    {
//      base_relation_content[i] = threads.get(i).retrieve_base_relation_content();
//    }
    
    return base_relation_content;
  }
  
  public static HashMap<String, String[]>[] retrieve_base_relations_multi_thread(String db_name, HashMap<String, HashMap<String, long[]>>[] query_prov_index, Query query, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    StringBuilder sb = new StringBuilder();
    
    HashMap<String, String[]>[] base_relation_content = new HashMap[query_prov_index.length];
    
    Vector<Loading_base_relations> threads = new Vector<Loading_base_relations>();
    
    for(int i = 0; i<query_prov_index.length; i++)
    {
      Loading_base_relations thread = new Loading_base_relations(db_name, query_prov_index[i], query, i);
      
      
      thread.start();
      
      threads.add(thread);
      
//      Set<String> curr_query_prov_sets = query_prov_index[i].keySet();
//      
//      Subgoal subgoal = (Subgoal) query.body.get(i);
//      
//      String sql = Query_converter.construct_query_base_relations(sb, curr_query_prov_sets, subgoal, query.subgoal_name_mapping.get(subgoal.name));
//      
//      base_relation_content[i] = construct_base_relation_sets(sql, subgoal, curr_query_prov_sets, c, pst);
    }
    
    for(int i = 0; i<threads.size(); i++)
    {
      threads.get(i).join();
    }
    
    for(int i = 0; i<threads.size(); i++)
    {
      base_relation_content[i] = threads.get(i).retrieve_base_relation_content();
    }
    
    return base_relation_content;
  }
  
}
