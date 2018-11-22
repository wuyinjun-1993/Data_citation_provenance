package edu.upenn.cis.citation.single_thread;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.query.Build_query_prov_index;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.query.Query_provenance_2;
import edu.upenn.cis.citation.util.Binary_search;
import edu.upenn.cis.citation.util.Bit_operation;
import edu.upenn.cis.citation.views.Materialized_view_query_generator;
import edu.upenn.cis.citation.views.Query_converter2;
import edu.upenn.cis.citation.views.Single_view;
import edu.upenn.cis.citation.views.Virtual_view_query_generator;

public class Single_thread{
//  private Thread t;
//  private String threadName;
//  public Single_view view;
//  
//  public HashSet<Tuple> view_mappings;
//  
//  public ArrayList<Vector<Head_strs>> values_from_why_tokens;
//  
////  public HashMap<Tuple, long[]> tuple_rows_bit_index = new HashMap<Tuple, long[]>();
//  
//  public HashMap<Tuple, Set> tuple_rows = new HashMap<Tuple, Set>();
//  
////  public HashMap<String, long[]>[] query_prov_index;
//  
//  public Vector<String>[] query_prov_lists;
//  
//  public Vector<long[]>[] prov_indexes;
//  
//  public HashMap<String, long[]>[] query_prov_index_lists;
//  
//  public HashMap<String, Integer> query_grouping_value_initial_count; 
//
//  public Query query = null;
//  
//  public Connection c;
//  
//  public PreparedStatement pst;
//  
//  public HashMap<String, String[][]> query_prov_instance;
//  
//  public HashMap<String, Integer> grouping_value_prov_count_mappings;
//  HashMap<String, String> subgoal_name_mappings;
  
//  public Single_thread(String db_name, String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, String[][]> query_prov_instance, Vector<String>[] query_prov_lists, Vector<long[]>[] query_prov_indexes, Query query, HashMap<String, Integer> grouping_value_prov_count_mappings) throws ClassNotFoundException, SQLException {
//     threadName = name;
//
//     this.view = view;
//     
//     this.view_mappings = view_mappings;
//     
//     this.values_from_why_tokens = curr_tuples;
//     
////     this.query_prov_index = query_prov_index;
//     
//     this.query_prov_lists = query_prov_lists;
//     
//     this.prov_indexes = query_prov_indexes;
//     
//     this.query_prov_instance = query_prov_instance;
//
//     this.query = query;
//     
//     this.grouping_value_prov_count_mappings = grouping_value_prov_count_mappings;
//     
//     Class.forName("org.postgresql.Driver");
//     this.c = DriverManager
//         .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
//     
//     this.pst = pst;
//     
////     this.subgoal_name_mappings = subgoal_name_mappings;
////     System.out.println("Creating " +  threadName );
//  }
//  
//  public Single_thread(String db_name, String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, String[][]> query_prov_instance, HashMap<String, long[]>[] query_prov_index_lists, Query query, HashMap<String, Integer> grouping_value_prov_count_mappings, HashMap<String, Integer> query_group_value_initial_count, HashMap<Tuple, Set> tuple_valid_rows) throws ClassNotFoundException, SQLException {
//    threadName = name;
//
//    this.view = view;
//    
//    this.view_mappings = view_mappings;
//    
//    this.values_from_why_tokens = curr_tuples;
//    
////    this.query_prov_index = query_prov_index;
//    
////    this.query_prov_lists = query_prov_lists;
//    
//    this.query_prov_index_lists = query_prov_index_lists;
//    
//    this.query_prov_instance = query_prov_instance;
//
//    this.query = query;
//    
//    this.grouping_value_prov_count_mappings = grouping_value_prov_count_mappings;
//    
//    this.query_grouping_value_initial_count = query_group_value_initial_count;
//    
//    this.tuple_rows = tuple_valid_rows;
//    
//    Class.forName("org.postgresql.Driver");
//    this.c = DriverManager
//        .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
//    
//    this.pst = pst;
//    
////    this.subgoal_name_mappings = subgoal_name_mappings;
////    System.out.println("Creating " +  threadName );
// }
  public static void run(Connection c, PreparedStatement pst, String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, String[][]> query_prov_instance, HashMap<String, long[]>[] query_prov_index_lists, Query query, HashMap<String, Integer> grouping_value_prov_count_mappings, HashMap<String, Integer> query_group_value_initial_count, HashMap<Tuple, Set> tuple_valid_rows) throws SQLException, ClassNotFoundException {

//    if(!view.head.has_agg)
//      deal_with_view_view_non_aggregation();
//    else
//    Class.forName("org.postgresql.Driver");
//    Connection c = DriverManager
//        .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
//    PreparedStatement pst = null;
    
    {
      try {
        deal_with_view_with_aggregation2(c, pst, name, view, view_mappings, curr_tuples, query_prov_instance, query_prov_index_lists, query, grouping_value_prov_count_mappings, query_group_value_initial_count, tuple_valid_rows);
//        c.close();

      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    
//    System.out.println(view.view_name);
  }
  
  
  static void deal_with_view_with_aggregation2(Connection c, PreparedStatement pst, String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, String[][]> query_prov_instance, HashMap<String, long[]>[] query_prov_index_lists, Query query, HashMap<String, Integer> grouping_value_prov_count_mappings, HashMap<String, Integer> query_group_value_initial_count, HashMap<Tuple, Set> tuple_valid_rows) throws SQLException
  {
    StringBuilder stringbuilder = new StringBuilder();
    
//    double time1 = System.nanoTime();
    
    ResultSet rs = Query_provenance_2.get_query_provenance(view, c, pst);

//    double time2 = System.nanoTime();
//    
//    double time = (time2 - time1) * 1.0/1000000000;
//    
//    System.out.println("sql_time::" + time);
    double time1 = System.nanoTime();
    for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
    {
      Tuple view_mapping = (Tuple) iter2.next();
      
//      HashSet<String> query_grouping_values = new HashSet<String>();
//      
//      query_grouping_values.addAll(query_prov_instance.keySet());
      
      
      
      get_view_provenance_num2(tuple_valid_rows, query_prov_instance, query_prov_index_lists, query_group_value_initial_count, stringbuilder, rs, view, view_mapping, c, pst);
      
      
      
//      HashMap<String, Head_strs> encoding_origin_mappings = new HashMap<String, Head_strs>();
//      
//      HashMap<String, HashMap<String, Integer>> query_grouping_value_prov_value_count_mappings = get_query_provenance_num(view_mapping,encoding_origin_mappings);
//      
//      double time3 = System.nanoTime();
//      
//      check_valid_rows(tuple_rows.get(view_mapping), encoding_origin_mappings, view_grouping_value_prov_value_count_mappings, query_grouping_value_prov_value_count_mappings, query_grouping_value_view_grouping_value_mappings);
//      
//      double time4 = System.nanoTime();
//      
      
//      System.out.println("t2::" + t2);
//      System.out.println("t3::" + t3);
    }
    double time2 = System.nanoTime();
    double t1 = (time2 - time1) *1.0/1000000000;
//      double t2 = (time3 - time2) *1.0/1000000000;
//      double t3 = (time4 - time3) *1.0/1000000000;
      
      System.out.println("t1::" + t1);
  }
  
  static String convert2md5(Head_strs heads)
  {
    
    StringBuilder sb = new StringBuilder();
    
    for(int i = 0; i<heads.head_vals.size(); i++)
    {
      sb.append("###########");
      sb.append(heads.head_vals.get(i));
    }
    
    
    return sb.toString();
    
    
    
//    return MD5.get_MD5_encoding(heads.toString());
  }
  static void get_view_provenance_num2(HashMap<Tuple, Set> tuple_rows, HashMap<String, String[][]> query_prov_instance, HashMap<String, long[]>[] query_prov_index_lists, HashMap<String, Integer> query_group_value_initial_count, StringBuilder stringbuilder, ResultSet rs, Single_view view, Tuple view_mapping, Connection c, PreparedStatement pst) throws SQLException
  {
    int[] view_head_var_ids = view.view_mapping_query_head_var_attr_in_view_head_ids_mappings.get(view_mapping);
    
    Vector<Integer> q_why_column_ids = view.view_mapping_q_why_prov_token_col_ids_mapping.get(view_mapping);
    
    Vector<Integer> v_why_column_ids = view.view_mapping_view_why_prov_token_col_ids_mapping.get(view_mapping);

//    HashMap<Head_strs, HashMap<String, int[]>> view_query_prov_index_mappings = new HashMap<Head_strs, HashMap<String, int[]>>(); 
    
    HashMap<String, Integer> view_query_count = new HashMap<String, Integer>();
    
    view_query_count.putAll(query_group_value_initial_count);
    
//    HashSet<Head_strs> view_query_keys = new HashSet<Head_strs>();
    
//    HashMap<Head_strs, HashMap<String, Integer>> query_prov_index_mappings = new HashMap<Head_strs, HashMap<String, Integer>>();
    
    double time0 = System.nanoTime();
    
    double intersect_time = 0;
    
    double r_time = 0;
    
    while(rs.next())
    {
      
//      Vector<String> grouping_value_vec = new Vector<String>();
//      
//      for(int i = 0; i<view.head.args.size(); i++)
//      {
//        grouping_value_vec.add(rs.getString(i + 1));
//      }
      
      
      
//      Vector<String> query_grouping_value_vec = new Vector<String>();
      double t11 = System.nanoTime();
      String query_grouping_value_arr = new String();

      for(int i = 0; i<view_head_var_ids.length; i++)
      {
        if(i >= 1)
          query_grouping_value_arr = Head_strs.concatenate_strings(stringbuilder, query_grouping_value_arr, rs.getString(view_head_var_ids[i] + 1));
        else
          query_grouping_value_arr = rs.getString(view_head_var_ids[i] + 1);
        
//        query_grouping_value_vec.add(rs.getString(view_head_var_ids[i] + 1));
      }
      
      
      
//      Head_strs query_grouping_value = new Head_strs(query_grouping_value_vec);
      
      Array citation_vec = rs.getArray(view.head.args.size() + 1);
      
      String[][] prov_lists = (String[][]) citation_vec.getArray();
      
      double t22 = System.nanoTime();
      
      r_time += (t22 - t11);
      
      for(int i = 0; i<prov_lists.length; i++)
      {
        String[] curr_provenance = prov_lists[i];
        
//        String prov_expr = Build_query_prov_index.convert2md5(curr_provenance, v_why_column_ids);
        
//        HashMap<String, int[]> curr_query_prov_index_mappings = view_query_prov_index_mappings.get(query_grouping_value);
//        
//        if(curr_query_prov_index_mappings != null)
//        {
//          int[] counts = curr_query_prov_index_mappings.get(prov_expr);
//          
//          if(counts !=null)
//          {
//            
//            counts[0]++;
////            HashMap<String, Integer> curr_view_prov_index_mappings = view_prov_index_mappings.get(query_grouping_value);
////            
////            int count = curr_view_prov_index_mappings.get(prov_expr);
////            
////            curr_view_prov_index_mappings.put(prov_expr, count + 1);
//            
//            continue;
//          }
//            
//        }
        
        int id = 0;
        
        long[] intersect_indexes = null;
        
        double t1 = System.nanoTime();
        
        for(Integer q_prov_col: q_why_column_ids)
        {
//          Vector<String> query_prov_list = query_prov_lists[q_prov_col];
//          
//          Vector<long[]> prov_index = prov_indexes[q_prov_col];
          
          HashMap<String, long[]> query_index = query_prov_index_lists[q_prov_col];
          
//          StringBuilder sb = new StringBuilder();
          
          stringbuilder.append(curr_provenance[v_why_column_ids.get(id)]);
          
          stringbuilder.append(init.separator);
          
          stringbuilder.append(query_grouping_value_arr);
          
          String res = stringbuilder.toString();
          
          stringbuilder.delete(0, res.length());
//          int pos = Binary_search.binarySearch(query_prov_list, sb.toString());
//          
//          if(!Binary_search.check_exists(query_prov_list, sb.toString(), pos))
//            break;
          
          
          long[] indexes = query_index.get(res);
          
//          long[] indexes = prov_index.get(pos);
          
//          HashSet<Integer> indexes = prov_index_mappings.get(query_grouping_value);
          
          if(indexes == null)
            break;
          
          
          
          if(id == 0)
          {
            intersect_indexes = Bit_operation.clone_array(indexes);
            
            
          }
          else
          {
            Bit_operation.and(intersect_indexes, indexes);
          }
          
          id++;
        }
        
        double t2 = System.nanoTime();
        
        intersect_time += (t2 - t1)*1.0/1000000000;
        
        
        //provenance not matching, quit reasoning for current view tuple
        if(id < q_why_column_ids.size())
        {
          break;
        }
        
        int count = Bit_operation.numberOfSetBits(intersect_indexes); 
        
        if(count == 0)
          break;
//        if(!intersect_indexes.isEmpty())
        else
        {
          
          
          view_query_count.compute(query_grouping_value_arr, (key, value) -> value == null ? count : value + count);

//          Integer count = view_query_count.get(query_grouping_value);
//          
//          if(count == null)
//          {
////            view_query_keys.add(query_grouping_value);
//            
//            view_query_count.put(query_grouping_value, intersect_indexes.size());
//          }
//          else
//            view_query_count.put(query_grouping_value, count + intersect_indexes.size());
          
          
//          int q_prov_count = intersect_indexes.size();
//          
//          HashMap<String, int[]> curr_view_prov_mappings = view_query_prov_index_mappings.get(query_grouping_value);
//          
//          if(curr_view_prov_mappings == null)
//          {
//            curr_view_prov_mappings = new HashMap<String, int[]>();
//            
//            int[] counts = new int[2];
//            
//            counts[0] = 1;
//            
//            counts[1] = q_prov_count;
//            
//            curr_view_prov_mappings.put(prov_expr, counts);
//            
//            view_query_prov_index_mappings.put(query_grouping_value, curr_view_prov_mappings);
////            
////            HashMap<String, Integer> curr_query_prov_mappings = new HashMap<String, Integer>();
////            
////            curr_query_prov_mappings.put(prov_expr, q_prov_count);
////            
////            query_prov_index_mappings.put(query_grouping_value, curr_query_prov_mappings);
//          }
//          else
//          {
//            int[] count = curr_view_prov_mappings.get(prov_expr);
//            
//            if(count == null)
//            {
//              count = new int[2];
//              
//              count[0] = 1;
//              
//              count[1] = q_prov_count;
//              
//              curr_view_prov_mappings.put(prov_expr, count);
//              
////              query_prov_index_mappings.get(query_grouping_value).put(prov_expr, q_prov_count);
//            }
//            else
//              count[0] ++;
//          }
        }
        
       
        
      }
      
    }
    
//    if(view_query_prov_index_mappings.isEmpty())
//    {
//      valid_q_grouping_values.clear();
//      
//      tuple_rows.put(view_mapping, valid_q_grouping_values);
//      
//      return;
//    }
    
//    double time1 = System.nanoTime();
    
    check_valid_rows2(tuple_rows, view_mapping, view_query_count, query_prov_instance);
    
//    double time2 = System.nanoTime();
//    
//    double delta_time = (time2 - time1)*1.0/1000000000;
//
//    double d_time = (time1 - time0) *1.0/1000000000;
//    
//    r_time = r_time *1.0/1000000000;
//    
//    System.out.println("time0------" + d_time);
//    
//    System.out.println("time::::::::::" + delta_time);
//    
//    System.out.println("intersect_time::::" + intersect_time);
//    
//    System.out.println("retrieve_time:::" + r_time);
//    
//    System.out.println(view.view_name);
    
//    System.out.println("row_count::" + valid_q_grouping_values.size());
//    
//    tuple_rows.put(view_mapping, valid_q_grouping_values);
    
//    return grouping_value_prov_value_count_mappings;
  }
  
  
//  static void check_valid_rows2(HashMap<Tuple, Set<Head_strs>> tuple_rows, Tuple view_mapping, HashMap<Head_strs, Integer> view_query_count, HashMap<Head_strs, String[][]> query_prov_instance)
  static void check_valid_rows2(HashMap<Tuple, Set> tuple_rows, Tuple view_mapping, HashMap<String, Integer> view_query_count, HashMap<String, String[][]> query_prov_instance)
  {
    
    Iterator<Entry<String, Integer>> it = view_query_count.entrySet().iterator();
    
//    double t1 = System.nanoTime();
    
    while(it.hasNext())
    {
      Entry<String, Integer> grouping_value_count_pairs = it.next();
      
      int count1 = grouping_value_count_pairs.getValue();
      
      int count2 = query_prov_instance.get(grouping_value_count_pairs.getKey()).length;
      
      if(count1 != count2)
        it.remove();
      
      
    }
    
//    double t2 = System.nanoTime();
//    
//    double time = (t2 - t1)*1.0/1000000000;
//    
//    System.out.println("compare_time::" + time);
    
//    HashSet<Head_strs> grouping_values = new HashSet<Head_strs>();
//    
//    grouping_values.addAll();
    
    tuple_rows.put(view_mapping, view_query_count.keySet());
    
//    while(it.hasNext())
//    {
//      Head_strs grouping_value = it.next();
//      
//      Integer count1 = view_query_count.get(grouping_value);
//      
//      if(count1 == null)
//        it.remove();
//      
//      String[][] prov_lists = query_prov_instance.get(grouping_value);
//      
//      if(prov_lists == null)
//        it.remove();
//      
//      if(prov_lists.length != count1)
//        it.remove();
//    }
  }
  
  static void check_valid_rows(HashSet<Head_strs> tuple_head_vars, HashMap<Head_strs, HashMap<String, int[]>> view_grouping_value_prov_value_count_mappings, HashMap<Head_strs, String[][]> query_prov_index)
  {
    Set<Head_strs> grouping_values = view_grouping_value_prov_value_count_mappings.keySet();
    
    for(Head_strs grouping_value: grouping_values)
    {
      HashMap<String, int[]> prov_value_count_mappings1 = view_grouping_value_prov_value_count_mappings.get(grouping_value);
      
      int expected_prov_length = query_prov_index.get(grouping_value).length;
      
      if(!compare_provenance_count(prov_value_count_mappings1, expected_prov_length))
      {
        tuple_head_vars.remove(grouping_value);
      }
    }
  }
  
  
  static void check_valid_rows(HashSet<Head_strs> tuple_head_vars, HashMap<String, Head_strs> encoding_origin_mappings, HashMap<String, HashMap<String, Integer>> view_grouping_value_prov_value_count_mappings, HashMap<String, HashMap<String, Integer>> query_grouping_value_prov_value_count_mappings, HashMap<String, HashSet<String>> query_grouping_value_view_grouping_value_mappings)
  {
    Set<String> grouping_values = query_grouping_value_prov_value_count_mappings.keySet();
    
    for(String grouping_value: grouping_values)
    {
      HashMap<String, Integer> prov_value_count_mappings1 = query_grouping_value_prov_value_count_mappings.get(grouping_value);
      
      HashMap<String, Integer> prov_value_count_mappings1_copy = new HashMap<String, Integer>();
      
      Set<String> prov_sets1 = prov_value_count_mappings1.keySet(); 
      
      HashSet<String> view_grouping_values = query_grouping_value_view_grouping_value_mappings.get(grouping_value);
      
      if(view_grouping_values == null)
      {
        tuple_head_vars.remove(encoding_origin_mappings.get(grouping_value));
        
        continue;
      }
      
      for(String view_grouping_value: view_grouping_values)
      {
        HashMap<String, Integer> prov_value_count_mappings2 = view_grouping_value_prov_value_count_mappings.get(view_grouping_value);
        
        Set<String> prov_sets2 = prov_value_count_mappings2.keySet();
        
        if(!prov_sets1.containsAll(prov_sets2))
        {
          continue;
        }
        else
        {
          for(String prov_value: prov_sets2)
          {
            Integer count1 = prov_value_count_mappings2.get(prov_value);
            
            Integer count = prov_value_count_mappings1_copy.get(prov_value);
            
            if(count == null)
            {
              prov_value_count_mappings1_copy.put(prov_value, count1);
            }
            else
            {
              int total_count = count1 + count;
              
              prov_value_count_mappings1_copy.put(prov_value, total_count);
            }
            
            
          }
        }
        
                
      }
      
//      if(!compare_provenance_count(prov_value_count_mappings1, prov_value_count_mappings1_copy))
      {
        tuple_head_vars.remove(encoding_origin_mappings.get(grouping_value));
      }
    }
  }
  
  static void check_valid_rows2(HashSet<Head_strs> tuple_head_vars, HashMap<String, Head_strs> encoding_origin_mappings, HashMap<String, Vector<HashMap<String, Integer>>> view_grouping_value_prov_value_count_mappings, HashMap<String, HashMap<String, Integer>> query_grouping_value_prov_value_count_mappings, HashMap<String, HashSet<String>> query_grouping_value_view_grouping_value_mappings)
  {
    Set<String> grouping_values = query_grouping_value_prov_value_count_mappings.keySet();
    
    for(String grouping_value: grouping_values)
    {
      HashMap<String, Integer> prov_value_count_mappings1 = query_grouping_value_prov_value_count_mappings.get(grouping_value);
      
      Vector<HashMap<String, Integer>> view_prov_value_counts = view_grouping_value_prov_value_count_mappings.get(grouping_value);
      
      HashMap<String, Integer> view_prov_value_count = new HashMap<String, Integer>();
      
      
      for(HashMap<String, Integer> curr_view_prov_value_count: view_prov_value_counts)
      {
        for(String prov_value: curr_view_prov_value_count.keySet())
        {
          Integer count1 = curr_view_prov_value_count.get(prov_value);
          
          Integer count = view_prov_value_count.get(prov_value);
          
          if(count == null)
          {
            view_prov_value_count.put(prov_value, count1);
          }
          else
          {
            int total_count = count1 + count;
            
            view_prov_value_count.put(prov_value, total_count);
          }
          
          
        }
      }
      
      
      
      
      
//      if(!compare_provenance_count(prov_value_count_mappings1, view_prov_value_count))
      {
        tuple_head_vars.remove(encoding_origin_mappings.get(grouping_value));
      }
    }
  }
  
  
  static boolean compare_provenance_count(HashMap<String, int[]> prov_value_count_mappings1, int expected_prov_length)
  {
    Set<String> prov_values1 = prov_value_count_mappings1.keySet();
    
    int multiplier = 0;
    
    int total_count = 0;
    
    for(String prov_value: prov_values1)
    {
      int[] count1 = prov_value_count_mappings1.get(prov_value);
      
      total_count += count1[1];
      {
        if(count1[0] != (count1[0]/count1[1])*count1[1])
          return false;
        
      }
    }
    
    if(total_count != expected_prov_length)
      return false;
    
    return true;
  }

//  @Override
//  public HashMap<Tuple, Set> get_tuple_rows() {
//    // TODO Auto-generated method stub
//    return tuple_rows;
//  }
}
