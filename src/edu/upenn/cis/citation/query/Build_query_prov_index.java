package edu.upenn.cis.citation.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.util.Binary_search;
import edu.upenn.cis.citation.util.Bit_operation;
import edu.upenn.cis.citation.views.Single_view;

public class Build_query_prov_index {
  
  
  /*build map from each prov_token to group value to location index
   * */
  
  
  public static String convert2md5(Vector<String> heads)
  {
    StringBuilder sb = new StringBuilder();
    
    for(int i = 0; i<heads.size(); i++)
    {
      sb.append("###########");
      sb.append(heads.get(i));
    }
    
    
    return sb.toString();
    
//    return MD5.get_MD5_encoding(heads.toString());
  }
  
  
  public static String convert2md5(String[] heads)
  {
    StringBuilder sb = new StringBuilder();
    
    for(int i = 0; i<heads.length; i++)
    {
      sb.append("###########");
      sb.append(heads[i]);
    }
    
    
    return sb.toString();
    
//    return MD5.get_MD5_encoding(heads.toString());
  }
  
  public static String convert2md5(String[] heads, Vector<Integer> ids)
  {
    StringBuilder sb = new StringBuilder();
    
    for(int i = 0; i<ids.size(); i++)
    {
      sb.append("###########");
      sb.append(heads[ids.get(i)]);
    }
    
    
    return sb.toString();
    
//    return MD5.get_MD5_encoding(heads.toString());
  }
  
  static Vector<String> retrieve_query_provenance(Query query, Vector<Head_strs> values)
  {
    Vector<String> query_prov_lists = new Vector<String>();
    
    for(int i = 0; i<query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
//      for(int j = 0; j<subgoal.args.size(); j++)
      Vector<Integer> primary_key_ids = Single_view.relation_primary_key_mappings.get(query.subgoal_name_mapping.get(subgoal.name));
      
//      for(int j = 0; j<subgoal.args.size(); j++)
      Vector<String> primary_key_strings = new Vector<String>();
      for(int j = 0; j < primary_key_ids.size(); j ++)
      {
        primary_key_strings.add(Single_view.clean_boolean_type((Argument) subgoal.args.get(primary_key_ids.get(j)), values.get(i).head_vals.get(primary_key_ids.get(j))));
//        view_provenance_values.add(values.get(q_why_column_ids.get(i)).head_vals.get(j));
      }
      
      query_prov_lists.add(convert2md5(primary_key_strings));
      
    }
    
    return query_prov_lists;
    
  }
  
  public static void build_index_for_query_prov(Query query, HashMap<String, HashMap<Head_strs, HashSet<Integer>>>[] query_prov_index, Vector<Head_strs> curr_provs, Head_strs curr_grouping_value, int prov_list_id)
  {
    
    Vector<String> query_prov_lists = retrieve_query_provenance(query, curr_provs);
    
    System.out.println(prov_list_id);
    
    System.out.println(query_prov_lists);
    
    for(int i = 0; i<query_prov_lists.size(); i++)
    {
      HashMap<String, HashMap<Head_strs, HashSet<Integer>>> curr_query_prov_index = query_prov_index[i];
      
      String query_prov = query_prov_lists.get(i);
      
      if(curr_query_prov_index.get(query_prov) == null)
      {
        HashMap<Head_strs, HashSet<Integer>> curr_grouping_value_index_mappings = new HashMap<Head_strs, HashSet<Integer>>();
        
        HashSet<Integer> indexes = new HashSet<Integer>();
        
        indexes.add(prov_list_id);
        
        curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
        
        curr_query_prov_index.put(query_prov, curr_grouping_value_index_mappings);
        
      }
      else
      {
        HashMap<Head_strs, HashSet<Integer>> curr_grouping_value_index_mappings = curr_query_prov_index.get(query_prov);
        
        HashSet<Integer> indexes = curr_grouping_value_index_mappings.get(curr_grouping_value);
        
        if(indexes == null)
        {
          indexes = new HashSet<Integer>();
          
          indexes.add(prov_list_id);
          
          curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
        }
        else
          indexes.add(prov_list_id);
      }
    }
    
    
  }
  
  public static void build_index_for_query_prov(Query query, HashMap<String, HashMap<Head_strs, HashSet<Integer>>>[] query_prov_index, Head_strs curr_grouping_value, String[][] prov_lists)
  {
    for(int i = 0; i<prov_lists.length; i++)
    {
      String[] prov_list = prov_lists[i];
      
      for(int j = 0; j<prov_list.length; j++)
      {
        HashMap<String, HashMap<Head_strs, HashSet<Integer>>> curr_query_prov_index = query_prov_index[j];
        
        String query_prov = prov_list[j];
        
        if(curr_query_prov_index.get(query_prov) == null)
        {
          HashMap<Head_strs, HashSet<Integer>> curr_grouping_value_index_mappings = new HashMap<Head_strs, HashSet<Integer>>();
          
          HashSet<Integer> indexes = new HashSet<Integer>();
          
          indexes.add(i);
          
          curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
          
          curr_query_prov_index.put(query_prov, curr_grouping_value_index_mappings);
          
        }
        else
        {
          HashMap<Head_strs, HashSet<Integer>> curr_grouping_value_index_mappings = curr_query_prov_index.get(query_prov);
          
          HashSet<Integer> indexes = curr_grouping_value_index_mappings.get(curr_grouping_value);
          
          if(indexes == null)
          {
            indexes = new HashSet<Integer>();
            
            indexes.add(i);
            
            curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
          }
          else
            indexes.add(i);
        }
      }
      
    }
  }
  
  public static void build_index_for_query_prov2(Query query, HashMap<String, HashSet<Integer>>[] query_prov_index, Head_strs curr_grouping_value, String[][] prov_lists)
  {
    for(int i = 0; i<prov_lists.length; i++)
    {
      String[] prov_list = prov_lists[i];
      
      for(int j = 0; j<prov_list.length; j++)
      {
        HashMap<String, HashSet<Integer>> curr_query_prov_index = query_prov_index[j];
        
        String query_prov = prov_list[j] + init.separator + curr_grouping_value;
        
        if(curr_query_prov_index.get(query_prov) == null)
        {
          HashSet<Integer> indexes = new HashSet<Integer>();
          
          indexes.add(i);
          
          curr_query_prov_index.put(query_prov, indexes);
          
        }
        else
        {
          curr_query_prov_index.get(query_prov).add(i);
          
//          HashSet<Integer> indexes = curr_grouping_value_index_mappings.get(curr_grouping_value);
//          
//          if(indexes == null)
//          {
//            indexes = new HashSet<Integer>();
//            
//            indexes.add(i);
//            
//            curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
//          }
//          else
//            indexes.add(i);
        }
      }
      
    }
  }
  public static void build_index_for_query_prov3(StringBuilder sb, Query query, HashMap<String, HashMap<String, long[]>>[] query_prov_index, String curr_grouping_value, String[][] prov_lists)
//  public static void build_index_for_query_prov3(StringBuilder sb, Query query, HashMap<String, long[]>[] query_prov_index, String curr_grouping_value, String[][] prov_lists)
//  public static void build_index_for_query_prov3(Query query, HashMap<String, long[]>[] query_prov_index, Head_strs curr_grouping_value, String[][] prov_lists)
  {
    for(int i = 0; i<prov_lists.length; i++)
    {
      String[] prov_list = prov_lists[i];
      
      for(int j = 0; j<prov_list.length; j++)
      {
//        HashMap<String, long[]> curr_query_prov_index = query_prov_index[j];
        HashMap<String, HashMap<String, long[]>> curr_query_prov_index = query_prov_index[j];
        
//        sb.append(prov_list[j]);
//        
//        sb.append(init.separator);
//        
//        sb.append(curr_grouping_value);
//        
////        String query_prov = prov_list[j] + init.separator + curr_grouping_value;
//        String query_prov = sb.toString();
//        
//        sb.setLength(0);
        
        HashMap<String, long[]> grouping_value_prov_index = curr_query_prov_index.get(prov_list[j]); 
        
        if(grouping_value_prov_index == null)
        {
          grouping_value_prov_index = new HashMap<String, long[]>();
          
          long[] indexes = Bit_operation.create_index(prov_lists.length);
          
          Bit_operation.set_bit(indexes, i);
          
          grouping_value_prov_index.put(curr_grouping_value, indexes);
//          indexes.add(i);
          
          curr_query_prov_index.put(prov_list[j], grouping_value_prov_index);
          
        }
        else
        {
          long[] indexes = grouping_value_prov_index.get(curr_grouping_value);
          
          if(indexes == null)
          {
            indexes = Bit_operation.create_index(prov_lists.length);
            
            Bit_operation.set_bit(indexes, i);
            
            grouping_value_prov_index.put(curr_grouping_value, indexes);
          }
          else          
            Bit_operation.set_bit(indexes, i);
          
//          HashSet<Integer> indexes = curr_grouping_value_index_mappings.get(curr_grouping_value);
//          
//          if(indexes == null)
//          {
//            indexes = new HashSet<Integer>();
//            
//            indexes.add(i);
//            
//            curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
//          }
//          else
//            indexes.add(i);
        }
      }
      
    }
  }
  
  
  public static void build_index_for_query_prov4(Query query, Vector<String>[] query_provs, Vector<long[]>[] prov_indexes, HashMap<Head_strs, String[][]> query_prov_lists, int length)
  {
    
    for(int i = 0; i<query_provs.length; i++)
    {
      query_provs[i].ensureCapacity(length);
      
      prov_indexes[i].ensureCapacity(length);
    }
    
    for(Entry<Head_strs, String[][]> curr_query_prov_list: query_prov_lists.entrySet())
    {
      Head_strs query_head = curr_query_prov_list.getKey();
      
      String[][] prov_lists = curr_query_prov_list.getValue();
      
      for(int i = 0; i<prov_lists.length; i++)
      {
        String[] prov_list = prov_lists[i];
        
        for(int j = 0; j<prov_list.length; j++)
        {
          Vector<String> curr_query_prov = query_provs[j];
          
          Vector<long[]> prov_index =prov_indexes[j];
          
//          HashMap<String, long[]> curr_query_prov_index = query_prov_index[j];
          
          String query_prov = prov_list[j] + init.separator + query_head;
          
//          System.out.print(i + "::");
//          
//          System.out.println(query_prov);
          
          int pos = Binary_search.binarySearch(curr_query_prov, query_prov);
          
          if(!Binary_search.check_exists(curr_query_prov, query_prov, pos))
          {
            long[] indexes = Bit_operation.create_index(prov_lists.length);
            
            Bit_operation.set_bit(indexes, i);
            
            Binary_search.insert2list(curr_query_prov, prov_index, query_prov, indexes, pos);
            
//            System.out.println(curr_query_prov.size());
//            curr_query_prov_index.put(query_prov, indexes);
            
          }
          else
          {
            Bit_operation.set_bit(prov_index.get(pos), i);
            
//            HashSet<Integer> indexes = curr_grouping_value_index_mappings.get(curr_grouping_value);
//            
//            if(indexes == null)
//            {
//              indexes = new HashSet<Integer>();
//              
//              indexes.add(i);
//              
//              curr_grouping_value_index_mappings.put(curr_grouping_value, indexes);
//            }
//            else
//              indexes.add(i);
          }
        }
        
      }
    }

  }
  
  
  public static void print_query_index(HashMap<String, HashMap<Head_strs, HashSet<Integer>>>[] query_indexes)
  {
    for(int i = 0; i<query_indexes.length; i++)
    {
      HashMap<String, HashMap<Head_strs, HashSet<Integer>>> query_index = query_indexes[i];
      
      Set<String> prov_tokens = query_index.keySet();
      
      for(String prov_token: prov_tokens)
      {
        System.out.println(prov_token);
        
        HashMap<Head_strs, HashSet<Integer>> grouping_value_index_mappings = query_index.get(prov_token);
        
        System.out.println(grouping_value_index_mappings);
      }
      
    }
    
  }
}
