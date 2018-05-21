package edu.upenn.cis.citation.multi_thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.bit_operation.Bit_operation;
import edu.upenn.cis.citation.citation_view.Head_strs;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class Check_valid_view_mappings_agg_batch_processing2 implements Check_valid_view_mappings {
  private Thread t;
  private String threadName;
  public Single_view view;
  
  public HashSet<Tuple> view_mappings;
  
  public ArrayList<Vector<Head_strs>> values_from_why_tokens;
  
//  public ConcurrentHashMap<Tuple, long[]> tuple_rows_bit_index = new ConcurrentHashMap<Tuple, long[]>();
  public ConcurrentHashMap<Head_strs, Head_strs> query_grouping_value_agg_value_mapping;
  
  public ConcurrentHashMap<Tuple, HashSet<String>> tuple_rows = new ConcurrentHashMap<Tuple, HashSet<String>>();
  
  public ConcurrentHashMap<String, Head_strs> md5_mappings = new ConcurrentHashMap<String, Head_strs>();
  
  public ConcurrentHashMap<Head_strs, ArrayList<Integer>> tuple_why_prov_mappings = new ConcurrentHashMap<Head_strs, ArrayList<Integer>>();

  public Query query = null;
  
  public Connection c;
  
  public PreparedStatement pst;
  
  public ConcurrentHashMap<String, ConcurrentHashMap<String, Vector<Integer>>> rel_attr_value_mappings;
  
//  ConcurrentHashMap<String, String> subgoal_name_mappings;
  
  public Check_valid_view_mappings_agg_batch_processing2( String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, ConcurrentHashMap<Head_strs, ArrayList<Integer>> tuple_why_prov_mappings, ConcurrentHashMap<String, ConcurrentHashMap<String, Vector<Integer>>> rel_attr_value_mappings, ConcurrentHashMap<Head_strs, Head_strs> grouping_value_agg_value_mapping, Query query, Connection c, PreparedStatement pst) throws SQLException, ClassNotFoundException {
     threadName = name;

     this.view = view;
     
     this.view_mappings = view_mappings;
     
     this.values_from_why_tokens = curr_tuples;
     
     this.rel_attr_value_mappings = rel_attr_value_mappings;
     
     this.tuple_why_prov_mappings = tuple_why_prov_mappings;
     
     this.query_grouping_value_agg_value_mapping = grouping_value_agg_value_mapping;
     
     this.query = query;
     
     Class.forName("org.postgresql.Driver");
     this.c = DriverManager
         .getConnection(init.db_url, init.usr_name , init.passwd);
     
     this.pst = pst;
     
//     this.subgoal_name_mappings = subgoal_name_mappings;
//     System.out.println("Creating " +  threadName );
  }
  
  static void get_valid_row_ids(Tuple tuple, ArrayList<String[][]> partial_mapping_values, HashSet<String> valid_head_vals, Head_strs[] head_val_array, HashSet<Integer> row_ids, Vector<Conditions> conditions, Vector<Subgoal> subgoals, HashMap<String, String> subgoal_name_mappings, Connection c, PreparedStatement pst) throws SQLException
  {
    
//    System.out.println(tuple);
    if(row_ids.isEmpty())
      return;

    String sql_base = "select " + temp_table_name + ".row_id from (VALUES ";
    
    HashSet<Integer> valid_row_ids = new HashSet<Integer>();
    valid_row_ids.addAll(row_ids);
    
    for(int i = 0; i<tuple.cluster_subgoal_ids.size(); i++)
    {
      
      
      if(tuple.cluster_patial_mapping_condition_ids.get(i).size() <= 0)
        continue;
      
//      System.out.println(tuple.cluster_subgoal_ids);
//      
//      System.out.println(tuple.cluster_patial_mapping_condition_ids);
//      
//      System.out.println(tuple.cluster_non_mapping_condition_ids);
      
      String[][] curr_partial_mapping_values = partial_mapping_values.get(i);
      
      String sql = sql_base;
      
      String join_condition = new String();
      
      boolean first_id = true;
      
      for(Integer id: valid_row_ids)
      {
        String [] curr_values = curr_partial_mapping_values[id];
      
        if(!first_id)
          sql += ",";
        
        sql += "(" + id;
        
        for(int k = 0; k<curr_values.length; k++)
        {
            sql += "," + curr_values[k];
          
        }
        
        sql += ")";
        
        first_id = false;
      }
      
      sql += ") as " + temp_table_name + "(row_id ";
      
      int join_condition_count = 0;
      
//      HashSet<String> subgoal_names = new HashSet<String>();
      
      HashSet<String> partial_join_mapped_attribute_names = new HashSet<String>();
      
      for(Integer id: tuple.cluster_patial_mapping_condition_ids.get(i))
      {
        Conditions condition = conditions.get(id);
        
        Argument arg1 = condition.arg1;
        
        Argument arg2 = condition.arg2;
        
        String subgoal_name1 = condition.subgoal1;
        
        String subgoal_name2 = condition.subgoal2;
        
//        subgoal_names.add(condition.subgoal1);
//        
//        subgoal_names.add(condition.subgoal2);
        
        if(tuple.mapSubgoals_str.get(subgoal_name2) == null)
        {
          String partial_join_mapped_attribute_name = arg1.name.replaceAll("\\" + init.separator, "_");
          
          if(!partial_join_mapped_attribute_names.contains(partial_join_mapped_attribute_name))
          {
            sql += "," + partial_join_mapped_attribute_name;
            
            partial_join_mapped_attribute_names.add(partial_join_mapped_attribute_name);
          }
          
          if(join_condition_count >= 1)
            join_condition += " and ";
          
          join_condition += temp_table_name + "." + arg1.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg2.name.replaceFirst("\\" + init.separator, ".");

        }
        else
        {
          String partial_join_mapped_attribute_name = arg2.name.replaceAll("\\" + init.separator, "_");
          
          if(!partial_join_mapped_attribute_names.contains(partial_join_mapped_attribute_name))
          {
            sql += "," + partial_join_mapped_attribute_name;
            
            partial_join_mapped_attribute_names.add(partial_join_mapped_attribute_name);
          }
          
//          sql += "," + partial_join_mapped_attribute_name;
          
          if(join_condition_count >= 1)
            join_condition += " and ";
          
          join_condition += temp_table_name + "." + arg2.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg1.name.replaceFirst("\\" + init.separator, ".");

        }
        
                
        join_condition_count ++;
                        
      }
      
      for(Integer id: tuple.cluster_non_mapping_condition_ids.get(i))
      {
        Conditions condition = conditions.get(id);
        
        Argument arg1 = condition.arg1;
        
        Argument arg2 = condition.arg2;
        
        if(join_condition_count >= 1)
          join_condition += " and ";
        
        join_condition += arg1.name.replaceFirst("\\" + init.separator, ".") + condition.op.toString() + arg2.name.replaceFirst("\\" + init.separator, ".");
        
        join_condition_count ++;
      }
      
      
      sql += ")";
      
      for(Integer id: tuple.cluster_subgoal_ids.get(i))
      {
        Subgoal subgoal = subgoals.get(id);
        
//        if(subgoal_names.contains(subgoal.name))
//        {
//          continue;
//        }
        
        String origin_name = subgoal_name_mappings.get(subgoal.name);
        
        sql += "," + origin_name + " " + subgoal.name;
        
      }
      
//      for(String subgoal_name: subgoal_names)
//      {
//        String origin_name = subgoal_name_mappings.get(subgoal_name);
//        
//        sql += "," + origin_name + " " + subgoal_name;
//      }
      
      if(!join_condition.isEmpty())
      {
        sql += " where " + join_condition;
      }
      
//      if(tuple.toString().equals("v4|family0=family,introduction2=introduction"))
//      {
        System.out.println(sql);
//        
//      }
      
      
      pst = c.prepareStatement(sql);
      
      ResultSet rs = pst.executeQuery();
      
//      HashSet<Integer> curr_valid_row_ids = new HashSet<Integer>();
      
      valid_row_ids.clear();
      
      while(rs.next())
      {
        valid_row_ids.add(rs.getInt(1));
      }
      
//      if(tuple.toString().equals("v4|family0=family,introduction2=introduction"))
//      {
//        System.out.println(row_ids);
//      }
    }
    
//    System.out.println(valid_row_ids);
    
    row_ids.removeAll(valid_row_ids);
    
//    System.out.println(row_ids);
    
    for(Integer rid: row_ids)
    {
      valid_head_vals.remove(convert_head_string2string(head_val_array[rid]));
    }
    
//    System.out.println(row_ids.size());
    
    
//    Set<String> undetermined_relations = undermined_table_conditions_mappings.keySet();
//    
//    HashSet<Integer> valid_row_ids = null;
//    
//    boolean first = true;
//    
//    for(String table: undetermined_relations)
//    {
//      String origin_relation_name = subgoal_name_mappings.get(table);
//      
//      String sql = sql_base;
//      
//      ArrayList<ArrayList<String>> values = undetermined_table_arg_value_mappings.get(table);
//      
//      ArrayList<Conditions> conditions = undermined_table_conditions_mappings.get(table);
//      
//      for(int i = 0; i<values.size(); i++)
//      {
//        
//        if(i >= 1)
//          sql += ",";
//        
//        sql += "(" + i;
//        
//        for(int j = 0; j<values.get(i).size(); j++)
//        {
//          sql += "," + values.get(i).get(j);
//        }
//        
//        sql += ")";
//      }
//      
//      sql += ") as t(row_id, ";
//      
//      String condition_str = new String();
//      
//      for(int i = 0; i < conditions.size(); i++)
//      {
//        
//        if(i >= 1)
//          condition_str += ",";
//        
//        Argument arg2 = conditions.get(i).arg2;
//
//        Argument arg1 = conditions.get(i).arg1;
//        
//        String arg2_name = arg2.name.replaceAll("\\" + init.separator, "_"); 
//        
//        String [] relation_arg_name = arg1.name.split("\\" + init.separator);
//        
//        String arg1_name = relation_arg_name[1];
//        
//        condition_str += origin_relation_name + "." + arg1_name + conditions.get(i).op.toString() + "t." + arg2_name;
//        
//        sql += arg2_name;
//        
//      }      
//      
//      sql += ") join " + origin_relation_name + " on (" + condition_str + ")";
//      
//      
//      
//    }
//    
//    row_ids.addAll(valid_row_ids);
    
  }

  
//  Vector<Head_strs> deal_with_view_with_aggregation_local_global_predicates(Tuple tuple, Single_view view)
//  {
//    Set<Head_strs> head_vals = tuple_why_prov_mappings.keySet();
//    
//    Vector<Head_strs> valid_head_vals = new Vector<Head_strs>();
//    
//    Vector<String[]> view_grouping_attr_values = new Vector<String[]> ();
//    
//    ArrayList<String[][]> partial_mapping_values = new ArrayList<String[][]>();
//    
//    for(int i = 0; i<tuple.cluster_patial_mapping_condition_ids.size(); i++)
//    {
//      HashSet<String> partial_mapping_subgoals = get_unique_partial_mapping_subgoals(view, tuple, i);
//      
//      String[][] curr_partial_mapping_values = new String[values_from_why_tokens.size()][partial_mapping_subgoals.size()];
//      
//      partial_mapping_values.add(curr_partial_mapping_values);
//    }
//    
//    HashMap<Head_strs, ArrayList<Integer>> view_grouping_attr_value_rid_mappings = new HashMap<Head_strs, ArrayList<Integer>>();
//    
//    for(Head_strs head_val: head_vals)
//    {
//      ArrayList<Integer> curr_rids = tuple_why_prov_mappings.get(head_val);
//      
//      boolean valid4curr_head_val = true;
//      
//      Vector<String[]> curr_view_grouping_attr_values = new Vector<String[]>();
//      
//      for(Integer curr_rid : curr_rids)
//      {
//        view.evaluate_args(values_from_why_tokens.get(curr_rid), tuple);
//        
//        view.evaluate_view_grouping_attrs(curr_view_grouping_attr_values, values_from_why_tokens.get(curr_rid), tuple);
//        
//        if(!view.check_validity(tuple))//rel_attr_value_mappings, undermined_table_conditions_mappings, undetermined_table_arg_value_mappings, first, c, pst))
//        {
//          valid4curr_head_val = false;
//          
//          break;
//          
//        }
//        else
//        {
//          view.get_partial_mapping_values(tuple, partial_mapping_values, curr_rid);
//        }
//        
//        
//        
//      }
//      
//      if(valid4curr_head_val)
//      {
//        tuple_rows.get(tuple).addAll(curr_rids);
//        
//        valid_head_vals.add(head_val);
//        
//        view_grouping_attr_values.addAll(curr_view_grouping_attr_values);
//      }
//    }
//    
//    //partial_mapping_checks here
//    get_valid_row_ids(tuple, partial_mapping_values, tuple_rows.get(tuple), view.conditions, view.subgoals, view.subgoal_name_mappings, c, pst);
//    
//    
//    return valid_head_vals;
//  }
  String gen_instantiation_condition_string(Vector<Argument> args)
  {
    String grouping_value_condition_string = "(";
    for(int i = 0; i<args.size(); i++)
    {
      if(i >= 1)
        grouping_value_condition_string += ",";
      Argument view_grouping_attr = (Argument) args.get(i);
      String view_grouping_attr_name = view_grouping_attr.name.replace(init.separator, ".");
      grouping_value_condition_string += "md5(" + view_grouping_attr_name + ")";
    }
    grouping_value_condition_string += ")";
    return grouping_value_condition_string;
  }
  
  String gen_instantiation_condition_string(Vector<Argument> all_args, Vector<String> relation_seqs)
  {
    HashMap<String, Vector<Argument>> view_relation_head_arg_mappings = new HashMap<String, Vector<Argument>>();
    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = (Argument) all_args.get(i);
      String relation_name = arg.name.substring(0, arg.name.indexOf(init.separator));
      if(view_relation_head_arg_mappings.get(relation_name) == null)
      {
        Vector<Argument> args = new Vector<Argument>();
        args.add(arg);
        view_relation_head_arg_mappings.put(relation_name, args);
      }
      else
      {
        view_relation_head_arg_mappings.get(relation_name).add(arg);
      }
    }
    
    String grouping_value_condition_string = "(";
    Set<String> relation_set = view_relation_head_arg_mappings.keySet();
    int count = 0;
    for(String relation: relation_set)
    {
      relation_seqs.add(relation);
      if(count >= 1)
        grouping_value_condition_string += ",";
      Vector<Argument> curr_args = view_relation_head_arg_mappings.get(relation);
      String curr_condition_string = new String();
      for(int i = 0; i<curr_args.size(); i++)
      {
        if(i >= 1)
          curr_condition_string += "||'" + init.separator + "'||";
        Argument view_grouping_attr = (Argument) curr_args.get(i);
        String view_grouping_attr_name = view_grouping_attr.name.replace(init.separator, ".");
        curr_condition_string += view_grouping_attr_name;
      }
      
      grouping_value_condition_string += "md5(" + curr_condition_string + ")";
      count ++;
    }
    
    
    
    grouping_value_condition_string += ")";
    return grouping_value_condition_string;
  }
  
  String gen_instantiation_condition_string2(Vector<Subgoal> all_args)
  {
//    HashMap<String, Vector<Argument>> view_relation_head_arg_mappings = new HashMap<String, Vector<Argument>>();
//    for(int i = 0; i<all_args.size(); i++)
//    {
//      Argument arg = (Argument) all_args.get(i);
//      String relation_name = arg.name.substring(0, arg.name.indexOf(init.separator));
//      if(view_relation_head_arg_mappings.get(relation_name) == null)
//      {
//        Vector<Argument> args = new Vector<Argument>();
//        args.add(arg);
//        view_relation_head_arg_mappings.put(relation_name, args);
//      }
//      else
//      {
//        view_relation_head_arg_mappings.get(relation_name).add(arg);
//      }
//    }
    
    String grouping_value_condition_string = "(";
//    Set<String> relation_set = view_relation_head_arg_mappings.keySet();
//    int count = 0;
    for(int k = 0; k<all_args.size(); k++)
    {
      Subgoal subgoal = all_args.get(k);
      if(k >= 1)
        grouping_value_condition_string += ",";
      Vector<Argument> curr_args = subgoal.args;
      String curr_condition_string = new String();
      for(int i = 0; i<curr_args.size(); i++)
      {
        if(i >= 1)
          curr_condition_string += "||'" + init.separator + "'||";
        Argument view_grouping_attr = (Argument) curr_args.get(i);
        String view_grouping_attr_name = view_grouping_attr.name.replace(init.separator, ".");
        curr_condition_string += view_grouping_attr_name;
      }
      
      grouping_value_condition_string += "md5(" + curr_condition_string + ")";
//      count ++;
    }
    
    
    
    grouping_value_condition_string += ")";
    return grouping_value_condition_string;
  }
  
  HashMap<String, Integer> count_tuples_within_view_groups_given_aggregate_values(Tuple tuple, HashMap<String, String> view_head_value_query_head_value_mappings, boolean has_having_clause, ArrayList<HashMap<String, HashSet<Integer>>> query_provenance_rid_mappings, ArrayList<String> view_head_values, HashMap<String, Integer> q_provenance_count2) throws SQLException
  {
    HashMap<String, Integer> view_head_vals_tuple_count = new HashMap<String, Integer>();
    
    HashSet<String> head_values = tuple_rows.get(tuple);
    
//    System.out.println(head_values);
    HashSet<String> grouping_values = new HashSet<String>();
    
    
    
    Vector<String> relation_seqs = new Vector<String>();
    String grouping_value_condition_string = gen_instantiation_condition_string(view.head.args, relation_seqs);
//    for(int i = 0; i<view.head.args.size(); i++)
//    {
//      if(i >= 1)
//        grouping_value_condition_string += ",";
//      Argument view_grouping_attr = (Argument) view.head.args.get(i);
//      String view_grouping_attr_name = view_grouping_attr.name.replace(init.separator, ".");
//      grouping_value_condition_string += view_grouping_attr_name;
//    }
//    grouping_value_condition_string += ")";
    int count = 0;
    
    if(view.head.args.size() > 0)
    for(String head_val : head_values)
    {
      ArrayList<Integer> curr_rids = tuple_why_prov_mappings.get(md5_mappings.get(head_val));
      
//      System.out.println("rids::" + curr_rids);
      
      for(Integer rid: curr_rids)
      {
//        if(count >= 1)
//          grouping_values += ",";
        if(count == 86000)
        {
          int y = 0;
          y++;
        }
        
        grouping_values.add((view.evaluate_view_grouping_attrs(values_from_why_tokens.get(rid), tuple, query, relation_seqs)));
//        System.out.println(grouping_values.length());
//        grouping_value_condition_string += curr_grouping_value_condition_string;
//        view_grouping_values_from_query.add(view_grouping_values);
//        System.out.println(count);
        count ++;
      }
      
    }
    
    
    
//    grouping_value_condition_string += "= ANY (VALUES" + grouping_values + ")";
//    System.out.println(view_grouping_values_from_query);
    int[] query_head_attr_view_head_ids = view.view_mapping_query_head_var_attr_in_view_head_ids_mappings.get(tuple);
    HashMap<String, String> sql_clauses = Query_converter.data2sql_check_having_clause2(tuple, view, query_head_attr_view_head_ids);
    ArrayList<String> grouping_value_copies = new ArrayList<String>();
    grouping_value_copies.addAll(grouping_values);
    view_head_vals_tuple_count = cal_count_partitions(grouping_value_copies, grouping_value_condition_string, sql_clauses, view.head.args.size(), view_head_value_query_head_value_mappings, query_provenance_rid_mappings, view_head_values, q_provenance_count2);
    

//    for(String view_head_val: view_head_vals_tuple_count.keySet())
//    {
//      Vector<String> q_head_values = new Vector<String>();
//      for(int i = 0; i<query_head_attr_view_head_ids.length; i++)
//      {
//        q_head_values.add(view_head_val.head_vals.get(query_head_attr_view_head_ids[i]));
//      }
//      
//      view_head_value_query_head_value_mappings.put(view_head_val, new Head_strs(q_head_values));
//    }
    
//    System.out.println(sql);
//    
//    pst = c.prepareStatement(sql);
//    
//    ResultSet rs = pst.executeQuery();
    
//    HashSet<Head_strs> potentail_invalid_head_values = null;
//    
//    if(has_having_clause)
//    {
//      potentail_invalid_head_values = new HashSet<Head_strs>();
//      
//      potentail_invalid_head_values.addAll(view_head_value_query_head_value_mappings.keySet());
//    }
    
    
//    while(rs.next())
//    {
//      Vector<String> h_values = new Vector<String>();
//      for(int i = 0; i<view.head.args.size(); i++)
//      {
//        h_values.add(rs.getString(i + 1));
//      }
//      
//      int tuple_count = rs.getInt(view.head.args.size() + 1);
//      
//      Head_strs view_head_values = new Head_strs(h_values);
//      
//      view_head_vals_tuple_count.put(view_head_values, tuple_count);
//      
//      
//      Vector<String> q_head_values = new Vector<String>();
//      for(int i = 0; i<query_head_attr_view_head_ids.length; i++)
//      {
//        q_head_values.add(h_values.get(query_head_attr_view_head_ids[i]));
//      }
//      
//      view_head_value_query_head_value_mappings.put(view_head_values, new Head_strs(q_head_values));
//      
////      System.out.println("head_values::" + view_head_values);
////      if(has_having_clause)
////        potentail_invalid_head_values.remove(view_head_values);
//    }
//    System.out.println(potentail_invalid_head_values);
//    if(has_having_clause)
//    {
//      for(Head_strs h: potentail_invalid_head_values)
//      {
//        head_values.remove(view_head_value_query_head_value_mappings.get(h));
//        
//        view_head_value_query_head_value_mappings.remove(h);
//      }
//    }
    
    
    return view_head_vals_tuple_count;
  }
  
  
//  String convert_head_string2string(Head_strs string)
//  {
//    String res = "(";
//    for(int i = 0; i<string.head_vals.size(); i++)
//    {
//      if(i >= 1)
//        res += ",";
//      String arg_name = string.head_vals.get(i).replaceAll("'", "''");
//      res += "'" + arg_name + "'";
//    }
//    res += ")";
//    return res;
//  }
  
  static String convert_head_string2string(Head_strs string)
  {
    String res = "(";
//    String res = new String();
    for(int i = 0; i<string.head_vals.size(); i++)
    {
      if(i >= 1)
        res += ",";
      String arg_name = string.head_vals.get(i);
      res += "'" + MD5.getMD5(arg_name) + "'";
    }
    
//    res = MD5.getMD5(res);
    res += ")";
    
    return res;
  }
  
  String convertHead_strs2md5(Head_strs h)
  {
    String res = new String();
    for(int i = 0; i<h.head_vals.size(); i++)
    {
      if(i >= 1)
        res += init.separator;
      res += h.head_vals.get(i);
    }
    return MD5.getMD5(res);
  }
  
  HashMap<String, Integer> count_tuples_within_view_groups_given_provenance(Tuple tuple, HashMap<String, String> view_provenance_values, HashMap<String, Integer> q_provenance_count, Vector<Subgoal> all_view_instantiation_condition_args, ArrayList<HashMap<String, HashSet<Integer>>> query_provenance_rid_mappings, ArrayList<String> view_head_vars) throws SQLException
  {
//    HashMap<Head_strs, Integer> view_head_vals_tuple_count = new HashMap<Head_strs, Integer>();
    
    Vector<Integer> q_why_column_ids = view.view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    HashSet<String> head_values = tuple_rows.get(tuple);
    HashMap<String, Integer> view_head_vals_tuple_count = new HashMap<String, Integer>();
    for(String head_val : head_values)
    {
      ArrayList<Integer> curr_rids = tuple_why_prov_mappings.get(md5_mappings.get(head_val));
      for(Integer rid: curr_rids)
      {
        ArrayList<String> query_provenance_values = view.evaluate_args_with_provenance2(values_from_why_tokens.get(rid), tuple, q_why_column_ids, v_why_column_ids);
        String q_provenance_value = "(";
        for(int i = 0; i < query_provenance_values.size(); i++)
        {
          if(i >= 1)
            q_provenance_value += ",";
          q_provenance_value += query_provenance_values.get(i);
        }
        q_provenance_value += ")";
        if(q_provenance_count.get(q_provenance_value)== null)
        {
          q_provenance_count.put(q_provenance_value, 1);
        }
        else
        {
          q_provenance_count.put(q_provenance_value, q_provenance_count.get(q_provenance_value)+1);
        }
        HashSet<Integer> rids = new HashSet<Integer>();
        rids.addAll(query_provenance_rid_mappings.get(0).get(query_provenance_values.get(0)));
        for(int i = 1; i<query_provenance_rid_mappings.size(); i++)
        {
           rids.retainAll(query_provenance_rid_mappings.get(i).get(query_provenance_values.get(i)));
        }
        for(Integer r: rids)
        {
          String view_head_values = view_head_vars.get(r);
          if(view_head_vals_tuple_count.get(view_head_values) == null)
          {
            view_head_vals_tuple_count.put(view_head_values, 1);
          }
          else
          {
            view_head_vals_tuple_count.put(view_head_values, view_head_vals_tuple_count.get(view_head_values) + 1);
          }
        }
      }
    }
    
    return view_head_vals_tuple_count;
//    ArrayList<String> all_view_grouping_values = new ArrayList<String>();
//    
////    Vector<Argument> all_view_instantiation_condition_args = new Vector<Argument>();
//    Vector<Subgoal> mapped_subgoals = new Vector<Subgoal>();
//    for(int i = 0; i<view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple).size(); i++)
//    {
//      int subgoal_id = view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple).get(i);
//      Subgoal subgoal = view.subgoals.get(subgoal_id);
//      mapped_subgoals.add(subgoal);
//      all_view_instantiation_condition_args.add(subgoal);
//    }
//    
//    String view_instantiation_where_clause = gen_instantiation_condition_string2(mapped_subgoals);
//    
//    for(String head_val : head_values)
//    {
//      ArrayList<Integer> curr_rids = tuple_why_prov_mappings.get(md5_mappings.get(head_val));
//      
////      ArrayList<Head_strs> curr_view_provenance_values = new ArrayList<Head_strs>();
//      
//      for(Integer rid: curr_rids)
//      {
//        String q_provenance_value = view.evaluate_args_with_provenance2(values_from_why_tokens.get(rid), tuple);
//        
//        if(q_provenance_count.get(q_provenance_value)== null)
//        {
//          q_provenance_count.put(q_provenance_value, 1);
//        }
//        else
//        {
//          q_provenance_count.put(q_provenance_value, q_provenance_count.get(q_provenance_value)+1);
//        }
////        curr_view_provenance_values.add(view_grouping_values);
//        
//        view_provenance_values.put(q_provenance_value, head_val);
//        all_view_grouping_values.add((q_provenance_value));
//
//      }
//      
//      
//    }
//    
////    System.out.println(head_values);
//    
//    HashMap<String, String> sql_clauses = Query_converter.data2sql_check_count_grouping_values(view);
//    HashMap<String, Integer> view_head_vals_tuple_count = cal_count_partitions(all_view_grouping_values, view_instantiation_where_clause, sql_clauses, view.head.args.size());
//
//    
////    String sql = Query_converter.data2sql_check_count_grouping_values(view, view_provenance_values.keySet(), view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple));
////    
////    pst = c.prepareStatement(sql);
////    
////    System.out.println(sql);
////    
////    ResultSet rs = pst.executeQuery();
//    
////    HashSet<Head_strs> potentail_invalid_head_values = null;
////    
////    if(has_having_clause)
////    {
////      potentail_invalid_head_values = new HashSet<Head_strs>();
////      
////      potentail_invalid_head_values.addAll(view_head_value_query_head_value_mappings.keySet());
////    }
//    
//    
//    
////    while(rs.next())
////    {
////      Vector<String> h_values = new Vector<String>();
////      for(int i = 0; i<view.head.args.size(); i++)
////      {
////        h_values.add(rs.getString(i + 1));
////      }
////      
////      int tuple_count = rs.getInt(view.head.args.size() + 1);
////      
////      Head_strs view_head_values = new Head_strs(h_values);
////      
////      view_head_vals_tuple_count.put(view_head_values, tuple_count);
////      
////    }
//    
//    return view_head_vals_tuple_count;
  }
  
  void compare_view_grouping_value_count(HashSet<String> q_head_values, HashMap<String, String> view_grouping_value_q_grouping_value_mappings, HashMap<String, Integer> view_grouping_value_tuple_count_mappings1, HashMap<String, Integer> view_grouping_value_tuple_count_mappings2, HashMap<String, String> view_provenance_values)
  {
    Set<String> view_grouping_value_sets = view_grouping_value_q_grouping_value_mappings.keySet();
    
    HashSet<String> deleted_q_grouping_values = new HashSet<String>();
    
    for(String viwe_grouping_value: view_grouping_value_sets)
    {
      Integer count1 = view_grouping_value_tuple_count_mappings1.get(viwe_grouping_value);
      
      Integer count2 = view_grouping_value_tuple_count_mappings2.get(viwe_grouping_value);
      
      if(count2 == null)
        continue;
      
      String q_grouping_value = view_grouping_value_q_grouping_value_mappings.get(viwe_grouping_value);
      
      if(!count1.equals(count2))
      {
        deleted_q_grouping_values.add(q_grouping_value);
        
        q_head_values.remove(q_grouping_value);
        
      }
    }
    
    view_provenance_values.entrySet().removeIf(e->  deleted_q_grouping_values.contains(e.getValue()));
    
  }
  
  
  HashMap<String, Integer> compute_provenance_count_in_view(Tuple tuple, HashMap<String, String> view_provenance_values, Vector<Subgoal> view_instantiated_with_provenance_args) throws SQLException
  {
//    HashSet<Head_strs> q_grouping_values = tuple_rows.get(tuple);
    
    Vector<Integer> view_why_prov_ids = view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    ArrayList<String> query_provenance_values = new ArrayList<String>();
    for(String query_provenance: view_provenance_values.keySet())
    {
      query_provenance_values.add(query_provenance);
    }
    
    HashMap<String, String> sql_clauses = Query_converter.data2sql_compute_count_grouping_values(view, view_why_prov_ids);
    HashMap<String, Integer> q_provenance_count2 = cal_count_partitions(query_provenance_values, gen_instantiation_condition_string2(view_instantiated_with_provenance_args), sql_clauses, view_instantiated_with_provenance_args.size());
    
//    String sql = Query_converter.data2sql_compute_count_grouping_values(view, view_provenance_values.keySet(), view_why_prov_ids);
//    
//    pst = c.prepareStatement(sql);
//    
//    System.out.println(sql);
//    
////    HashMap<Head_strs, Integer> q_provenance_count2 = new HashMap<Head_strs, Integer>();
//    
//    ResultSet rs = pst.executeQuery();
//    
//    int total_arg_count = 0;
//    for(int i = 0; i<view_why_prov_ids.size(); i++)
//    {
//      total_arg_count += view.subgoals.get(view_why_prov_ids.get(i)).size();
//    }
//    
//    while(rs.next())
//    {
//      Vector<String> provenance_values = new Vector<String>();
//      for(int i = 0; i<total_arg_count; i++)
//      {
//        provenance_values.add(rs.getString(i + 1));
//      }
//      
//      int count = rs.getInt(total_arg_count + 1);
//      
//      q_provenance_count2.put(new Head_strs(provenance_values), count);
//    }
    
    return q_provenance_count2;
  }
  
  void compare_provenance_count_query_and_view(Tuple tuple, HashMap<String, Integer> q_provenance_count1, HashMap<String, Integer> q_provenance_count2, HashMap<String, String> view_provenance_values)
  {
    for(Entry<String, String> entry : view_provenance_values.entrySet())
    {
      int count1 = q_provenance_count1.get(entry.getKey());
      
      int count2 = q_provenance_count2.get(entry.getKey());
      
      if(count1 != (count1/count2)*count2)
      {
        String q_grouping_values = entry.getValue();
        
        tuple_rows.get(tuple).remove(q_grouping_values);
      }
    }
  }
  
  static String gen_value_partitions(ArrayList<String> where_clause_values, int start, int end)
  {
    StringBuilder sb = new StringBuilder();
    for(int i = start; i<end; i++)
    {
      if(i >= start + 1)
        sb.append(",");
      sb.append(where_clause_values.get(i));
    }
    return sb.toString();
  }
  
  void merge_result(HashMap<Head_strs, Integer> res1, HashMap<Head_strs, Integer> res2)
  {
    for(Entry<Head_strs, Integer> entry: res2.entrySet())
    {
      if(res1.get(entry.getKey()) == null)
      {
        res1.put(entry.getKey(), entry.getValue());
      }
      else
      {
        res1.put(entry.getKey(), entry.getValue() + res1.get(entry.getKey()));
      }
    }
  }
  
  void retrive_result(ResultSet rs, int head_size, HashMap<String, Integer> res) throws SQLException
  {
//     = new HashMap<Head_strs, Integer>();
    while(rs.next())
    {
//      Vector<String> grouping_value = new Vector<String>();
//      for(int i = 0; i<1; i++)
//      {
//        grouping_value.add(rs.getString(i + 1));
//      }
      String grouping_value = rs.getString(1);
      int count = rs.getInt(2);
      res.put(grouping_value, count);
    }
//    return res;
  }
  
  int retrive_result(ResultSet rs, int count, int head_size, HashMap<String, Integer> res, HashMap<String, String> view_grouping_value_query_grouping_value_mappings, ArrayList<HashMap<String, HashSet<Integer>>> query_provenance_rid_mappings, ArrayList<String> view_head_vars, HashMap<String, Integer> q_provenance_count2) throws SQLException
  {
//     = new HashMap<Head_strs, Integer>();
    
    while(rs.next())
    {
//      Vector<String> grouping_value = new Vector<String>();
//      for(int i = 0; i<1; i++)
//      {
//        grouping_value.add(rs.getString(i + 1));
//      }
      String q_provenance_value = "(";
      for(int i = 0; i<head_size; i++)
      {
        String head_var = rs.getString(i + 1);
        if(i >= 1)
          q_provenance_value += ",";
        q_provenance_value += head_var;
        if(query_provenance_rid_mappings.get(i).get(head_var) == null)
        {
          HashSet<Integer> rids = new HashSet<Integer>();
          rids.add(count);
          query_provenance_rid_mappings.get(i).put(head_var, rids);
        }
        else
        {
          query_provenance_rid_mappings.get(i).get(head_var).add(count);
        }
      }
      
      q_provenance_value += ")";
      if(q_provenance_count2.get(q_provenance_value) == null)
      {
        q_provenance_count2.put(q_provenance_value, 1);
      }
      else
      {
        q_provenance_count2.put(q_provenance_value, q_provenance_count2.get(q_provenance_value) + 1);
      }
      
      count++;
      
      String view_head_values = rs.getString(head_size + 1);
      view_head_vars.add(view_head_values);
      String query_head_values = rs.getString(head_size + 2);
      if(res.get(view_head_values) == null)
      {
        res.put(view_head_values, 1);
      }
      else
      {
        res.put(view_head_values, res.get(view_head_values) + 1);
      }
      view_grouping_value_query_grouping_value_mappings.put(view_head_values, query_head_values);
    }
    return count;
//    return res;
  }
  
  static String get_partially_instantiated_view_instance_where_clauses(String where_clause_attrs, ArrayList<String> where_clause_values, int start, int end)
  {
    return where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start, end) + ")";
  }
  
  static String compose_with_clauses(Single_view view, HashMap<String, String> sql_clauses, String partial_instant_where_condition)
  {
    String string = view.local_with_clause + "," + sql_clauses.get("with_head") + " as ( select " + sql_clauses.get("with_select") 
            + sql_clauses.get("with_select_agg") + " from " + sql_clauses.get("with_from") + " where " + partial_instant_where_condition;
    
    if(sql_clauses.get("with_where") != null)
    {
      string += " and " + sql_clauses.get("with_where");
    }
    
    if(sql_clauses.get("with_group_by") != null)
    {
      string += " group by " + sql_clauses.get("with_group_by"); 
    }
    
    if(sql_clauses.get("with_having") != null)
    {
      string += " having " + sql_clauses.get("with_having");
    }
    string += ")";
    
    return string;
  }
  
  HashMap<String, Integer> cal_count_partitions(ArrayList<String> where_clause_values, String where_clause_attrs, HashMap<String, String> sql_other_clauses, int grouping_attr_num, HashMap<String, String> view_grouping_value_query_grouping_value_mappings, ArrayList<HashMap<String, HashSet<Integer>>> query_provenance_rid_mappings, ArrayList<String> view_head_vars, HashMap<String, Integer> q_provenance_count2) throws SQLException
  {
    int partition_size = 10000;//(int) Math.sqrt(where_clause_values.size());
    
    int num = 0;
    
    HashMap<String, Integer> counts_per_group = new HashMap<String, Integer>();
    long t1 = System.nanoTime();
    int count = 0;
    while(num < where_clause_values.size())
    {
      int start = num;
      int end = (start + partition_size) < where_clause_values.size() ? (start + partition_size): where_clause_values.size();
      
      String partial_instant_where_condition = get_partially_instantiated_view_instance_where_clauses(where_clause_attrs, where_clause_values, start, end);
      
      String sql = compose_with_clauses(view, sql_other_clauses, partial_instant_where_condition) + " select ";
      
      sql += sql_other_clauses.get("select");
//      if(sql_other_clauses.get("select") != null)
//      {
        // + sql_other_clauses.get("select_agg");
//      }
//      else
//      {
//        sql += sql_other_clauses.get("select_agg");
//      }
      
      sql += " from " + sql_other_clauses.get("from");
            long t3 = System.nanoTime();
      if(sql_other_clauses.get("where") != null)
      {
        sql += " where " + sql_other_clauses.get("where");// + " and " + partial_instant_where_condition;
      }
//      else
//      {
//        sql += " where " + partial_instant_where_condition;
//      }
      
      long t4 = System.nanoTime();
      double time = (t4 - t3)*1.0/1000000000;
      System.out.println("string_time::" + time);
      
//      if(sql_other_clauses.get("group_by") != null)
//      {
//        sql += " group by " + sql_other_clauses.get("group_by"); 
//      }
//      
//      if(sql_other_clauses.get("having") != null)
//      {
//        sql += " having " + sql_other_clauses.get("having");
//      }
      
//      System.out.println(sql);
//      System.out.println(sql);
      
      pst = c.prepareStatement(sql);
      
      t3 = System.nanoTime();
      ResultSet rs = pst.executeQuery();
      t4 = System.nanoTime();
      time = (t4 - t3)*1.0/1000000000;
      System.out.println("sql_time::" + time);
      t3 = System.nanoTime();
      count = retrive_result(rs, count, view.view_mapping_q_why_prov_token_col_ids_mapping.size(), counts_per_group, view_grouping_value_query_grouping_value_mappings, query_provenance_rid_mappings, view_head_vars, q_provenance_count2);
      t4 = System.nanoTime();
      time = (t4 - t3)*1.0/1000000000;
      System.out.println("retrieve_time::" + time);
//      counts_per_group.putAll();
//      merge_result(counts_per_group, retrive_result(rs, grouping_attr_num));
      System.out.println(num);
      num += partition_size;
    }
    
    long t2 = System.nanoTime();
    double time = (t2 - t1)*1.0/1000000000;
    System.out.println("sql_time::" + time);
    
    return counts_per_group;
    
  }
  
  HashMap<String, Integer> cal_count_partitions(ArrayList<String> where_clause_values, String where_clause_attrs, HashMap<String, String> sql_other_clauses, int grouping_attr_num) throws SQLException
  {
    int partition_size = 2000;//(int) Math.sqrt(where_clause_values.size());
    
    int num = 0;
    
    HashMap<String, Integer> counts_per_group = new HashMap<String, Integer>();
    long t1 = System.nanoTime();
    while(num < where_clause_values.size())
    {
      String sql = view.local_with_clause + " select ";
      
      if(sql_other_clauses.get("select") != null)
      {
        sql += sql_other_clauses.get("select") + sql_other_clauses.get("select_agg");
      }
      else
      {
        sql += sql_other_clauses.get("select_agg");
      }
      
      sql += " from " + sql_other_clauses.get("from");
      int start = num;
      int end = (start + partition_size) < where_clause_values.size() ? (start + partition_size): where_clause_values.size();
      
      if(sql_other_clauses.get("where") != null)
      {
        sql += " where " + sql_other_clauses.get("where") + " and " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start, end) + ")";
      }
      else
      {
        sql += " where " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start, end) + ")";
      }
      
      
      
      if(sql_other_clauses.get("group_by") != null)
      {
        sql += " group by " + sql_other_clauses.get("group_by"); 
      }
      
      if(sql_other_clauses.get("having") != null)
      {
        sql += " having " + sql_other_clauses.get("having");
      }
      
//      System.out.println(sql);
//      System.out.println(sql);
      
      pst = c.prepareStatement(sql);
      
      
      ResultSet rs = pst.executeQuery();
//      t2 = System.nanoTime();
//      time = (t2 - t1)*1.0/1000000000;
//      System.out.println("sql_time::" + time);
      retrive_result(rs, grouping_attr_num, counts_per_group);
//      counts_per_group.putAll();
//      merge_result(counts_per_group, retrive_result(rs, grouping_attr_num));
      System.out.println(num);
      num += partition_size;
    }
    
    long t2 = System.nanoTime();
    double time = (t2 - t1)*1.0/1000000000;
    System.out.println("sql_time::" + time);
    
    return counts_per_group;
    
  }
  
  void deal_with_view_with_aggregation() throws SQLException
  {

//    System.out.println(tuple_why_prov_mappings);
    
    for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
    {
//      view.reset_values();
      
      Tuple tuple = (Tuple) iter2.next();
      System.out.println(tuple_rows.get(tuple).size());
      if(tuple_rows.get(tuple).isEmpty())
        continue;
      
      //store the count of the tuples in each view grouping values (instantiated by the grouping values from provenance)
      HashMap<String, Integer> view_grouping_value_tuple_count_mappings1 = null;
      
    //build mappings between view grouping values and query grouping values
      HashMap<String, String> view_grouping_value_q_grouping_value_mappings = new HashMap<String, String>();
      
      ArrayList<HashMap<String, HashSet<Integer>>> query_provenance_rid_mappings = new ArrayList<HashMap<String, HashSet<Integer>>>();
      
      for(int k = 0; k<view.view_mapping_q_why_prov_token_col_ids_mapping.size(); k++)
      {
        HashMap<String, HashSet<Integer>> query_provenance_rid_mapping = new HashMap<String, HashSet<Integer>>();
        query_provenance_rid_mappings.add(query_provenance_rid_mapping);
      }
      
      ArrayList<String> view_head_vars = new ArrayList<String>();
      HashMap<String, Integer> q_provenance_count2 = new HashMap<String, Integer>();
      if(view.has_having_clause)
        view_grouping_value_tuple_count_mappings1 = count_tuples_within_view_groups_given_aggregate_values(tuple, view_grouping_value_q_grouping_value_mappings, true, query_provenance_rid_mappings, view_head_vars, q_provenance_count2);
      else
        view_grouping_value_tuple_count_mappings1 = count_tuples_within_view_groups_given_aggregate_values(tuple, view_grouping_value_q_grouping_value_mappings, false, query_provenance_rid_mappings, view_head_vars, q_provenance_count2);
      
            
//      System.out.println("second_test::" + tuple_rows.get(tuple) + "::" + tuple_rows.get(tuple).isEmpty());
      
      System.out.println("second_test::");
      
      if(!tuple_rows.get(tuple).isEmpty())
      {
        //store the mappings between the query provenance and query grouping values
        HashMap<String, String> query_provenance_grouping_value_mappings = new HashMap<String, String>();
        
        //store the count of the provenance expression in query
        HashMap<String, Integer> q_provenance_count1 = new HashMap<String, Integer>();
        
        Vector<Subgoal> view_instantiated_with_provenance_args = new Vector<Subgoal>();
        //store the count of the tuples in each view grouping values (instantiated by the provenance)
        HashMap<String, Integer> view_grouping_value_tuple_count_mappings2 = count_tuples_within_view_groups_given_provenance(tuple, query_provenance_grouping_value_mappings, q_provenance_count1, view_instantiated_with_provenance_args, query_provenance_rid_mappings, view_head_vars);
        
//        System.out.println("view_grouping_value_count1::" + view_grouping_value_tuple_count_mappings1);
        
//        System.out.println("view_grouping_value_count2::" + view_grouping_value_tuple_count_mappings2);
        
//        System.out.println("view_grouping_value_query_grouping_value_mappings::" + view_grouping_value_q_grouping_value_mappings);
        
        compare_view_grouping_value_count(tuple_rows.get(tuple), view_grouping_value_q_grouping_value_mappings, view_grouping_value_tuple_count_mappings1, view_grouping_value_tuple_count_mappings2, query_provenance_grouping_value_mappings);
        
//        System.out.println("third_test::" + tuple_rows.get(tuple) + "::" + tuple_rows.get(tuple).isEmpty());
        System.out.println("third_test::");
        if(!tuple_rows.get(tuple).isEmpty())
        {
        
          //store the count of the provenance expression in view
//          HashMap<String, Integer> q_provenance_count2 = compute_provenance_count_in_view(tuple, query_provenance_grouping_value_mappings, view_instantiated_with_provenance_args);
          System.out.println("q_count2::" + q_provenance_count2.size());
          System.out.println("q_count1::" + q_provenance_count1.size());
          
          compare_provenance_count_query_and_view(tuple, q_provenance_count1, q_provenance_count2, query_provenance_grouping_value_mappings);
          
//          System.out.println("fourth_test::" + tuple_rows.get(tuple) + "::" + tuple_rows.get(tuple).isEmpty());
          System.out.println("fourth_test::");
          System.out.println(tuple_rows.get(tuple).size());
        }
      }
    }
  
  }
  
  
  static String get_value_seq(Head_strs values)
  {
    String string = new String();
    
    for(int i = 0; i<values.head_vals.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      string += values.head_vals.get(i);
      
    }
    
    return string;
    
  }
  
  static Vector<Argument> get_grouping_attrs(Tuple tuple, Query query, Single_view view)
  {
    Vector<Argument> args = (Vector<Argument>) tuple.args.clone();
    
    args.retainAll(query.head.args);
    
    Vector<Argument> selected_args = new Vector<Argument>();
    
    for(int i = 0; i < view.head.args.size(); i++)
    {
      Argument arg = (Argument) view.head.args.get(i);
      
      if(args.contains(tuple.phi.apply(arg)))
      {
        selected_args.add(arg);
      }
    }
    
    return selected_args;
    
  }
  
  static Vector<Argument> get_attribute_in_view(Tuple tuple, Vector<Argument> arg_in_q)
  {
    Vector<Argument> args = new Vector<Argument>();
    
    for(int i = 0; i<arg_in_q.size(); i++)
    {
      args.add(tuple.reverse_phi.apply(arg_in_q.get(i)));
    }
    
    return args;
  }
  
  static Vector<Vector<Argument>> get_agg_attribute_in_view(Tuple tuple, Vector<Vector<Argument>> arg_in_q)
  {
    Vector<Vector<Argument>> args = new Vector<Vector<Argument>>();
    
    for(int i = 0; i<arg_in_q.size(); i++)
    {
      Vector<Argument> curr_agg_args = new Vector<Argument>();
      
      for(int k = 0; k<arg_in_q.get(i).size(); k++)
      {
        curr_agg_args.add(tuple.reverse_phi.apply(arg_in_q.get(i).get(k)));
      }
      
      args.add(curr_agg_args);
    }
    
    return args;
  }
  
  void partially_instantiate_views_with_grouping_values(Tuple tuple, HashSet<Integer> rids, Vector<Head_strs> head_vals) throws SQLException
  {
    
    Vector<Argument> selected_args = get_attribute_in_view(tuple, query.head.args);
    
    Vector<Vector<Argument>> grouping_args = get_agg_attribute_in_view(tuple, tuple.target_agg_args);
    
    Vector<Integer> grouping_arg_ids = tuple.target_agg_ids;
    
    Vector<Integer> view_tokens = view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    String sql = Query_converter.data2sql_partial_instantiation_with_grouping_values(view, selected_args, grouping_args, tuple.target_agg_functions, head_vals, view.subgoals, view_tokens);
    
    System.out.println(sql);
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
   
    rids.clear();
    
    head_vals.clear();
    
    while(rs.next())
    {
      Vector<String> grouping_values = new Vector<String>();
      
      Vector<String> agg_values = new Vector<String>();
      
      for(int i = 0; i<selected_args.size(); i++)
      {
        grouping_values.add(rs.getString(i + 1));
      }
      
      for(int i = 0; i<grouping_args.size(); i++)
      {
        agg_values.add(rs.getString(selected_args.size() + i + 1));
      }
      
      Head_strs grouping_vals = new Head_strs(grouping_values);
      
      Head_strs target_agg_vals = query_grouping_value_agg_value_mapping.get(grouping_vals);
      
      for(int i = 0; i<grouping_arg_ids.size(); i++)
      {
        String target_agg_val = target_agg_vals.head_vals.get(grouping_arg_ids.get(i));
        
        String agg_val = agg_values.get(i);
        
        if(agg_val.equals(target_agg_val))
        {
          rids.addAll(tuple_why_prov_mappings.get(grouping_vals));
          
          head_vals.add(grouping_vals);
        }
      }
      
    }
    
  }
    
  void partially_instantiate_views_with_provenance(Tuple tuple, HashSet<Integer> rids, Vector<Head_strs> head_vals) throws SQLException
  {
    
    Vector<Argument> selected_args = get_attribute_in_view(tuple, query.head.args);
    
    Vector<Vector<Argument>> grouping_args = get_agg_attribute_in_view(tuple, tuple.target_agg_args);
    
    Vector<Integer> grouping_arg_ids = tuple.target_agg_ids;
    
    Vector<Integer> view_tokens = view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    String [] provenance_table_sqls = get_provenance_table_sql(tuple, rids, values_from_why_tokens, view_tokens, view);
    
    String sql = Query_converter.data2sql_partial_instantiation_with_provenance_values(view, selected_args, grouping_args, tuple.target_agg_functions, provenance_table_sqls, view.subgoals, view_tokens);
    
    pst = c.prepareStatement(sql);
    
    System.out.println(sql);
    
    ResultSet rs = pst.executeQuery();
   
    rids.clear();
    
    head_vals.clear();
    
    while(rs.next())
    {
      Vector<String> grouping_values = new Vector<String>();
      
      Vector<String> agg_values = new Vector<String>();
      
      for(int i = 0; i<selected_args.size(); i++)
      {
        grouping_values.add(rs.getString(i + 1));
      }
      
      for(int i = 0; i<grouping_args.size(); i++)
      {
        agg_values.add(rs.getString(selected_args.size() + i + 1));
      }
      
      Head_strs grouping_vals = new Head_strs(grouping_values);
      
      Head_strs target_agg_vals = query_grouping_value_agg_value_mapping.get(grouping_vals);
      
      for(int i = 0; i<grouping_arg_ids.size(); i++)
      {
        String target_agg_val = target_agg_vals.head_vals.get(grouping_arg_ids.get(i));
        
        String agg_val = agg_values.get(i);
        
        if(agg_val.equals(target_agg_val))
        {
          rids.addAll(tuple_why_prov_mappings.get(grouping_vals));
          
          head_vals.add(grouping_vals);
        }
      }
      
    }
    
  }
  
  static String[] get_provenance_table_sql(Tuple tuple, HashSet<Integer> rids, ArrayList<Vector<Head_strs>> values_from_why_tokens, Vector<Integer> view_tokens, Single_view view)
  {
    String[] rel_sqls = new String[view_tokens.size()];
    
    for(int i = 0; i<rel_sqls.length; i++)
    {
      rel_sqls[i] = "(VALUES";
    }
    
    int count = 0;
    
    for(Integer rid : rids)
    {
      Vector<Head_strs> all_provenances = values_from_why_tokens.get(rid);
      
      Vector<Head_strs> provenances = view.get_values_from_why_tokens(tuple, all_provenances);
      
      
      if(count >= 1)
      {
        for(int i = 0; i<rel_sqls.length; i++)
        {
          rel_sqls[i] += ",";
        }
      }
      
      for(int i = 0; i<rel_sqls.length; i++)
      {
        rel_sqls[i] += "(" + get_value_seq(provenances.get(i)) + ")";
      }
      
      count ++;
      
    }
    
    for(int i = 0; i<rel_sqls.length; i++)
    {
      rel_sqls[i] += ") as " + get_provenances_table_schema(view.subgoals.get(i));
    }
    
    return rel_sqls;
  }
  
  static String get_provenances_table_schema(Subgoal subgoal)
  {
    String string = subgoal.name + "_copy(";
    
    for(int i = 0; i<subgoal.args.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      string += subgoal.args.get(i).toString().replace("|", "_");
    }
    
    string += ")";
    
    return string;
  }
  
  void deal_with_view_view_non_aggregation()
  {
    for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
    {
//      view.reset_values();
      
      Tuple tuple = (Tuple) iter2.next();
      
//      System.out.println(tuple.name + "|" + tuple.mapSubgoals_str);
      
      //each table -> related table -> arg_list
      
      ConcurrentHashMap<String, ArrayList<Conditions>> undermined_table_conditions_mappings = new ConcurrentHashMap<String, ArrayList<Conditions>>();
      
      ConcurrentHashMap<String, ArrayList<ArrayList<String>>> undetermined_table_arg_value_mappings = new ConcurrentHashMap<String, ArrayList<ArrayList<String>>>();
      
      long [] bit_sequence = Bit_operation.init(values_from_why_tokens.size());
      
      HashSet<String> head_values = new HashSet<String>();
      
//      tuple_rows_bit_index.put(tuple, bit_sequence);
      
      tuple_rows.put(tuple, head_values);
      
      boolean first = true;
      
      ArrayList<String[][]> partial_mapping_values = new ArrayList<String[][]>();
      
      for(int i = 0; i<tuple.cluster_patial_mapping_condition_ids.size(); i++)
      {
        HashSet<String> partial_mapping_subgoals = get_unique_partial_mapping_subgoals(view, tuple, i);
        
        String[][] curr_partial_mapping_values = new String[values_from_why_tokens.size()][partial_mapping_subgoals.size()];
        
        partial_mapping_values.add(curr_partial_mapping_values);
      }
      
      Set<Head_strs> head_vals = tuple_why_prov_mappings.keySet();
      
      Head_strs[] head_val_array = new Head_strs[values_from_why_tokens.size()];
      
      HashSet<Integer> valid_row_ids = new HashSet<Integer>();
      
      for(Head_strs head_val: head_vals)
      {
        ArrayList<Integer> curr_rids = tuple_why_prov_mappings.get(head_val);
        
        boolean valid4curr_head_val = true;
        
        for(Integer curr_rid : curr_rids)
        {
          view.evaluate_args(values_from_why_tokens.get(curr_rid), tuple);
          
          head_val_array[curr_rid] = head_val;
          
          if(!view.check_validity(tuple))//rel_attr_value_mappings, undermined_table_conditions_mappings, undetermined_table_arg_value_mappings, first, c, pst))
          {
            
            valid4curr_head_val = false;
            
            break;
            
          }
          else
          {
            view.get_partial_mapping_values(tuple, partial_mapping_values, curr_rid);
          }
          
        }
        
        if(valid4curr_head_val)
        {
          String md5_string = convert_head_string2string(head_val);
          
          tuple_rows.get(tuple).add(md5_string);
          
          if(md5_mappings.get(md5_string) == null)
            md5_mappings.put(md5_string, head_val);
          
          valid_row_ids.addAll(curr_rids);
        }
        
      }
      
//      for(int i = 0; i<values_from_why_tokens.size(); i++)
//      {
//        view.evaluate_args(values_from_why_tokens.get(i), tuple);
//        
//        if(view.check_validity(tuple, partial_mapping_values, i))//rel_attr_value_mappings, undermined_table_conditions_mappings, undetermined_table_arg_value_mappings, first, c, pst))
//        {
//          
////              if(undermined_table_conditions_mappings.size() == 0)              
//          {
////            Bit_operation.set_bit(tuple_rows_bit_index.get(tuple), i);
//            
//            tuple_rows.get(tuple).add(i);
//          }
//          
//        }
//        
////        first = false;
//      }
//      System.out.println(tuple_rows.get(tuple));

      try {
        get_valid_row_ids(tuple, partial_mapping_values, tuple_rows.get(tuple), head_val_array, valid_row_ids, view.conditions, view.subgoals, view.subgoal_name_mappings, c, pst);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
//      System.out.println(tuple_rows.get(tuple));
      
    }
  }
  
  public void run() {
//     System.out.println("Running " +  threadName );
//     try {
//        for(int i = 4; i > 0; i--) {
//           System.out.println("Thread: " + threadName + ", " + i);
//           // Let the thread sleep for a while.
//           Thread.sleep(50);
//        }
//     } catch (InterruptedException e) {
//        System.out.println("Thread " +  threadName + " interrupted.");
//     }
//     System.out.println("Thread " +  threadName + " exiting.");
    
//    if(!view.head.has_agg)
    deal_with_view_view_non_aggregation();
    System.out.println("first_test::");
    if(view.head.has_agg)
    {
      try {
        deal_with_view_with_aggregation();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    
  }
  
  static HashSet<String> get_unique_partial_mapping_subgoals(Single_view view, Tuple tuple, int i)
  {
    HashSet<String> partial_join_mapped_attribute_names = new HashSet<String>();
    
    for(Integer condition_id : tuple.cluster_patial_mapping_condition_ids.get(i))
    {
      Conditions condition = view.conditions.get(condition_id);
      
      Conditions dst_condition = tuple.phi.apply(condition);
      
      if(dst_condition == null)
        continue;
      
      Argument arg2 = condition.arg2;
      
      Argument arg1 = condition.arg1;
      
      if(dst_condition.get_mapping2)
      {
        if(!partial_join_mapped_attribute_names.contains(arg2.name))
        {
          partial_join_mapped_attribute_names.add(arg2.name);
          
        }
      }
      
      if(dst_condition.get_mapping1)
      {
        if(!partial_join_mapped_attribute_names.contains(arg1.name))
        {
          partial_join_mapped_attribute_names.add(arg1.name);

        }
        
      }
      
      
    }
    
    return partial_join_mapped_attribute_names;
  }
  
//  static void get_valid_row_ids(Tuple tuple, ArrayList<String[][]> partial_mapping_values, HashSet<Integer> row_ids, Vector<Conditions> conditions, Vector<Subgoal> subgoals, HashMap<String, String> subgoal_name_mappings, Connection c, PreparedStatement pst) throws SQLException
//  {
//    
////    System.out.println(tuple);
//
//    String sql_base = "select t.row_id from (VALUES ";
//    
//
//    
//    for(int i = 0; i<tuple.cluster_subgoal_ids.size(); i++)
//    {
//      if(row_ids.isEmpty())
//        return;
//      
//      if(tuple.cluster_patial_mapping_condition_ids.get(i).size() <= 0)
//        continue;
//      
////      System.out.println(tuple.cluster_subgoal_ids);
////      
////      System.out.println(tuple.cluster_patial_mapping_condition_ids);
////      
////      System.out.println(tuple.cluster_non_mapping_condition_ids);
//      
//      String[][] curr_partial_mapping_values = partial_mapping_values.get(i);
//      
//      String sql = sql_base;
//      
//      String join_condition = new String();
//      
//      boolean first_id = true;
//      
//      for(Integer id: row_ids)
//      {
//        String [] curr_values = curr_partial_mapping_values[id];
//      
//        if(!first_id)
//          sql += ",";
//        
//        sql += "(" + id;
//        
//        for(int k = 0; k<curr_values.length; k++)
//        {
//            sql += "," + curr_values[k];
//          
//        }
//        
//        sql += ")";
//        
//        first_id = false;
//      }
//      
//      sql += ") as t(row_id ";
//      
//      int join_condition_count = 0;
//      
////      HashSet<String> subgoal_names = new HashSet<String>();
//      
//      HashSet<String> partial_join_mapped_attribute_names = new HashSet<String>();
//      
//      for(Integer id: tuple.cluster_patial_mapping_condition_ids.get(i))
//      {
//        Conditions condition = conditions.get(id);
//        
//        Argument arg1 = condition.arg1;
//        
//        Argument arg2 = condition.arg2;
//        
//        String subgoal_name1 = condition.subgoal1;
//        
//        String subgoal_name2 = condition.subgoal2;
//        
////        subgoal_names.add(condition.subgoal1);
////        
////        subgoal_names.add(condition.subgoal2);
//        
//        if(tuple.mapSubgoals_str.get(subgoal_name2) == null)
//        {
//          String partial_join_mapped_attribute_name = arg1.name.replaceAll("\\" + init.separator, "_");
//          
//          if(!partial_join_mapped_attribute_names.contains(partial_join_mapped_attribute_name))
//          {
//            sql += "," + partial_join_mapped_attribute_name;
//            
//            partial_join_mapped_attribute_names.add(partial_join_mapped_attribute_name);
//          }
//          
//          if(join_condition_count >= 1)
//            join_condition += " and ";
//          
//          join_condition += "t." + arg1.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg2.name.replaceFirst("\\" + init.separator, ".");
//
//        }
//        else
//        {
//          String partial_join_mapped_attribute_name = arg2.name.replaceAll("\\" + init.separator, "_");
//          
//          if(!partial_join_mapped_attribute_names.contains(partial_join_mapped_attribute_name))
//          {
//            sql += "," + partial_join_mapped_attribute_name;
//            
//            partial_join_mapped_attribute_names.add(partial_join_mapped_attribute_name);
//          }
//          
////          sql += "," + partial_join_mapped_attribute_name;
//          
//          if(join_condition_count >= 1)
//            join_condition += " and ";
//          
//          join_condition += "t." + arg2.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg1.name.replaceFirst("\\" + init.separator, ".");
//
//        }
//        
//                
//        join_condition_count ++;
//                        
//      }
//      
//      for(Integer id: tuple.cluster_non_mapping_condition_ids.get(i))
//      {
//        Conditions condition = conditions.get(id);
//        
//        Argument arg1 = condition.arg1;
//        
//        Argument arg2 = condition.arg2;
//        
//        if(join_condition_count >= 1)
//          join_condition += " and ";
//        
//        join_condition += arg1.name.replaceFirst("\\" + init.separator, ".") + condition.op.toString() + arg2.name.replaceFirst("\\" + init.separator, ".");
//        
//        join_condition_count ++;
//      }
//      
//      
//      sql += ")";
//      
//      for(Integer id: tuple.cluster_subgoal_ids.get(i))
//      {
//        Subgoal subgoal = subgoals.get(id);
//        
////        if(subgoal_names.contains(subgoal.name))
////        {
////          continue;
////        }
//        
//        String origin_name = subgoal_name_mappings.get(subgoal.name);
//        
//        sql += "," + origin_name + " " + subgoal.name;
//        
//      }
//      
////      for(String subgoal_name: subgoal_names)
////      {
////        String origin_name = subgoal_name_mappings.get(subgoal_name);
////        
////        sql += "," + origin_name + " " + subgoal_name;
////      }
//      
//      if(!join_condition.isEmpty())
//      {
//        sql += " where " + join_condition;
//      }
//      
////      if(tuple.toString().equals("v4|family0=family,introduction2=introduction"))
////      {
//        System.out.println(sql);
////        
////      }
//      
//      
//      pst = c.prepareStatement(sql);
//      
//      ResultSet rs = pst.executeQuery();
//      
////      HashSet<Integer> curr_valid_row_ids = new HashSet<Integer>();
//      
//      row_ids.clear();
//      
//      while(rs.next())
//      {
//        row_ids.add(rs.getInt(1));
//      }
//      
////      if(tuple.toString().equals("v4|family0=family,introduction2=introduction"))
////      {
////        System.out.println(row_ids);
////      }
//    }
//    
////    System.out.println(row_ids.size());
//    
//    
////    Set<String> undetermined_relations = undermined_table_conditions_mappings.keySet();
////    
////    HashSet<Integer> valid_row_ids = null;
////    
////    boolean first = true;
////    
////    for(String table: undetermined_relations)
////    {
////      String origin_relation_name = subgoal_name_mappings.get(table);
////      
////      String sql = sql_base;
////      
////      ArrayList<ArrayList<String>> values = undetermined_table_arg_value_mappings.get(table);
////      
////      ArrayList<Conditions> conditions = undermined_table_conditions_mappings.get(table);
////      
////      for(int i = 0; i<values.size(); i++)
////      {
////        
////        if(i >= 1)
////          sql += ",";
////        
////        sql += "(" + i;
////        
////        for(int j = 0; j<values.get(i).size(); j++)
////        {
////          sql += "," + values.get(i).get(j);
////        }
////        
////        sql += ")";
////      }
////      
////      sql += ") as t(row_id, ";
////      
////      String condition_str = new String();
////      
////      for(int i = 0; i < conditions.size(); i++)
////      {
////        
////        if(i >= 1)
////          condition_str += ",";
////        
////        Argument arg2 = conditions.get(i).arg2;
////
////        Argument arg1 = conditions.get(i).arg1;
////        
////        String arg2_name = arg2.name.replaceAll("\\" + init.separator, "_"); 
////        
////        String [] relation_arg_name = arg1.name.split("\\" + init.separator);
////        
////        String arg1_name = relation_arg_name[1];
////        
////        condition_str += origin_relation_name + "." + arg1_name + conditions.get(i).op.toString() + "t." + arg2_name;
////        
////        sql += arg2_name;
////        
////      }      
////      
////      sql += ") join " + origin_relation_name + " on (" + condition_str + ")";
////      
////      
////      
////    }
////    
////    row_ids.addAll(valid_row_ids);
//    
//  }
//  
  @Override
  public void start () {
//     System.out.println("Starting " +  threadName );
     if (t == null) {
        t = new Thread (this, threadName);
        t.start ();
     }
  }
  
  @Override
  public void join() throws InterruptedException
  {
    t.join();
  }

  @Override
  public ConcurrentHashMap<Tuple, HashSet<String>> get_tuple_rows() {
    // TODO Auto-generated method stub
    return tuple_rows;
  }
}

