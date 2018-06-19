package edu.upenn.cis.citation.prov_reasoning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import org.json.JSONException;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view1.Covering_set;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.citation_view1.citation_view;
import edu.upenn.cis.citation.citation_view1.citation_view_parametered;
import edu.upenn.cis.citation.citation_view1.citation_view_unparametered;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.gen_citations.Gen_citation;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.multi_thread.Calculate_covering_sets;
import edu.upenn.cis.citation.multi_thread.Calculate_covering_sets_first_round;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_non_agg;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class Prov_reasoning2 {
  
  
  public static Vector<Single_view> view_objs = new Vector<Single_view>();
  
  
  static HashMap<String, Vector<String>> rel_attr_mappings = new HashMap<String, Vector<String>>();
  
  static HashMap<String, Head_strs> tuples = new HashMap<String, Head_strs>();
  
  public static boolean test_case = true;
  
  public static double view_mapping_time = 0.0;
  
  public static double covering_set_time = 0.0;
  
  static int gap = 5;
  
  public static int rows = 0;
  
  public static HashMap<Head_strs, ArrayList<Integer>> tuple_why_prov_mappings = new HashMap<Head_strs, ArrayList<Integer>>();
  
  public static ArrayList<Vector<Head_strs>> all_why_tokens = new ArrayList<Vector<Head_strs>>();
  
  public static HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
  
  public static ResultSet rs = null;
  
  public static HashMap<String, HashMap<String, Vector<Integer>>> relation_attribute_value_mappings = new HashMap<String, HashMap<String, Vector<Integer>>>();

  public static HashMap<Tuple, Vector<Integer>> tuple_valid_rows = new HashMap<Tuple, Vector<Integer>>();
  
  public static boolean agg_intersection = true;
  
  public static HashMap<String, Integer> max_num = new HashMap<String, Integer>();
  
  public static ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_schema_level = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
  
  static HashMap<String, Integer> query_subgoal_id_mappings = new HashMap<String, Integer>();
  
  static HashMap<String, Integer> query_relation_attr_id_mappings = new HashMap<String, Integer>();
  
  public static String get_string()
  {
    String string = new String();
    
    for(int i = 0; i<1000000; i++)
    {
      string += "(family|family_id|" + i +",family|name|"+i+",family|"+i+")";
    }
    
    return string;
  }
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException
  {
//    String string1 = "11";
//    
//    String string2 = "9";
//    
//    System.out.println(string1.compareTo(string2));
    
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
    
//  view_operation.delete_view_by_name("v7", c, pst, false);
  
  view_operation.delete_view_by_name("v5", c, pst, false);
//    Vector<Query> views = Load_views_and_citation_queries.get_views("views", c, pst);
//    
//    test_case = false;
//    
//    for(int i = 0; i<views.size(); i++)
//    {
//      Query view = views.get(i);
//      
//      Single_view view_obj = new Single_view(view, view.name, c, pst);
//      
//      view_objs.add(view_obj);
//    }
//    
//    HashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
//    
//    Vector<Query> query = Load_views_and_citation_queries.get_views("query", c, pst);
//    
//    HashSet<citation_view_vector> covering_sets = reasoning(query.get(0), curr_valid_view_mappings, c, pst);
//    
//    System.out.println(covering_sets);
    
    c.close();
    
  }
  
  public static void init_from_files(String file_name, Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> views = Load_views_and_citation_queries.get_views(file_name, c, pst);
    
    for(int i = 0; i<views.size(); i++)
    {
      Query view = views.get(i);
      
      Single_view view_obj = new Single_view(view, view.name, c, pst);
      
      view_objs.add(view_obj);
    }
  }

  public static void init_from_database(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> views = get_all_views(c, pst);
    
    for(int i = 0; i<views.size(); i++)
    {
      Single_view curr_view_obj = new Single_view(views.get(i), views.get(i).name, c, pst);
      
      view_objs.add(curr_view_obj);
    }
  }
  
  
  static Vector<Query> get_all_views(Connection c, PreparedStatement pst) throws SQLException
  {
    return view_operation.get_all_views(c, pst);
  }
  
  
  static void clone_view_mappings(HashMap<Single_view, HashSet<Tuple>> view_mappings, HashMap<Single_view, HashSet<Tuple>> view_mappings_copy)
  {
    Set<Single_view> views = view_mappings.keySet();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = view_mappings.get(view);
      
      view_mappings_copy.put(view, (HashSet<Tuple>) tuples.clone());
      
    }
  }
  
  
  
  
//  static ArrayList<HashMap<Single_view, HashSet<Tuple>>> reasoning_covering_sets_conjunctive_query(Query user_query, Connection c, PreparedStatement pst) throws SQLException
//  {
//    HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = get_all_possible_view_mappings(user_query);
//    
//    String sql = Query_converter.data2sql_with_token_columns(user_query);
//    
//    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//    
//    ResultSet rs = pst.executeQuery();
//    
//    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
//    
//    int num = 0;
//    
//    while(rs.next())
//    {
//      
//      if(num == 0)
//      {        
//        for(int i = 0; i<user_query.head.args.size(); i++)
//        {
//          valid_view_mappings_per_head_var.add(clone_view_mappings(all_possible_view_mappings));
//        }
//      }
//      
//      Vector<String> why_tokens = new Vector<String>();
//      
//      for(int i = 0; i < user_query.body.size(); i++)
//      {
//        why_tokens.add(rs.getString(user_query.head.args.size() + i + 1));
//        
//      }
//      
//      checking_why_provenance_tokens(why_tokens, all_possible_view_mappings);
//      
//      for(int i = 0; i<user_query.head.args.size(); i++)
//      {
//        String where_token = rs.getString(i + 1);
//        
//        check_where_tokens(where_token, valid_view_mappings_per_head_var.get(i));
//        
//        checking_where_why_provenance_tokens(valid_view_mappings_per_head_var.get(i), where_token, why_tokens, all_possible_view_mappings);
//      }
//      
//      num++;
//      
//    }
//    
//    return valid_view_mappings_per_head_var;
//    
//  }
//  
  static Vector<String> get_curr_where_token_seq(ResultSet rs, Vector<String> where_token_seqs, int subgoal_size, int head_arg_size) throws SQLException
  {
    
    Vector<String> where_tokens = new Vector<String>();
    
    for(int i = 0; i<head_arg_size; i++)
    {
      String where_token = rs.getString(i+1);
      
      where_token = where_token.replaceAll("\\|", "\\\\|");
      
      where_tokens.add(where_token);
      
      if(i >= where_token_seqs.size())
      {
        
        //(?=.*\bjack\b)
        String where_token_seq = "^(?=.*\\b" + where_token + "\\b)";
        
        where_token_seqs.add(where_token_seq);
        
      }
      else
      {
        String where_token_seq = where_token_seqs.get(i);
        
        where_token_seq += "(?=.*\\b" + where_token + "\\b)";
        
        where_token_seqs.set(i, where_token_seq);
        
      }
    }
    
    return where_tokens;
  }
  
  static Vector<String> get_curr_why_token_seq(ResultSet rs, Vector<String> why_token_seqs, int subgoal_size, int head_arg_size, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings) throws SQLException
  {
    Vector<String> why_tokens = new Vector<String>();
    
    for(int i = head_arg_size; i<subgoal_size + head_arg_size; i++)
    {
      String why_token = rs.getString(i+1);
      
      why_token = why_token.replaceAll("\\|", "\\\\|");
      
      why_tokens.add(why_token);
    }
    
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    int num = 0;
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
        Tuple tuple = (Tuple) iter2.next();
        
        String why_token_seq = view.get_q_why_provenance_token_seq(why_tokens, tuple);
        
        if(num >= why_token_seqs.size())
        {
          why_token_seqs.add(why_token_seq);
        }
        else
        {
          String curr_why_token_seq = why_token_seqs.get(num);
          
          curr_why_token_seq += why_token_seq;
          
          why_token_seqs.set(num, curr_why_token_seq);
        }
        
        num++;
      }
      
    }
    
    return why_tokens;
  }
  
  
  static void get_curr_where_why_token_seq(ResultSet rs, Vector<String> where_tokens, Vector<String> why_tokens, Vector<HashMap<Single_view, Vector<String>>> where_why_token_seqs, int subgoal_size, int head_arg_size, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings) throws SQLException
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    for(int i = 0; i<where_tokens.size(); i++)
    {
      
      String where_token = where_tokens.get(i);
      
      HashMap<Single_view, Vector<String>> where_why_token_seq;
      
      if(i >= where_why_token_seqs.size())
      {
        where_why_token_seq = new HashMap<Single_view, Vector<String>>();
        
        where_why_token_seqs.add(where_why_token_seq);
      }
      else
      {
        where_why_token_seq = where_why_token_seqs.get(i);
      }
      
      
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
        
        int num = 0;
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          Tuple tuple = (Tuple) iter2.next();
          
          String why_token_seq = view.get_q_where_why_provenance_token_seq(where_token, why_tokens, tuple);
          
          if(where_why_token_seq.get(view) == null)
          {
            
            Vector<String> curr_token_seqs = new Vector<String>();
            
            curr_token_seqs.add(why_token_seq);
            
            where_why_token_seq.put(view, curr_token_seqs);
          }
          else
          {
            
            Vector<String> curr_why_token_seqs = where_why_token_seq.get(view);
            
            if(num >= curr_why_token_seqs.size())
            {
              curr_why_token_seqs.add(why_token_seq);
            }
            else
            {
              String curr_why_token_seq = curr_why_token_seqs.get(num);
              
              curr_why_token_seq += why_token_seq;
              
              curr_why_token_seqs.set(num, curr_why_token_seq);
            }
          }
          
          num++;
        }
        
      }
    }
  }
  
  static void init_view_mappings_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> head_args, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {    
    for(int i = 0; i<head_args.size(); i++)
    {
      HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = new HashMap<Single_view, HashSet<Tuple>>(); 
      
      clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
      
      Set<Single_view> views = all_possible_view_mappings_copy.keySet();
      
      Argument arg = head_args.get(i);
            
      String arg_rel_name = arg.relation_name;
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = all_possible_view_mappings_copy.get(view);
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          Tuple tuple = (Tuple) iter2.next();
          
          if(!tuple.args.contains(arg))
          {
            iter2.remove();
          }
          
//          if(i == 0)
//          {
//            HashSet<String> target_subgoal_names = tuple.getTargetSubgoal_strs();
//            
//            tuple_targeted_subgoal_mappings.put(tuple, target_subgoal_names);
//            
//            if(!tuple.args.contains(arg))
//            {
//              iter2.remove();
//            }
//          }
//          else
//          {
//            HashSet<String> target_subgoal_names = tuple_targeted_subgoal_mappings.get(tuple);
//            
//            if(!target_subgoal_names.contains(arg_rel_name))
//            {
//              iter2.remove();
//            }
//          }
          
        }
        
        if(tuples.isEmpty())
          iter.remove();
        
      }
      
      if(views.isEmpty())
      {
        continue;
      }
      
      valid_view_mappings_per_head_var.add(all_possible_view_mappings_copy);
      
    }
    
    
    
    
    
  }
  
  static void add_suffix_token_seq(Vector<String> token_seqs)
  {
    for(int i = 0; i<token_seqs.size(); i++)
    {
      String token_seq = token_seqs.get(i);
      
      token_seq += ".*$";
      
      token_seqs.set(i, token_seq);
    }
  }
  
  
  static void input_single_relation(HashMap<String, Integer> relation_attr_id_mappings, String relation, Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<String> attributes = init.get_attributes_single_relation(relation, c, pst);
    
    rel_attr_mappings.put(relation, attributes);
    
    String query = "select * from " + relation;
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    String col_name_encoding = MD5.get_MD5_encoding(relation);
    
    String prov_col_name = "c" + init.separator + col_name_encoding + init.provenance_column_suffix;
    
    
    ResultSetMetaData meta_data = rs.getMetaData();
    
    for(int i = 0; i<meta_data.getColumnCount(); i++)
    {
      String attr_name = meta_data.getColumnLabel(i + 1);
      
      relation_attr_id_mappings.put(relation + init.separator + attr_name, i);
    }
    
    
    while(rs.next())
    {
      Vector<String> values = new Vector<String>();
      
      for(int i = 0; i<attributes.size(); i++)
      {
        String value = rs.getString(attributes.get(i));
        
        values.add(value);
        
      }
      
      Head_strs tuple = new Head_strs(values);
      
      String token = rs.getString(prov_col_name);
      
      tuples.put(token, tuple);
    }
    
  }
  
  static void input_relations(HashMap<String, Integer> relation_attr_id_mappings, HashSet<String> tables, Connection c, PreparedStatement pst) throws SQLException
  {
    for(Iterator it = tables.iterator(); it.hasNext();)
    {
      String relation = (String) it.next();
      
      input_single_relation(relation_attr_id_mappings, relation, c, pst);
      
    }
  }
  
  static Vector<Head_strs> get_tuples(ResultSet rs, Vector<Argument> args, Vector<Subgoal> subgoals) throws SQLException
  {
    Vector<Head_strs> curr_tuples = new Vector<Head_strs>();
    
    for(int i = args.size(); i < subgoals.size() + args.size(); i++)
    {
      String token = rs.getString(i + 1);
      
      Head_strs curr_tuple = tuples.get(token);
      
      curr_tuples.add(curr_tuple);
    }
    
    return curr_tuples;
    
  }
  
  static void check_valid_view_mappings(Vector<Head_strs> curr_tuples, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      
      
    }
  }
  
  static void evaluate_views(Vector<Head_strs> curr_tuples, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings, Connection c, PreparedStatement pst) throws InterruptedException, SQLException
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    
    Vector<Check_valid_view_mappings_non_agg> check_threads = new Vector<Check_valid_view_mappings_non_agg>();
    
//    for(Iterator iter = views.iterator(); iter.hasNext();)
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
//      
//      Check_valid_view_mappings check_thread = new Check_valid_view_mappings(view.view_name, view, tuples, curr_tuples);
//      
//      check_thread.start();
//      
//      check_threads.add(check_thread);
//      
//    }
//    
//    for(int i = 0; i<check_threads.size(); i++)
//    {
//      check_threads.get(i).join();
//    }
//    
//    for(Iterator iter = views.iterator(); iter.hasNext();)
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
//      
//      if(tuples.isEmpty())
//        iter.remove();
//            
//    }
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
//        view.reset_values();
        
        Tuple tuple = (Tuple) iter2.next();
        
        view.evaluate_args(curr_tuples, tuple);
        
        if(!view.check_validity(tuple, relation_attribute_value_mappings, c, pst))
          iter2.remove();
      }
      
      if(tuples.isEmpty())
        iter.remove();
      
    }
  }
  
  static void evaluate_views(ArrayList<Vector<Head_strs>> curr_tuples, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings, Connection c, PreparedStatement pst) throws InterruptedException
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    
    Vector<Check_valid_view_mappings_non_agg> check_threads = new Vector<Check_valid_view_mappings_non_agg>();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      Check_valid_view_mappings_non_agg check_thread = new Check_valid_view_mappings_non_agg(view.view_name, view, tuples, curr_tuples, relation_attribute_value_mappings, c, pst);
      
      check_thread.start();
      
      check_threads.add(check_thread);
      
    }
    
    for(int i = 0; i<check_threads.size(); i++)
    {
      check_threads.get(i).join();
    }
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      if(agg_intersection)
      {
        for(Tuple tuple: tuples)
        {
          if(tuple_valid_rows.get(tuple).size() < rows)
            tuples.remove(tuple);
        }
      }
      else
      {
        for(Tuple tuple: tuples)
        {
          if(tuple_valid_rows.get(tuple).isEmpty())
            tuples.remove(tuple);
        }
      }
      
           
      if(tuples.isEmpty())
        iter.remove();
            
    }
    
//    for(Iterator iter = views.iterator(); iter.hasNext();)
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
//      
//      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
//      {
////        view.reset_values();
//        
//        Tuple tuple = (Tuple) iter2.next();
//        
//        for(int i = 0; i<curr_tuples.size(); i++)
//        {
//          view.evaluate_args(curr_tuples.get(i), tuple);
//          
//          if(!view.check_validity(tuple))
//          {
//            iter2.remove();
//            break;
//          }
//        }
//        
//      }
//      
//      if(tuples.isEmpty())
//        iter.remove();
//      
//    }
  }
  
  static void get_valid_view_mappings(HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings, ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var)
  {
    
    HashSet<HashSet<Tuple>> all_tuples = new HashSet<HashSet<Tuple>>();
    
    for(int i = 0; i<valid_view_mappings_per_head_var.size(); i++)
    {
      HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(i);
      
      Set<Single_view> views = valid_view_mappings.keySet();
      
      HashSet<Tuple> curr_tuples = new HashSet<Tuple>();
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples1 = valid_view_mappings.get(view);
        
        HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
        
        if(tuples2 == null)
        {
          iter.remove();
          
          continue;
        }
        
        tuples1.retainAll(tuples2);
        
        if(tuples1.isEmpty())
          iter.remove();
        
        curr_tuples.addAll(tuples1);
        
      }
      
      if(all_tuples.contains(curr_tuples))
      {
        valid_view_mappings_per_head_var.remove(i);
        
        i--;
      }
      else
        all_tuples.add(curr_tuples);
    }
  }
  
  static Head_strs get_query_result(ResultSet rs, int head_arg_size) throws SQLException
  {
    Vector<String> values = new Vector<String>();
    
    for(int i = 0; i<head_arg_size; i++)
    {
      String value = rs.getString(i + 1);
      
      values.add(value);
    }
    
    Head_strs curr_query_result = new Head_strs(values);
    
    return curr_query_result;
  }
  
  static ArrayList<HashMap<Single_view, HashSet<Tuple>>> reasoning_valid_view_mappings_conjunctive_query(Query user_query, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, ResultSet rs, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    
    HashSet<String> tables = new HashSet<String>();
    
    for(int i = 0; i<user_query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) user_query.body.get(i);
      
      query_subgoal_id_mappings.put(subgoal.name, i);
      
      tables.add(user_query.subgoal_name_mapping.get(subgoal));
    }
    
    input_relations(query_relation_attr_id_mappings, tables, c, pst);
    
    all_possible_view_mappings = get_all_possible_view_mappings(query_subgoal_id_mappings, user_query);
    
    clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
    
    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
        
    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head.args, all_possible_view_mappings_copy);    
    
    while(rs.next())
    {
      
      Vector<Head_strs> curr_tuples = get_tuples(rs, user_query.head.args, user_query.body);
      
      Head_strs values = get_query_result(rs, user_query.head.args.size());
      
      if(tuple_why_prov_mappings.get(values) == null)
      {
        ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
        
        curr_tokens.add(rows);
        
        tuple_why_prov_mappings.put(values, curr_tokens);
        
      }
      else
      {
        tuple_why_prov_mappings.get(values).add(rows);
      }
      
      all_why_tokens.add(curr_tuples);
      
      rows ++;
//      Vector<String> where_tokens = get_curr_where_token_seq(rs, where_token_seqs, subgoal_size, head_var_size);
//      
//      Vector<String> why_tokens = get_curr_why_token_seq(rs, why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
//      
//      get_curr_where_why_token_seq(rs, where_tokens, why_tokens, where_why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
      
    }
    
    evaluate_views(all_why_tokens, all_possible_view_mappings_copy, c, pst);

    
    get_valid_view_mappings(all_possible_view_mappings_copy, valid_view_mappings_per_head_var);
    
//    add_suffix_token_seq(where_token_seqs);
//    
//    check_where_tokens(where_token_seqs, valid_view_mappings_per_head_var, where_why_token_seqs);
//    
//    checking_why_provenance_tokens(why_token_seqs, all_possible_view_mappings);
//
//    
//    checking_where_why_provenance_tokens(valid_view_mappings_per_head_var, where_why_token_seqs, all_possible_view_mappings);
    
    return valid_view_mappings_per_head_var;
    
  }
  
  static ArrayList<Vector<Head_strs>> get_sel_tokens(Vector<Head_strs> sel_values)
  {
    ArrayList<Vector<Head_strs>> all_tokens = new ArrayList<Vector<Head_strs>>();
    
    for(int i = 0; i<sel_values.size(); i++)
    {
      
      ArrayList<Integer> row_ids = tuple_why_prov_mappings.get(sel_values.get(i));
      
      for(int j = 0; j<row_ids.size(); j++)
      {
        all_tokens.add(all_why_tokens.get(row_ids.get(j)));
      }
      
    }
    
    return all_tokens;
  }
  
  static HashSet<Covering_set> reasoning_multi_tuples(Vector<Head_strs> values, Query user_query, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings = reasoning_valid_view_mappings_conjunctive_query_multi_tuples(user_query, values, c, pst);
    
    HashSet<Covering_set> covering_sets = reasoning_covering_set_multi_threads_multi_hops_conjunctive_query(valid_view_mappings, user_query.head.args, true);

    return covering_sets;
    
  }
  
  static ArrayList<HashMap<Single_view, HashSet<Tuple>>> reasoning_valid_view_mappings_conjunctive_query_multi_tuples(Query user_query, Vector<Head_strs> sel_values, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {    
    HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = new HashMap<Single_view, HashSet<Tuple>>(); 
        
    clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
        
    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
    
    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head.args, all_possible_view_mappings);
    
    ArrayList<Vector<Head_strs>> all_tuples = get_sel_tokens(sel_values);//new ArrayList<Vector<Head_strs>>();

    evaluate_views(all_tuples, all_possible_view_mappings_copy, c, pst);
    
    get_valid_view_mappings(all_possible_view_mappings_copy, valid_view_mappings_per_head_var);
    
    return valid_view_mappings_per_head_var;
    
  }
  
  public static HashSet<Covering_set> reasoning(Query user_query, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, boolean multi_thread, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    String sql = new String();
    
    
    if(!test_case)
      sql = Query_converter.data2sql_with_why_token_columns(user_query);
    else
      sql = Query_converter.data2sql_with_why_token_columns_test(user_query);
      
    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
  
    ResultSet rs = pst.executeQuery();
    
    
    double start = 0.0;
    
    double end = 0.0;
    
    start = System.nanoTime();
    
    valid_view_mappings_schema_level = reasoning_valid_view_mappings_conjunctive_query(user_query, all_possible_view_mappings_copy, rs, c, pst);
    
    end = System.nanoTime();
    
    view_mapping_time = (end - start) * 1.0/1000000000;
    
    start = System.nanoTime();
    
    HashSet<Covering_set> covering_sets;
    
    if(!multi_thread)
    {
      covering_sets = reasoning_covering_set_multi_hops_conjunctive_query(valid_view_mappings_schema_level, user_query.head.args, true);
    }
    
    else
    {
      covering_sets = reasoning_covering_set_multi_threads_multi_hops_conjunctive_query(valid_view_mappings_schema_level, user_query.head.args, true);
    }
    
    end = System.nanoTime();
    
    covering_set_time = (end - start) * 1.0/1000000000;
    
    return covering_sets;
  }
  
  public static HashSet<String> gen_citations(HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, HashSet<Covering_set> covering_sets, Connection c, PreparedStatement pst) throws SQLException, JSONException
  {
    HashSet<String> formatted_citations = Gen_citation.gen_citation_entire_query(all_possible_view_mappings_copy, covering_sets, tuple_valid_rows, all_why_tokens, max_num, c, pst);
    
    return formatted_citations;
  }
  
  static HashSet<Covering_set> reasoning_covering_set_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args)
  {
    
    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(2));
    
    Vector<HashSet<Covering_set>> covering_sets = new Vector<HashSet<Covering_set>>();
    
    for(int i = 1; i<=loop_time; i++)
    {
      int j = 0;
      
      if(i == 1)
      {
        
        for(j = 0; j<valid_view_mappings_per_head_var.size() + 2*i; j = j+2*i)
        {
          HashSet<Covering_set> view_com = new HashSet<Covering_set>();
          
          for(int k = j; k<j+2*i && k < valid_view_mappings_per_head_var.size(); k++)
          {
            HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
            
            Set<Single_view> views = valid_view_mappings.keySet();
            
            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
            
            for(Iterator iter = views.iterator(); iter.hasNext();)
            {
              Single_view view = (Single_view) iter.next();
              
              HashSet<Tuple> tuples = valid_view_mappings.get(view);
              
              all_tuples.addAll(tuples);
              
              
            }
            
            view_com = join_views_curr_relation(all_tuples, view_com, args);
            
          }
          
          if(!view_com.isEmpty())
             covering_sets.add(view_com);
        }
        
        
      }
      else
      {
        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(2*i));
        
        for(int k = 0; k<covering_sets.size(); k=k+2)
        {
          if(k + 1 < covering_sets.size())
          {
            HashSet<Covering_set> updated_covering_set = join_operation(covering_sets.get(k), covering_sets.get(k + 1));
            
            covering_sets.set(k/2, updated_covering_set);
          }
          else
          {
            covering_sets.set(k/2, covering_sets.get(k));
          }
        }
        
        int redundant_start = (covering_sets.size() + 1)/2;
        
        int redundant_end = covering_sets.size();
        
        for(int k = redundant_start; k<redundant_end; k++)
        {
          covering_sets.remove(covering_sets.size() - 1);
        }
      }
    }
    
    return covering_sets.get(0);
  }
  
//  static HashSet<citation_view_vector> reasoning_covering_set_multi_threads_multi_hops_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, boolean multi_thread) throws InterruptedException
//  {
//    
////    System.out.println("multi_thread");
//    
//    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(gap));
//    
//    ArrayList<HashSet<citation_view_vector>> covering_sets = new ArrayList<HashSet<citation_view_vector>>();
//    
//    for(int i = 1; i<=loop_time; i++)
//    {
//      int j = 0;
//      
//      if(i == 1)
//      {
//        
//        ArrayList<Calculate_covering_sets_first_round> cal_threads = new ArrayList<Calculate_covering_sets_first_round>();
//        
//        for(j = 0; j<valid_view_mappings_per_head_var.size() + gap*i; j = j+gap*i)
//        {
////          HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();
////          
////          for(int k = j; k<j+gap*i && k < valid_view_mappings_per_head_var.size(); k++)
////          {
////            HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
////            
////            Set<Single_view> views = valid_view_mappings.keySet();
////            
////            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
////            
////            for(Iterator iter = views.iterator(); iter.hasNext();)
////            {
////              Single_view view = (Single_view) iter.next();
////              
////              HashSet<Tuple> tuples = valid_view_mappings.get(view);
////              
////              all_tuples.addAll(tuples);
////              
////              
////            }
////            
////            view_com = join_views_curr_relation(all_tuples, view_com, args);
////            
////          }
////          if(!view_com.isEmpty())
////            covering_sets.add(view_com);
//          
//          Calculate_covering_sets_first_round cal_thread = new Calculate_covering_sets_first_round(valid_view_mappings_per_head_var, args, j, j+gap*i);
//          
//          cal_thread.start();
//              
//          cal_threads.add(cal_thread);
//        }
//        
//        for(int p = 0; p<cal_threads.size(); p++)
//        {
//          cal_threads.get(p).join();
//        }
//        
//        for(int p = 0; p<cal_threads.size(); p++)
//        {
//          
//          HashSet<citation_view_vector> view_com = cal_threads.get(p).get_reasoning_result();
//          
//          if(!view_com.isEmpty())
//            covering_sets.add(view_com);
//        }
//        
//        
//      }
//      else
//      {
//        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(gap*i));
//        
//        ArrayList<Calculate_covering_sets> cal_threads = new ArrayList<Calculate_covering_sets>(); 
//        
//        
//        for(int k = 0; k<covering_sets.size(); k=k+gap)
//        {
//          if(k + gap - 1 < covering_sets.size())
//          {
//            Calculate_covering_sets cal_thread = new Calculate_covering_sets(covering_sets, k, k + gap);
//            
//            cal_thread.start();
//            
//            cal_threads.add(cal_thread);
////            HashSet<citation_view_vector> updated_covering_set = join_operation();
////            
////            covering_sets.set(k/2, updated_covering_set);
//          }
//          else
//          {
//            
//            Calculate_covering_sets cal_thread = new Calculate_covering_sets(covering_sets, k, covering_sets.size());
//            
//            cal_thread.start();
//            
//            cal_threads.add(cal_thread);
//            
////            covering_sets.set(k/gap, covering_sets.get(k));
//          }
//        }
//        
//        for(int p = 0; p<cal_threads.size(); p++)
//        {
//          cal_threads.get(p).join();
//        }
//        
//        for(int k = 0; k<covering_sets.size(); k = k + gap)
//        {
//          
////          if(k + gap - 1 < covering_sets.size())
//          {
//            HashSet<citation_view_vector> updated_covering_set = cal_threads.get(k/gap).get_reasoning_result();
//            
//            covering_sets.set(k/gap, updated_covering_set);
//          }
//          
//        }
//        
//        
//        int redundant_start = (covering_sets.size() + 1)/gap;
//        
//        int redundant_end = covering_sets.size();
//        
//        for(int k = redundant_start; k<redundant_end; k++)
//        {
//          covering_sets.remove(covering_sets.size() - 1);
//        }
//      }
//    }
//    
//    if(loop_time == 0)
//    {
//      HashMap<Single_view, HashSet<Tuple>> view_mappings = valid_view_mappings_per_head_var.get(0);
//      
//      Set<Single_view> views = view_mappings.keySet();
//      
//      for(Iterator iter = views.iterator(); iter.hasNext();)
//      {
//        Single_view view = (Single_view) iter.next();
//        
//        HashSet<Tuple> tuples = view_mappings.get(view);
//        
//        HashSet<citation_view_vector> curr_covering_sets = new HashSet<citation_view_vector>();
//        
//        for(Tuple tuple: tuples)
//        {          
//          if(tuple.lambda_terms.size() > 0)
//          {
//              
//              citation_view_parametered c = new citation_view_parametered(tuple.name, tuple.query, tuple);
//              
//              citation_view_vector curr_views = new citation_view_vector(c);
//              
//              curr_covering_sets.add(curr_views);
//          }   
//          else
//          {
//              
//              citation_view_unparametered c = new citation_view_unparametered(tuple.name, tuple);
//              
//              citation_view_vector curr_views = new citation_view_vector(c);
//              
//              curr_covering_sets.add(curr_views);
//          }
//        }
//        
//        return curr_covering_sets;
//      }
//    }
//    
//    return covering_sets.get(0);
//  }
//  
  static HashSet<Covering_set> reasoning_covering_set_multi_hops_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, boolean multi_thread) throws InterruptedException
  {
    
    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(gap));
    
    ArrayList<HashSet<Covering_set>> covering_sets = new ArrayList<HashSet<Covering_set>>();
    
    for(int i = 1; i<=loop_time; i++)
    {
      int j = 0;
      
      if(i == 1)
      {
        
        for(j = 0; j<valid_view_mappings_per_head_var.size() + gap*i; j = j+gap*i)
        {
          HashSet<Covering_set> view_com = new HashSet<Covering_set>();
          
          for(int k = j; k<j+gap*i && k < valid_view_mappings_per_head_var.size(); k++)
          {
            HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
            
            Set<Single_view> views = valid_view_mappings.keySet();
            
            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
            
            for(Iterator iter = views.iterator(); iter.hasNext();)
            {
              Single_view view = (Single_view) iter.next();
              
              HashSet<Tuple> tuples = valid_view_mappings.get(view);
              
              all_tuples.addAll(tuples);
              
              
            }
            
            view_com = join_views_curr_relation(all_tuples, view_com, args);
            
            
          }
          
          if(!view_com.isEmpty())
            covering_sets.add(view_com);
          
        }
              
      }
      else
      {
        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(gap*i));
        
        for(int k = 0; k<covering_sets.size(); k=k+gap)
        {
          
          int start = 0;
          
          int end = 0;
          
          
          if(k + gap - 1 < covering_sets.size())
          {
            
            start = k;
            
            end = k + gap;
          }
          else
          {
            
            start = k;
            
            end = covering_sets.size();
            
          }
          
          HashSet<Covering_set> resulting_covering_set = covering_sets.get(start);
          
          for(int p = start + 1; p<end; p++)
          {
            resulting_covering_set = join_operation(resulting_covering_set, covering_sets.get(p));

          }
          
          covering_sets.set(k/gap, resulting_covering_set);
        }
        
        
        int redundant_start = (covering_sets.size() + 1)/gap;
        
        int redundant_end = covering_sets.size();
        
        for(int k = redundant_start; k<redundant_end; k++)
        {
          covering_sets.remove(covering_sets.size() - 1);
        }
      }
    }
    
    if(loop_time == 0)
    {
      HashMap<Single_view, HashSet<Tuple>> view_mappings = valid_view_mappings_per_head_var.get(0);
      
      Set<Single_view> views = view_mappings.keySet();
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = view_mappings.get(view);
        
        HashSet<Covering_set> curr_covering_sets = new HashSet<Covering_set>();
        
        for(Tuple tuple: tuples)
        {          
          if(tuple.lambda_terms.size() > 0)
          {
              
              citation_view_parametered c = new citation_view_parametered(tuple.name, tuple.query, tuple);
              
              Covering_set curr_views = new Covering_set(c);
              
              curr_covering_sets.add(curr_views);
          }   
          else
          {
              
              citation_view_unparametered c = new citation_view_unparametered(tuple.name, tuple);
              
              Covering_set curr_views = new Covering_set(c);
              
              curr_covering_sets.add(curr_views);
          }
        }
        
        return curr_covering_sets;
      }
    }
    
    return covering_sets.get(0);
  }
  
  static HashSet<Covering_set> reasoning_covering_set_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, boolean multi_thread) throws InterruptedException
  {
    
    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(2));
    
    ArrayList<HashSet<Covering_set>> covering_sets = new ArrayList<HashSet<Covering_set>>();
    
    for(int i = 1; i<=loop_time; i++)
    {
      int j = 0;
      
      if(i == 1)
      {
        
        for(j = 0; j<valid_view_mappings_per_head_var.size() + 2*i; j = j+2*i)
        {
          HashSet<Covering_set> view_com = new HashSet<Covering_set>();
          
          for(int k = j; k<j+2*i && k < valid_view_mappings_per_head_var.size(); k++)
          {
            HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
            
            Set<Single_view> views = valid_view_mappings.keySet();
            
            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
            
            for(Iterator iter = views.iterator(); iter.hasNext();)
            {
              Single_view view = (Single_view) iter.next();
              
              HashSet<Tuple> tuples = valid_view_mappings.get(view);
              
              all_tuples.addAll(tuples);
              
              
            }
            
            view_com = join_views_curr_relation(all_tuples, view_com, args);
            
          }
          
          if(!view_com.isEmpty())
             covering_sets.add(view_com);
        }
        
        
      }
      else
      {
        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(2*i));
        
        ArrayList<Calculate_covering_sets> cal_threads = new ArrayList<Calculate_covering_sets>(); 
        
        
        for(int k = 0; k<covering_sets.size(); k=k+2)
        {
          if(k + 2 - 1 < covering_sets.size())
          {
            Calculate_covering_sets cal_thread = new Calculate_covering_sets(covering_sets, k, k + 2);
            
            cal_thread.start();
            
            cal_threads.add(cal_thread);
//            HashSet<citation_view_vector> updated_covering_set = join_operation();
//            
//            covering_sets.set(k/2, updated_covering_set);
          }
          else
          {
            covering_sets.set(k/2, covering_sets.get(k));
          }
        }
        
        for(int p = 0; p<cal_threads.size(); p++)
        {
          cal_threads.get(p).join();
        }
        
        for(int k = 0; k<covering_sets.size(); k = k + 2)
        {
          
          if(k + 2 - 1 < covering_sets.size())
          {
            HashSet<Covering_set> updated_covering_set = cal_threads.get(k/2).get_reasoning_result();
            
            covering_sets.set(k/2, updated_covering_set);
          }
          
        }
        
        
        int redundant_start = (covering_sets.size() + 1)/2;
        
        int redundant_end = covering_sets.size();
        
        for(int k = redundant_start; k<redundant_end; k++)
        {
          covering_sets.remove(covering_sets.size() - 1);
        }
      }
    }
    
    return covering_sets.get(0);
  }
  
  public static HashSet<Covering_set> join_operation(HashSet<Covering_set> c_combinations, HashSet<Covering_set> insert_citations)
  {
/*      if(i == 0)
      {

          c_combinations.addAll(insert_citations);
          
          return c_combinations;
      }
      else*/
      {
          
          HashSet<Covering_set> updated_c_combinations = new HashSet<Covering_set>();
          
          for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
          {
              
              Covering_set curr_combination1 = (Covering_set) iter.next();
                              
              for(Iterator it = insert_citations.iterator(); it.hasNext();)
              {
                  
                  Covering_set curr_combination2 = (Covering_set)it.next(); 
                  
                  Covering_set new_citation_vec = curr_combination2.clone();
                  
                  Covering_set new_covering_set = curr_combination1.merge(new_citation_vec);
                  
                  remove_duplicate(updated_c_combinations, new_covering_set);
              }
          }
                      
          return updated_c_combinations;
          
      }
  }
  
  public static HashSet<Covering_set> join_views_curr_relation(HashSet<Tuple> tuples, HashSet<Covering_set> curr_view_com, Vector<Argument> args)
  {
      if(curr_view_com.isEmpty())
      {
          if(tuples.isEmpty())
              return new HashSet<Covering_set>();
          else
          {
              HashSet<Covering_set> new_view_com = new HashSet<Covering_set>();
              
              for(Tuple tuple:tuples)
              {
                  
                  Tuple valid_tuple = (Tuple) tuple.clone();
                  
                  valid_tuple.args.retainAll(args);
                                    
                  if(valid_tuple.lambda_terms.size() > 0)
                  {
                      
                      citation_view_parametered c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple);
                      
                      Covering_set curr_views = new Covering_set(c);
                      
                      remove_duplicate(new_view_com, curr_views);
                  }   
                  else
                  {
                      
                      citation_view_unparametered c = new citation_view_unparametered(valid_tuple.name, valid_tuple);
                      
                      Covering_set curr_views = new Covering_set(c);
                      
                      remove_duplicate(new_view_com, curr_views);
                  }
              }
              
              return new_view_com;
          }
      }
      
      else
      {
          HashSet<Covering_set> new_view_com = new HashSet<Covering_set>();
          
          for(Tuple tuple:tuples)
          {
              Tuple valid_tuple = (Tuple)tuple.clone();
              
              valid_tuple.args.retainAll(args);
              
              citation_view c = null;
              
              if(valid_tuple.lambda_terms.size() > 0)
              {
                  
                  c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple);
              }   
              else
              {
                  
                  c = new citation_view_unparametered(valid_tuple.name, valid_tuple);
              }
              
              for(Iterator iter = curr_view_com.iterator(); iter.hasNext();)
              {
                  Covering_set old_view_com = (Covering_set)iter.next();
                  
                  Covering_set old_view_com_copy = old_view_com.clone(); 
                  
                  Covering_set view_com = Covering_set.merge(old_view_com_copy, c);
                  
//                  HashSet<String> string_list = new HashSet<String>();
//                  
//                  for(int j = 0; j<view_com.c_vec.size(); j++)
//                  {
//                      string_list.add(view_com.c_vec.get(j).get_name());
//                  }
//                  
//                if(string_list.contains("v4") && string_list.contains("v8") && string_list.contains("v11") && string_list.contains("v6") && string_list.contains("v14") && string_list.contains("v20"))
//                {
//                    int y = 0;
//                    
//                    y++;
//                }
//                if(string_list.contains("v4") && string_list.contains("v8"))
//                {
//                    int y = 0;
//                    
//                    y++;
//                }
                  
                  remove_duplicate(new_view_com, view_com);
              }
          }
          
          return new_view_com;
      }
  }
  
  public static HashSet<Covering_set> remove_duplicate_arg(HashSet<Covering_set> c_combinations, Covering_set c_view)
  {
      int i = 0;
      
      if(c_combinations.contains(c_view))
          return c_combinations;
              
      for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
      {
//        String str = (String) iter.next();
                      
          Covering_set c_combination = (Covering_set) iter.next();
          {
              {
                  Covering_set curr_combination = c_view;
                  if(view_vector_contains(c_combination, curr_combination)&& curr_combination.head_variables.containsAll(c_combination.head_variables) && c_combination.index_vec.size() > curr_combination.index_vec.size())
                  {
                      iter.remove();                      
                  }
                  
                  if(view_vector_contains(curr_combination, c_combination) && c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
                  {
                      break;
                  }
              }
              
          }
          
          i++;
      }
      
      
      if(i >= c_combinations.size())
          c_combinations.add(c_view);
      
              
      return c_combinations;
  }
  
  public static HashSet<Covering_set> remove_duplicate(HashSet<Covering_set> c_combinations, Covering_set c_view)
  {
      int i = 0;
      
      if(c_combinations.contains(c_view))
          return c_combinations;
              
      for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
      {
//        String str = (String) iter.next();
                      
          Covering_set c_combination = (Covering_set) iter.next();
          
//        if(c_combination.toString().equals("v11*v20*v4*v8"))
//        {
//            int y = 0;
//            
//            y++;
//        }
          {
              {
                  Covering_set curr_combination = c_view;
                  if(view_vector_contains(c_combination, curr_combination)&& table_names_contains(c_combination, curr_combination)&& curr_combination.head_variables.containsAll(c_combination.head_variables) && c_combination.index_vec.size() > curr_combination.index_vec.size())
                  {
                      iter.remove();                      
                  }
                  
                  if(view_vector_contains(curr_combination, c_combination) && table_names_contains(curr_combination, c_combination)&& c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
                  {
                      break;
                  }
              }
              
          }
          
          i++;
      }
      
      
      if(i >= c_combinations.size())
          c_combinations.add(c_view);
      
              
      return c_combinations;
  }
  
  static boolean view_vector_contains(Covering_set c_vec1, Covering_set c_vec2)
  {
      
      String s1 = ".*?";
      
      String s2 = c_vec1.index_str;
      
      for(int i = 0; i<c_vec2.index_vec.size(); i++)
      {
          String str = c_vec2.index_vec.get(i);
          
          str = str.replaceAll("\\(", "\\\\(");
          
          str = str.replaceAll("\\)", "\\\\)");
          
          str = str.replaceAll("\\[", "\\\\[");
          
          str = str.replaceAll("\\]", "\\\\]");
          
          str = str.replaceAll("\\/", "\\\\/");
          
          s1 += "\\(" + str + "\\).*?";
      }
      
      return s2.matches(s1);

  }
  
  static boolean table_names_contains(Covering_set c_vec1, Covering_set c_vec2)
  {
      String s1 = ".*?";
      
      String s2 = c_vec1.table_name_str;
      
      for(citation_view view_mapping: c_vec2.c_vec)
      {
          
          String str = view_mapping.get_table_name_string();
          
          str = str.replaceAll("\\[", "\\\\[");
          
          str = str.replaceAll("\\]", "\\\\]");
          
          s1 += str + ".*?";
          
      }
      
      return s2.matches(s1);
  }
  
  
  static void check_where_tokens(Vector<String> where_tokens, ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings, Vector<HashMap<Single_view, Vector<String>>> where_why_token_seqs)
  {
    
    for(int i = 0; i<where_tokens.size(); i++)
    {
      String curr_where_token = where_tokens.get(i);
      
      HashMap<Single_view,HashSet<Tuple>> curr_valid_view_mappings = valid_view_mappings.get(i);
      
      Set<Single_view> views = curr_valid_view_mappings.keySet();
      
      HashMap<Single_view, Vector<String>> curr_where_why_token_seqs = where_why_token_seqs.get(i);
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = curr_valid_view_mappings.get(view);
        
        if(!view.check_where_provenance_token(curr_where_token))
        {
          iter.remove();
          
          curr_where_why_token_seqs.remove(view);
          
        }
    
      }
      
    }
  }
  
  static void checking_why_provenance_tokens(Vector<String> why_tokens, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    int num = 0;
    
    for(Iterator iter = views.iterator(); iter.hasNext(); )
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
        String curr_why_token = why_tokens.get(num);
        
        Tuple tuple = (Tuple) iter2.next();
        
        if(!view.check_provenance_tokens(curr_why_token))
        {
          iter2.remove();
        }
        
        num++;
      }
      
      if(tuples.isEmpty())
      {
        iter.remove();
      }
      
      
    }
  }
  
  static void checking_where_why_provenance_tokens(ArrayList<HashMap<Single_view, HashSet<Tuple>>> possible_valid_view_mappings, Vector<HashMap<Single_view, Vector<String>>> where_why_tokens, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    
    for(int i = 0; i<possible_valid_view_mappings.size(); i++)
    {
      HashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = possible_valid_view_mappings.get(i);
            
      HashMap<Single_view, Vector<String>> curr_where_why_token_seqs = where_why_tokens.get(i); 
      
      Set<Single_view> views = curr_valid_view_mappings.keySet();
      
      
      
      for(Iterator iter = views.iterator(); iter.hasNext(); )
      {
        Single_view view = (Single_view) iter.next();
        
        Vector<String> curr_token_seqs = curr_where_why_token_seqs.get(view);
        
        HashSet<Tuple> tuples = curr_valid_view_mappings.get(view);
        
        HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
        
        tuples.retainAll(tuples2);
        
        int k = 0;
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          String curr_why_token = curr_token_seqs.get(k);
          
          Tuple tuple = (Tuple) iter2.next();
          
          if(!tuples2.contains(tuple))
          {
            iter2.remove();
            
            k++;
            
            continue;
          }
          
          if(!view.check_provenance_tokens(curr_why_token))
          {
            iter2.remove();
          }
          
          k++;
        }
        
        if(tuples.isEmpty())
        {
          iter.remove();
        }
        
      }
      
    }
//    Set<Single_view> views = possible_valid_view_mappings.keySet();
//    
//    for(Iterator iter = views.iterator(); iter.hasNext(); )
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples1 = possible_valid_view_mappings.get(view);
//      
//      HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
//      
//      tuples1.retainAll(tuples2);
//      
//      view.check_where_why_provenance_tokens(tuples1, where_token, where_why_tokens);
//            
//      if(tuples1.isEmpty())
//      {
//        iter.remove();
//      }
//    }
  }
  
  static HashMap<Single_view, HashSet<Tuple>> get_all_possible_view_mappings(HashMap<String, Integer> subgoal_id_mappings, Query q)
  {
    
    HashMap<Single_view, HashSet<Tuple>> view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
    
    for(int i = 0; i<view_objs.size(); i++)
    {
      Single_view view = view_objs.get(i);
      
      view.build_view_mappings(q.body, q.subgoal_name_mapping, subgoal_id_mappings, tuple_valid_rows);
      
      if(!view.view_mappings.isEmpty())
        view_mappings.put(view, view.view_mappings);
      
    }
    
    return view_mappings;
  }
  
}
