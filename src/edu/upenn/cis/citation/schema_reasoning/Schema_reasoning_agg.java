package edu.upenn.cis.citation.schema_reasoning;

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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import org.gprom.jdbc.driver.GProMConnection;
import org.json.JSONException;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Template;
//import com.apporiented.algorithm.clustering.AverageLinkageStrategy;
//import com.apporiented.algorithm.clustering.Cluster;
//import com.apporiented.algorithm.clustering.ClusteringAlgorithm;
//import com.apporiented.algorithm.clustering.DefaultClusteringAlgorithm;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.CoreCover;
import edu.upenn.cis.citation.Corecover.Database;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
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
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing1;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing1_materialized;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing1_materialized2;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing2;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing3;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_agg_batch_processing_multi_thread;
import edu.upenn.cis.citation.multi_thread.Check_valid_view_mappings_non_agg;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;
import fr.lri.tao.apro.ap.Apro;
import fr.lri.tao.apro.ap.AproBuilder;
import fr.lri.tao.apro.data.DataProvider;
import fr.lri.tao.apro.data.MatrixProvider;

public class Schema_reasoning_agg {
  
  
public static Vector<Single_view> view_objs = new Vector<Single_view>();
  
  
  static ConcurrentHashMap<String, Vector<String>> rel_attr_mappings = new ConcurrentHashMap<String, Vector<String>>();
  
//  static ConcurrentHashMap<String, Head_strs> tuples = new ConcurrentHashMap<String, Head_strs>();
//  public static String view_file_name = Query_provenance.directory+ "views";
//  
//  public static String citation_query_file_name = Query_provenance.directory+"citation_queries";
  
  public static String db_name = "provenance";
  
  public static String view_citation_query_mapping_file_name = Query_provenance.directory+"view_citation_query_mappings";
  
  public static boolean test_case = true;
  
  public static double view_mapping_time = 0.0;
  
  public static double covering_set_time = 0.0;
  
  static int gap = 5;
  
  public static int rows = 0;
  
//  public static ConcurrentHashMap<Head_strs, ArrayList<Integer>> tuple_why_prov_mappings = new ConcurrentHashMap<Head_strs, ArrayList<Integer>>();
  
  public static ConcurrentHashMap<Head_strs, Head_strs> grouping_value_agg_value_mappings = new ConcurrentHashMap<Head_strs, Head_strs>();
  
//  public static ArrayList<Vector<Head_strs>> all_why_tokens = new ArrayList<Vector<Head_strs>>();
  
  public static ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
  
//  public static ResultSet rs = null;
  
  public static ConcurrentHashMap<String, ConcurrentHashMap<String, Vector<Integer>>> relation_attribute_value_mappings = new ConcurrentHashMap<String, ConcurrentHashMap<String, Vector<Integer>>>();

  public static ConcurrentHashMap<Tuple, HashSet> tuple_valid_rows = new ConcurrentHashMap<Tuple, HashSet>();
  
  public static boolean agg_intersection = true;
  
  public static ConcurrentHashMap<String, Integer> max_num = new ConcurrentHashMap<String, Integer>();
  
  public static ArrayList<HashSet<Tuple>> valid_view_mappings_schema_level = new ArrayList<HashSet<Tuple>>();
  
  static ConcurrentHashMap<String, Integer> query_subgoal_id_mappings = new ConcurrentHashMap<String, Integer>();
  
  static ConcurrentHashMap<String, Integer> query_relation_attr_id_mappings = new ConcurrentHashMap<String, Integer>();

//  static ConcurrentHashMap<Argument, Integer> query_arg_id_mappings = new ConcurrentHashMap<Argument, Integer>();
  
  static ConcurrentHashMap<Tuple, ArrayList<Integer>> view_mapping_query_arg_ids_mappings = new ConcurrentHashMap<Tuple, ArrayList<Integer>>();
  
  static ConcurrentHashMap<String, HashSet> group_ids = new ConcurrentHashMap<String, HashSet>();
  
  public static ConcurrentHashMap<String, ConcurrentHashMap<Tuple, Integer>> group_view_mappings = new ConcurrentHashMap<String, ConcurrentHashMap<Tuple, Integer>>();

  public static ConcurrentHashMap<String, HashSet<Covering_set>> group_covering_sets = new ConcurrentHashMap<String, HashSet<Covering_set>>();
  
  static ArrayList<HashSet<Tuple>> valid_view_mappings_per_head_var = new ArrayList<HashSet<Tuple>>();
  
  public static boolean sort_cluster = false;
  
  public static int factor = 1;
  
  static String[] recomputable_agg_function_arr = {"sum", "count", "max", "min"};
  
  static Vector<String> recomputable_agg_functions = new Vector<String>(Arrays.asList(recomputable_agg_function_arr));
  
  static ConcurrentHashMap<String, String[]> reliable_agg_functions = new ConcurrentHashMap<String, String[]>();
  
  public static int batch_size = 5;
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException
  {
    Vector<Integer> index = new Vector<Integer>();
    
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
//    ConcurrentHashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
//    
//    Vector<Query> query = Load_views_and_citation_queries.get_views("query", c, pst);
//    
//    HashSet<citation_view_vector> covering_sets = reasoning(query.get(0), curr_valid_view_mappings, c, pst);
//    
//    System.out.println(covering_sets);
    
    c.close();
    
  }
  
  public static void init(Connection c, PreparedStatement pst) throws SQLException
  {
    String [] r_agg_funs1 = {"SUM", "COUNT"};
    
    reliable_agg_functions.put("AVG", r_agg_funs1);
    
    Single_view.get_relation_primary_key(c, pst);
    
    Single_view.clear_views_in_database(c, pst);
  }
  
  public static void init_from_files(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> views = Load_views_and_citation_queries.get_views(Query_provenance.view_file, c, pst);
//    Vector<Query> citation_queries = Load_views_and_citation_queries.get_views(citation_query_file_name, c, pst);
//    HashMap<String, HashMap<String, String>> view_citation_query_mappings = Load_views_and_citation_queries.get_view_citation_query_mappings(view_citation_query_mapping_file_name);
//    HashMap<String, Query> name_citation_query_mappings = new HashMap<String, Query>();
//    for(int i = 0; i<citation_queries.size(); i++)
//    {
//      name_citation_query_mappings.put(citation_queries.get(i).name, citation_queries.get(i));
//    }
    for(int i = 0; i<views.size(); i++)
    {
      Query view = views.get(i);
      
//      System.out.println(view);
      
      Single_view view_obj = new Single_view(view, view.name, c, pst);
      
      view_objs.add(view_obj);
      
//      view_obj.load_citation_queries(view_citation_query_mappings.get(view.name), name_citation_query_mappings);
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
  
  
  static void clone_view_mappings(ConcurrentHashMap<Single_view, HashSet<Tuple>> view_mappings, ConcurrentHashMap<Single_view, HashSet<Tuple>> view_mappings_copy)
  {
    Set<Single_view> views = view_mappings.keySet();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = view_mappings.get(view);
      
      view_mappings_copy.put(view, (HashSet<Tuple>) tuples.clone());
      
    }
  }
  
  
  
  
//  static ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> reasoning_covering_sets_conjunctive_query(Query user_query, Connection c, PreparedStatement pst) throws SQLException
//  {
//    ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = get_all_possible_view_mappings(user_query);
//    
//    String sql = Query_converter.data2sql_with_token_columns(user_query);
//    
//    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//    
//    ResultSet rs = pst.executeQuery();
//    
//    ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>>();
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
  
  static ConcurrentHashMap<Tuple, Boolean> remove_invalid_view_mappings_schema_level(ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings, Subgoal q_head)
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    ConcurrentHashMap<Tuple, Boolean> strictly_finers = new ConcurrentHashMap<Tuple, Boolean>();
    
    for(Iterator<Single_view> v_iterator = views.iterator(); v_iterator.hasNext();)
    {
      Single_view view = v_iterator.next();
      
      HashSet<Tuple> view_tuples = all_possible_view_mappings.get(view);
      
      for(Iterator<Tuple> iter = view_tuples.iterator(); iter.hasNext();)
      {
        Tuple view_tuple = iter.next();
        
        if(!view_tuple.args.containsAll(q_head.args))
        {
          iter.remove();
        }
        else
        {
          if(q_head.args.containsAll(view_tuple.args))
          {
            strictly_finers.put(view_tuple, false);
          }
          else
          {
            strictly_finers.put(view_tuple, true);
          }
        }
      }
      
      if(view_tuples.isEmpty())
      {
        v_iterator.remove();
      }
      
    }
    
    return strictly_finers;
  }
  
  static boolean deal_with_same_agg_functions(boolean finer_grouping, Tuple tuple, Vector<Argument> q_group_arg, String agg_function, int q_group_agg_id)
  {
    int id = -1;
    
    for(int i = 0; i<tuple.agg_args.size(); i++)
    {
      if(tuple.agg_args.get(i).containsAll(q_group_arg) && q_group_arg.containsAll(tuple.agg_args.get(i)))
      {
        id = i;
      }
    }
//    tuple.agg_args.indexOf(q_group_arg);
    
    if(id < 0)
      return false;
    
    if(!finer_grouping)
    {
      if(tuple.agg_functions.get(id).equals(agg_function))
      {
        if(tuple.target_agg_args == null)
        {
          tuple.target_agg_args = new Vector<Vector<Argument>>();
          
          tuple.target_agg_functions = new Vector<String>();
          
          tuple.target_agg_ids = new Vector<Integer>();
          
        }
        
        tuple.target_agg_args.add(q_group_arg);
        
        tuple.target_agg_functions.add(agg_function);
        
        tuple.target_agg_ids.add(q_group_agg_id);
        
        return true;
      }
      else
        return false;
    }
    else
    {
      if(tuple.agg_functions.get(id).equals(agg_function))
      {
        if(recomputable_agg_functions.contains(agg_function))
        {
          if(tuple.target_agg_args == null)
          {
            tuple.target_agg_args = new Vector<Vector<Argument>>();
            
            tuple.target_agg_functions = new Vector<String>();
            
            tuple.target_agg_ids = new Vector<Integer>();
          }
          
          tuple.target_agg_args.add(q_group_arg);
          
          tuple.target_agg_functions.add(agg_function);
          
          tuple.target_agg_ids.add(q_group_agg_id);
          
          return true;
        }
        else
          return false;
      }
      
    }
    
    return false;
    
    
  }
  
  static boolean deal_with_agg_functions(boolean finer_grouping, Tuple tuple, Vector<Argument> q_group_arg, String q_group_function, int q_group_arg_id)
  {
    int id = 0;
    
    Vector<String> agg_functions = new Vector<String>();
    
    while(id >= 0)
    {
//      id = tuple.agg_args.indexOf(q_group_arg, id + 1);
      
      int i = 0;
      
      for(i = id; i<tuple.agg_args.size(); i++)
      {
        if(tuple.agg_args.get(i).containsAll(q_group_arg) && q_group_arg.containsAll(tuple.agg_args.get(i)))
        {
          id = i;
          break;
        }
      }
      
      if(i >= tuple.agg_args.size())
        break;
      
      
      if(id >= 0)
      {
        String func = (String)tuple.agg_functions.get(id);
        
        agg_functions.add(func);
      }
    }
    
    String[] expected_agg_functions = reliable_agg_functions.get(q_group_function);
    
    
    if(expected_agg_functions != null && agg_functions.containsAll(Arrays.asList(expected_agg_functions)))
    {
      if(!finer_grouping)
      {
        if(tuple.target_agg_args == null)
        {
          tuple.target_agg_args = new Vector<Vector<Argument>>();
          
          tuple.target_agg_functions = new Vector<String>();
          
          tuple.target_agg_ids = new Vector<Integer>();
          
        }
        
        tuple.target_agg_args.add(q_group_arg);
        
        tuple.target_agg_functions.add(q_group_function);
        
        tuple.target_agg_ids.add(q_group_arg_id);
        
        return true;
      }
      else
      {
        for(int i = 0; i<expected_agg_functions.length; i++)
        {
          if(!recomputable_agg_functions.contains(expected_agg_functions[i]))
          {
            return false;
          }
        }
        
        if(tuple.target_agg_args == null)
        {
          tuple.target_agg_args = new Vector<Vector<Argument>>();
          
          tuple.target_agg_functions = new Vector<String>();
          
          tuple.target_agg_ids = new Vector<Integer>();
          
        }
        
        tuple.target_agg_args.add(q_group_arg);
        
        tuple.target_agg_functions.add(q_group_function);
        
        tuple.target_agg_ids.add(q_group_arg_id);
        
        return true;
      }
      
    }
    else
      return false;
    
    
  }
  
  static<K, V> void put_mappings(ConcurrentHashMap<K, ArrayList<V>> maps, K key, V value)
  {
    ArrayList<V> values = maps.get(key);
    
    if(values == null)
    {
      values = new ArrayList<V>();
    }
    
    values.add(value);
    
    maps.put(key, values);
  }
  
  
  static boolean check_conditions_view_mappings(Tuple tuple, Query q)
  {
    int i = 0;
    
    for(i = 0; i<tuple.conditions.size(); i++)
    {
        Conditions cond1 = tuple.conditions.get(i);
        
        int j = 0;
        
        Conditions cond2 = null;
        
        if(q.conditions.size() == 0)
            return false;
        
        for(j = 0; j<q.conditions.size(); j++)
        {
            cond2 = q.conditions.get(j);
            
            if(Conditions.check_predicate_match(cond1, cond2, tuple))
            {
                break;
            }
        }
        
        if(j >= q.conditions.size())
            return false;
        
//        if(Conditions.check_predicates_satisfy(cond1, cond2, q))
//        {
//            continue;
//        }
//        else
//        {
//            return false;
//        }
    }
    
    return true;
  }
  
  static void init_view_mappings_conjunctive_query(ArrayList<HashSet<Tuple>> valid_view_mappings_per_head_var, Subgoal head, ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {    
    
    HashSet<Tuple> candidate_view_mappings = new HashSet<Tuple>();
    
    if(!head.has_agg)
    {
      for(int i = 0; i<head.args.size(); i++)
      {
        ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = new ConcurrentHashMap<Single_view, HashSet<Tuple>>(); 
        
        clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
        
        Set<Single_view> views = all_possible_view_mappings_copy.keySet();
        
        Argument arg = (Argument) head.args.get(i);
              
        String arg_rel_name = arg.relation_name;
        
        HashSet<Tuple> possible_view_mappings_per_head_var = new HashSet<Tuple>();
        
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
            else
            {
              put_mappings(view_mapping_query_arg_ids_mappings, tuple, i);
            }
            
          }
          
          if(tuples.isEmpty())
            iter.remove();
          
          possible_view_mappings_per_head_var.addAll(tuples);
          
        }
        
        if(views.isEmpty())
        {
          continue;
        }
        
        valid_view_mappings_per_head_var.add(possible_view_mappings_per_head_var);
        
        candidate_view_mappings.addAll(possible_view_mappings_per_head_var);
        
      }
    }
    else
    {
      
      ConcurrentHashMap<Tuple, Boolean> strictly_finers = remove_invalid_view_mappings_schema_level(all_possible_view_mappings, head);
      
      for(int i = 0; i<head.agg_args.size(); i++)
      {
        ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = new ConcurrentHashMap<Single_view, HashSet<Tuple>>(); 
        
        clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
        
        Set<Single_view> views = all_possible_view_mappings_copy.keySet();
        
        Vector<Argument> args = head.agg_args.get(i);
        
        String agg_function = (String) head.agg_function.get(i);
        
        HashSet<Tuple> possible_view_mappings_per_head_var = new HashSet<Tuple>();
        
        for(Iterator iter = views.iterator(); iter.hasNext();)
        {
          Single_view view = (Single_view) iter.next();
          
          HashSet<Tuple> tuples = all_possible_view_mappings_copy.get(view);
          
          for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
          {
            Tuple tuple = (Tuple) iter2.next();
            
            if(tuple.agg_args!=null && tuple.agg_args.size() > 0)
            {
              if(tuple.args.containsAll(args) || deal_with_same_agg_functions(strictly_finers.get(tuple), tuple, args, agg_function, i) || deal_with_agg_functions(strictly_finers.get(tuple), tuple, args, agg_function, i))
              {
                put_mappings(view_mapping_query_arg_ids_mappings, tuple, i);
                
                tuple.is_strictly_finer = strictly_finers.get(tuple);
                
                continue;
              }
              else
              {
                iter2.remove();
              }
            }
            else
            {
              if(tuple.args.containsAll(args))
              {
                put_mappings(view_mapping_query_arg_ids_mappings, tuple, i);
                
                tuple.is_strictly_finer = false;
                
                continue;
              }
              else
              {
                iter2.remove();
              }
            }
            

            
//            if(!tuple.args.contains(arg))
//            {
//              iter2.remove();
//            }
          }
          
          if(tuples.isEmpty())
            iter.remove();
          
          possible_view_mappings_per_head_var.addAll(tuples);
          
        }
        
        if(views.isEmpty())
        {
          continue;
        }
        
        valid_view_mappings_per_head_var.add(possible_view_mappings_per_head_var);
        
        candidate_view_mappings.addAll(possible_view_mappings_per_head_var);
        
      }
    }
    
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    for(Single_view view: views)
    {
      HashSet<Tuple> view_mappings = all_possible_view_mappings.get(view);
      
      view_mappings.retainAll(candidate_view_mappings);
      
      all_possible_view_mappings.put(view, view_mappings);
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
  
  
//  static void input_single_relation(ConcurrentHashMap<String, Integer> relation_attr_id_mappings, String relation, Connection c, PreparedStatement pst) throws SQLException
//  {
//    Vector<String> attributes = init.get_attributes_single_relation(relation, c, pst);
//    
//    rel_attr_mappings.put(relation, attributes);
//    
//    String query = "select ";//* from " + relation;
//    
//    for(int i = 0; i<attributes.size(); i++)
//    {
//      if(i >= 1)
//        query += ",";
//      
//      query += attributes.get(i);
//    }
//    
//    String col_name_encoding = MD5.get_MD5_encoding(relation);
//    
//    String prov_col_name = "c" + init.separator + col_name_encoding + init.provenance_column_suffix;
//
//    query += ",\"" + prov_col_name + "\"";
//    
//    query += " from " + relation;
//    
//    pst = c.prepareStatement(query);
//    
//    ResultSet rs = pst.executeQuery();
//    
//    
//    
//    
//    ResultSetMetaData meta_data = rs.getMetaData();
//    
//    for(int i = 0; i<meta_data.getColumnCount(); i++)
//    {
//      String attr_name = meta_data.getColumnLabel(i + 1);
//      
//      relation_attr_id_mappings.put(relation + init.separator + attr_name, i);
//    }
//    
//    
//    while(rs.next())
//    {
//      Vector<String> values = new Vector<String>();
//      
//      for(int i = 0; i<attributes.size(); i++)
//      {
//        String value = rs.getString(attributes.get(i));
//        
//        values.add(value);
//        
//      }
//      
//      Head_strs tuple = new Head_strs(values);
//      
//      String token = rs.getString(prov_col_name);
//      
//      tuples.put(token, tuple);
//    }
//    
//  }
//  
//  static void input_relations(ConcurrentHashMap<String, Integer> relation_attr_id_mappings, HashSet<String> tables, Connection c, PreparedStatement pst) throws SQLException
//  {
//    for(Iterator it = tables.iterator(); it.hasNext();)
//    {
//      String relation = (String) it.next();
//      
//      input_single_relation(relation_attr_id_mappings, relation, c, pst);
//      
//    }
//  }
  
  static Vector<Head_strs> get_tuples(String[] provenance_row, Query query, ConcurrentHashMap<String, Integer> subgoal_attr_nums_mappings) throws SQLException
  {
    Vector<Head_strs> curr_tuples = new Vector<Head_strs>();
    
    Vector<String> provenance = new Vector<String>();
    
    int total_col_count = provenance_row.length;
    
    int col_nums = query.head.args.size();
    
    if(query.head.has_agg)
      col_nums += query.head.agg_args.size();
    
    for(int i = 0; i<query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      String origin_subgoal_name = query.subgoal_name_mapping.get(subgoal.name);
      
      int attri_nums = subgoal_attr_nums_mappings.get(origin_subgoal_name);
      
      provenance.clear();
      
      for(int j = 0; j<attri_nums; j++)
      {
        provenance.add(provenance_row[col_nums]);
        
        col_nums++;
      }
      
      Head_strs curr_tuple = new Head_strs(provenance);
      
      curr_tuples.add(curr_tuple);
    }
    
//    System.out.println(total_col_count + "::" + col_nums);
    
    return curr_tuples;
    
  }
  
  static Vector<Head_strs> get_tuples(ResultSet rs, Query query, ConcurrentHashMap<String, Integer> subgoal_attr_nums_mappings) throws SQLException
  {
    Vector<Head_strs> curr_tuples = new Vector<Head_strs>();
    
    Vector<String> provenance = new Vector<String>();
    
//    int total_col_count = provenance_row.length;
    
    int col_nums = query.head.args.size() + query.head.agg_args.size();
    
    for(int i = 0; i<query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      String origin_subgoal_name = query.subgoal_name_mapping.get(subgoal.name);
      
      int attri_nums = subgoal_attr_nums_mappings.get(origin_subgoal_name);
      
      provenance.clear();
      
      for(int j = 0; j<attri_nums; j++)
      {
        provenance.add(rs.getString(col_nums + 1));
        
        col_nums++;
      }
      
      Head_strs curr_tuple = new Head_strs(provenance);
      
      curr_tuples.add(curr_tuple);
    }
    
//    System.out.println(total_col_count + "::" + col_nums);
    
    return curr_tuples;
    
  }
  
  static void check_valid_view_mappings(Vector<Head_strs> curr_tuples, ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      
      
    }
  }
  
  static HashSet<Tuple> evaluate_views(ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings, Query user_query) throws InterruptedException
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
        
    HashSet<Tuple> all_valid_view_mappings = new HashSet<Tuple>();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
        Tuple tuple = (Tuple) iter2.next();
        
        if(!check_conditions_view_mappings(tuple, user_query))
          iter2.remove();
        else
          all_valid_view_mappings.add(tuple);
          
      }
    }
    
    return all_valid_view_mappings;
//    for(int i = 0; i<check_threads.size(); i++)
//    {
//      check_threads.get(i).join();
//    }
//    
//    for(int i = 0; i<check_threads.size(); i++)
//    {
//      tuple_valid_rows.putAll(check_threads.get(i).get_tuple_rows());
//    }
//    check_threads.clear();
//    
////    for(int i = 0; i<check_threads.size(); i++)
////    {
////      check_threads.get(i).join();
////    }
//    
//
//    
//    int id = 0;
//    
//    if(user_query.head.has_agg)
//      rows = tuple_why_prov_mappings.size();
//    
//    for(Iterator iter = views.iterator(); iter.hasNext();)
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
//      
//      if(agg_intersection)
//      {
//        Iterator it = tuples.iterator();
//        
//        while(it.hasNext())
//        {
//          Tuple tuple = (Tuple)it.next();
//          
//          if(tuple_valid_rows.get(tuple).size() < rows)
//          {
//            it.remove();
//          }
//          else
//          {
//            tuple_ids.put(tuple, id);
//            
//            id++;
//          }
//        }
//      }
//      else
//      {
//        Iterator it = tuples.iterator();
//        
//        while(it.hasNext())
//        {
//          Tuple tuple = (Tuple) it.next();
//          
//          if(tuple_valid_rows.get(tuple).isEmpty())
//          {
//            it.remove();
//          }
//          else
//          {
//            tuple_ids.put(tuple, id);
//            
//            id++;
//          }
//        }
//      }
//      
//      if(tuples.isEmpty())
//        iter.remove();
//            
//    }
//    
////    System.out.println();
////    
////    System.out.println(all_why_tokens);
////    
////    System.out.println();
////    
////    System.out.println(grouping_value_agg_value_mappings);
////    
////    System.out.println();
////    
////    System.out.println(tuple_why_prov_mappings);
////    
////    System.out.println();
////    
//    System.out.println(tuple_valid_rows.size());
//    
//    get_valid_view_mappings_per_group(user_query.head.has_agg);
//    
////    output(group_view_mappings);
  }
  
  static void output(ConcurrentHashMap<String, ConcurrentHashMap<Tuple, Integer>> group_view_mappings)
  {
    Set<String> group_ids = group_view_mappings.keySet();
    
    for(String group_id : group_ids)
    {
      System.out.println(group_id);
      
      Set<Tuple> view_mappings = group_view_mappings.get(group_id).keySet();
      
      for(Tuple view_mapping: view_mappings)
      {
        System.out.print(view_mapping.getName() + "    ");
      }
      
      System.out.println();
    }
  }
  
  
//  static void get_valid_view_mappings_per_group(boolean query_has_agg)
//  {
//    Set<Tuple> view_mappings = tuple_valid_rows.keySet();
//    
//    ArrayList<HashSet> rid_sets = new ArrayList<HashSet>();
//    
//    ArrayList<Tuple> all_view_mappings = new ArrayList<Tuple>();
//    
//    for(Tuple view_mapping: view_mappings)
//    {
//      HashSet curr_rids = tuple_valid_rows.get(view_mapping);
//      
//      rid_sets.add(curr_rids);
//      
//      all_view_mappings.add(view_mapping);
//      
//    }
//    
//    if(query_has_agg)
//    {
//      Set<Head_strs> all_head_values = tuple_why_prov_mappings.keySet();
//      
//      for(Head_strs head_value: all_head_values)
//      {
//        String signiture = new String();
//        
//        ArrayList<Integer> tuple_ids = new ArrayList<Integer>();
//        
//        for(int j = 0; j< rid_sets.size(); j++)
//        {
//          if(rid_sets.get(j).contains(head_value))
//          {
//            signiture += "," + j;
//            
//            tuple_ids.add(j);
//            
//          }
//        }
//        
//        
//        if(group_ids.get(signiture) == null)
//        {
//          HashSet<Head_strs> ids = new HashSet<Head_strs>();
//          
//          ids.add(head_value);
//          
//          group_ids.put(signiture, ids);
//          
//          ConcurrentHashMap<Tuple, Integer> curr_view_mappings = new ConcurrentHashMap<Tuple, Integer>();
//          
//          for(int j = 0; j<tuple_ids.size(); j++)
//          {
//            curr_view_mappings.put(all_view_mappings.get(tuple_ids.get(j)), j);
//          }
//          
//          group_view_mappings.put(signiture, curr_view_mappings);
//          
//        }
//        else
//        {
//          group_ids.get(signiture).add(head_value);
//        }
//      
//      }
//    }
//    else
//    {
//      for(int i = 0; i<rows; i++)
//      {
//
//        String signiture = new String();
//        
//        ArrayList<Integer> tuple_ids = new ArrayList<Integer>();
//        
//        for(int j = 0; j< rid_sets.size(); j++)
//        {
//          if(rid_sets.get(j).contains(i))
//          {
//            signiture += "," + j;
//            
//            tuple_ids.add(j);
//          }
//        }
//        
//        if(group_ids.get(signiture) == null)
//        {
//          HashSet<Integer> ids = new HashSet<Integer>();
//          
//          ids.add(i);
//          
//          group_ids.put(signiture, ids);
//          
//          ConcurrentHashMap<Tuple, Integer> curr_view_mappings = new ConcurrentHashMap<Tuple, Integer>();
//          
//          for(int j = 0; j<tuple_ids.size(); j++)
//          {
//            curr_view_mappings.put(all_view_mappings.get(tuple_ids.get(j)), j);
//          }
//          
//          group_view_mappings.put(signiture, curr_view_mappings);
//        }
//        else
//        {
//          group_ids.get(signiture).add(i);
//        }
//      }
//    }
//        
//    
//    
//    
//    
//    
//  }
//  
  
  static ConcurrentHashMap<Tuple, Integer> get_tuple_id_mappings(HashSet<Tuple> all_view_mappings)
  {
    ConcurrentHashMap<Tuple, Integer> tuple_id_mappings = new ConcurrentHashMap<Tuple, Integer>();
    
    int count = 0;
    
    for(Tuple view_mapping: all_view_mappings)
    {
      tuple_id_mappings.put(view_mapping, count);
      
      count++;
    }
    
    return tuple_id_mappings;
  }
  
  static ArrayList<int[]> get_valid_view_mappings(ArrayList<HashSet<Covering_set>> covering_sets_per_attributes, Vector<Argument> args, Query query, ArrayList<HashSet<Tuple>> valid_view_mappings_per_head_var, HashSet<Tuple> all_valid_view_mappings)
  {
    
//    System.out.println(tuple_ids);
    
    ConcurrentHashMap<Tuple, Integer> tuple_ids = get_tuple_id_mappings(all_valid_view_mappings);
    
    HashSet<HashSet<Tuple>> all_tuples = new HashSet<HashSet<Tuple>>();
    
    ArrayList<int[]> tuple_index = new ArrayList<int[]>();
    
    for(int i = 0; i<valid_view_mappings_per_head_var.size(); i++)
    {
      HashSet<Tuple> valid_view_mappings = valid_view_mappings_per_head_var.get(i);
      
      valid_view_mappings.retainAll(all_valid_view_mappings);
      
//      Set<Single_view> views = all_possible_view_mappings.keySet();
//      
//      HashSet<Tuple> curr_tuples = new HashSet<Tuple>();
//            
//      for(Iterator iter = views.iterator(); iter.hasNext();)
//      {
//        Single_view view = (Single_view) iter.next();
//        
//        HashSet<Tuple> tuples1 = valid_view_mappings.get(view);
//        
//        HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
//        
//        if(tuples2 == null)
//        {
//          iter.remove();
//          
//          continue;
//        }
//        
//        tuples1.retainAll(tuples2);
//        
//        if(tuples1.isEmpty())
//          iter.remove();
//        
//        curr_tuples.addAll(tuples1);
//        
//      }
      
      if(all_tuples.contains(valid_view_mappings))
      {
        valid_view_mappings_per_head_var.remove(i);
        
        i--;
      }
      else
      {
        all_tuples.add(valid_view_mappings);
        
        int [] curr_tuple_index = new int[all_valid_view_mappings.size()];
        
        for(int k = 0; k<curr_tuple_index.length; k++)
        {
          curr_tuple_index[k] = 0;
        }
        
        HashSet<Covering_set> curr_covering_sets = new HashSet<Covering_set>();
        
        for(Tuple tuple: valid_view_mappings)
        {
          int id = tuple_ids.get(tuple);
          
          
          curr_tuple_index[id] = 1;
          
          Tuple valid_tuple = (Tuple) tuple.clone();
          
          valid_tuple.args.retainAll(args);
              
          
          long [] tuple_id_contained = new long[(tuple_ids.size() + Long.SIZE - 1)/Long.SIZE];
          
          tuple_id_contained[id/Long.SIZE] |= (1L << (id % Long.SIZE));
          
          int arg_size = (query.head.has_agg)?query.head.agg_args.size():query.head.args.size(); 
          
          if(valid_tuple.lambda_terms.size() > 0)
          {
              
              citation_view_parametered c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple, query_subgoal_id_mappings, arg_size, view_mapping_query_arg_ids_mappings, tuple_ids);
              
              Covering_set curr_views = new Covering_set(c, tuple_id_contained);
              
              remove_duplicate(curr_covering_sets, curr_views);
              
//              curr_covering_sets.add(curr_views);
              
              
              
          }   
          else
          {
              
              citation_view_unparametered c = new citation_view_unparametered(valid_tuple.name, valid_tuple, query_subgoal_id_mappings, arg_size, view_mapping_query_arg_ids_mappings, tuple_ids);
              
              Covering_set curr_views = new Covering_set(c, tuple_id_contained);
              
              remove_duplicate(curr_covering_sets, curr_views);
          }
        }
        
        covering_sets_per_attributes.add(curr_covering_sets);
        
        tuple_index.add(curr_tuple_index);
      }
    }
    
    return tuple_index;
  }
  
  static Head_strs get_query_result(String[] provenance_row, int start_size, int end_size) throws SQLException
  {
    Vector<String> values = new Vector<String>();
    
    for(int i = start_size; i<end_size; i++)
    {
      String value = provenance_row[i];
      
      values.add(value);
    }
    
    Head_strs curr_query_result = new Head_strs(values);
    
    return curr_query_result;
  }
  
  static Head_strs get_query_result(ResultSet rs, int start_size, int end_size) throws SQLException
  {
    Vector<String> values = new Vector<String>();
    
    for(int i = start_size; i<end_size; i++)
    {
      String value = rs.getString(i + 1);
      
      values.add(value);
    }
    
    Head_strs curr_query_result = new Head_strs(values);
    
    return curr_query_result;
  }
  
  static ArrayList<HashSet<Tuple>> clone_view_mapping_per_attribute(ArrayList<HashSet<Tuple>> view_mappings_per_head_vairable)
  {
    ArrayList<HashSet<Tuple>> view_mappings_per_head_variable_copy = new ArrayList<HashSet<Tuple>>();
    
    for(int i = 0; i<view_mappings_per_head_vairable.size(); i++)
    {
      view_mappings_per_head_variable_copy.add((HashSet<Tuple>) view_mappings_per_head_vairable.get(i).clone());
    }
    
    return view_mappings_per_head_variable_copy;
  }
  
  static ConcurrentHashMap<String, Integer> get_table_attribute_nums(HashSet<String> tables, Connection c, PreparedStatement pst) throws SQLException
  {
    ConcurrentHashMap<String, Integer> table_attr_nums_mappings = new ConcurrentHashMap<String, Integer>();
    
    for(String table: tables)
    {
      int nums = get_attribute_nums_single_table(table, c, pst);
      
      table_attr_nums_mappings.put(table, nums);
    }
    
    return table_attr_nums_mappings;
  }
  
  static int get_attribute_nums_single_table(String table, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "select count(column_name) from information_schema.columns where table_name = '" + table + "'";
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    int nums = 0;
    
    if(rs.next())
    {
      nums = rs.getInt(1);
    }
    
    return nums;
  }
  
//  static void set_head_vars_id_mappings(Vector<Head_strs> head_vars)
//  {
//    int num = 0;
//    
//    for(Head_strs values:head_vars)
//    {
//      if(tuple_why_prov_mappings.get(values) == null)
//      {
//        ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
//        
//        curr_tokens.add(num);
//        
//        tuple_why_prov_mappings.put(values.hashCode(), curr_tokens);
//        
//        System.out.println(values + "::" + curr_tokens);
//        
//      }
//      else
//      {
//        tuple_why_prov_mappings.get(values.hashCode()).add(num);
//        
//    //    System.out.println(values + "::" + tuple_why_prov_mappings.get(values));
//      }
//      
//      num++;
//    }
//    
//    
//  }
  
  static void build_group_agg_value_mapping(Head_strs grouping_values, Head_strs agg_values)
  {
    grouping_value_agg_value_mappings.put(grouping_values, agg_values);
  }
  
  static double[][] reasoning_valid_view_mappings_conjunctive_query(ArrayList<HashSet<Covering_set>> covering_sets_per_attribute, Query user_query, ConcurrentHashMap<Tuple, Integer> tuple_ids, boolean isclustering, ResultSet rs, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
    
    HashSet<String> tables = new HashSet<String>();
    
    for(int i = 0; i<user_query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) user_query.body.get(i);
      
      query_subgoal_id_mappings.put(subgoal.name, i);
      
      tables.add(user_query.subgoal_name_mapping.get(subgoal.name));
    }
    
    ConcurrentHashMap<String, Integer> table_attr_nums_mappings = get_table_attribute_nums(tables, c, pst);
    
//    for(int i = 0; i<user_query.head.args.size(); i++)
//    {
//      Argument arg = (Argument) user_query.head.args.get(i);
//      
//      query_arg_id_mappings.put(arg, i);
//    }
    
//    System.out.println(user_query);
//    
//    System.out.println(tables);
    
//    input_relations(query_relation_attr_id_mappings, tables, c, pst);
    
    all_possible_view_mappings = get_all_possible_view_mappings(query_subgoal_id_mappings, user_query);
    
    
    
    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head, all_possible_view_mappings);    
    
    
//    ArrayList<HashSet<Tuple>> valid_view_mappings_per_head_var_copy = clone_view_mapping_per_attribute(valid_view_mappings_per_head_var);
    
//    Provenance_reasoning.con.getW().setLogLevel(0);
    
//    Statement st = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    
    
//    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    
//    ResultSet rs = st.executeQuery(sql);
        
//    Provenance_reasoning.con.clearWarnings();
//    
//    Provenance_reasoning.con.close();
    
//    System.out.println("here");
//    
//    System.out.println(Runtime.getRuntime().totalMemory());
//    
//    System.out.println(Runtime.getRuntime().freeMemory());
    
//    printResult(rs);
    
    while(rs.next())
    {
      
//      System.out.println("here");
      
      try{
        Head_strs values = get_query_result(rs, 0, user_query.head.args.size());
        
        if(user_query.head.agg_args != null)
        {
          Head_strs agg_values = get_query_result(rs, user_query.head.args.size(), user_query.head.agg_args.size());
          
          build_group_agg_value_mapping(values, agg_values);
        }
        
//        Vector<Head_strs> curr_tuples = get_tuples(rs, user_query, table_attr_nums_mappings);
        
//        System.out.println(values + "::" + rows);
//        
//        System.out.println(Runtime.getRuntime().totalMemory());
//        
//        System.out.println(Runtime.getRuntime().freeMemory());
//        
//        System.out.println(curr_tuples);
        
        
        
//        if(tuple_why_prov_mappings.get(values) == null)
//        {
//          ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
//          
//          curr_tokens.add(rows);
//          
//          tuple_why_prov_mappings.put(values, curr_tokens);
//          
////          System.out.println(values + "::" + curr_tokens);
//          
//        }
//        else
//        {
//          tuple_why_prov_mappings.get(values).add(rows);
//          
////          System.out.println(values + "::" + tuple_why_prov_mappings.get(values));
//        }
        
        rows ++;
        
//        all_why_tokens.add(curr_tuples);
        
        
      }
      catch(Exception e)
      {
        System.out.println(e.getStackTrace());
      }
      
      
//      Vector<String> where_tokens = get_curr_where_token_seq(rs, where_token_seqs, subgoal_size, head_var_size);
//      
//      Vector<String> why_tokens = get_curr_why_token_seq(rs, why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
//      
//      get_curr_where_why_token_seq(rs, where_tokens, why_tokens, where_why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
      
    }
    
//    set_head_vars_id_mappings(head_vals);
    
    HashSet<Tuple> all_valid_view_mappings = evaluate_views(all_possible_view_mappings, user_query);
    
    
//    output_view_mapping_id_mappings(tuple_ids);
    
    ArrayList<int[]> tuple_index = get_valid_view_mappings(covering_sets_per_attribute, user_query.head.args, user_query, valid_view_mappings_per_head_var, all_valid_view_mappings);
    
//    add_suffix_token_seq(where_token_seqs);
//    
//    check_where_tokens(where_token_seqs, valid_view_mappings_per_head_var, where_why_token_seqs);
//    
//    checking_why_provenance_tokens(why_token_seqs, all_possible_view_mappings);
//
//    
//    checking_where_why_provenance_tokens(valid_view_mappings_per_head_var, where_why_token_seqs, all_possible_view_mappings);
    
    valid_view_mappings_schema_level = valid_view_mappings_per_head_var;
    
    if(isclustering)
      return cal_distances(tuple_index);
    else
      return null;
    
  }
  
//  static double[][] reasoning_valid_view_mappings_conjunctive_query(ArrayList<HashSet<Covering_set>> covering_sets_per_attribute, Query user_query, ConcurrentHashMap<Tuple, Integer> tuple_ids, boolean isclustering, Vector<String[]> provenance_instances, boolean is_materialized, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
//  {
//    
//    HashSet<String> tables = new HashSet<String>();
//    
//    for(int i = 0; i<user_query.body.size(); i++)
//    {
//      Subgoal subgoal = (Subgoal) user_query.body.get(i);
//      
//      query_subgoal_id_mappings.put(subgoal.name, i);
//      
//      tables.add(user_query.subgoal_name_mapping.get(subgoal.name));
//    }
//    
//    ConcurrentHashMap<String, Integer> table_attr_nums_mappings = get_table_attribute_nums(tables, c, pst);
//    
////    for(int i = 0; i<user_query.head.args.size(); i++)
////    {
////      Argument arg = (Argument) user_query.head.args.get(i);
////      
////      query_arg_id_mappings.put(arg, i);
////    }
//    
////    System.out.println(user_query);
////    
////    System.out.println(tables);
//    
////    input_relations(query_relation_attr_id_mappings, tables, c, pst);
//    
//    all_possible_view_mappings = get_all_possible_view_mappings(query_subgoal_id_mappings, user_query);
//    
//    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head, all_possible_view_mappings);    
//    
//    ArrayList<HashSet<Tuple>> valid_view_mappings_per_head_var_copy = clone_view_mapping_per_attribute(valid_view_mappings_per_head_var);
//    
////    Provenance_reasoning.con.getW().setLogLevel(0);
//    
////    Statement st = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//    
//    
////    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//    
////    ResultSet rs = st.executeQuery(sql);
//        
////    Provenance_reasoning.con.clearWarnings();
////    
////    Provenance_reasoning.con.close();
//    
////    System.out.println("here");
////    
////    System.out.println(Runtime.getRuntime().totalMemory());
////    
////    System.out.println(Runtime.getRuntime().freeMemory());
//    
////    printResult(rs);
//    
//    for(String[] provenance_row: provenance_instances)
//    {
//      
////      System.out.println("here");
//      
//      Head_strs values = get_query_result(provenance_row, 0, user_query.head.args.size());
//      
//      
//      if(user_query.head.agg_args != null)
//      {
//        Head_strs agg_values = get_query_result(provenance_row, user_query.head.args.size(), user_query.head.args.size() + user_query.head.agg_args.size());
//        
//        build_group_agg_value_mapping(values, agg_values);
//      } 
//      
//      Vector<Head_strs> curr_tuples = get_tuples(provenance_row, user_query, table_attr_nums_mappings);
//      
////      System.out.println(rows);
////      
////      System.out.println(Runtime.getRuntime().totalMemory());
////      
////      System.out.println(Runtime.getRuntime().freeMemory());
////      
////      System.out.println(curr_tuples);
//      
//      
//      
////      if(tuple_why_prov_mappings.get(values) == null)
////      {
////        ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
////        
////        curr_tokens.add(rows);
////        
////        tuple_why_prov_mappings.put(values, curr_tokens);
////        
//////        System.out.println(values + "::" + curr_tokens);
////        
////      }
////      else
////      {
////        tuple_why_prov_mappings.get(values).add(rows);
////        
//////        System.out.println(values + "::" + tuple_why_prov_mappings.get(values));
////      }
//      
//      rows ++;
//      
//      all_why_tokens.add(curr_tuples);
////      Vector<String> where_tokens = get_curr_where_token_seq(rs, where_token_seqs, subgoal_size, head_var_size);
////      
////      Vector<String> why_tokens = get_curr_why_token_seq(rs, why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
////      
////      get_curr_where_why_token_seq(rs, where_tokens, why_tokens, where_why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
//      
//    }
//    
////    set_head_vars_id_mappings(head_vals);
//    
//    evaluate_views(all_possible_view_mappings, tuple_ids, user_query);
//    
//    
////    output_view_mapping_id_mappings(tuple_ids);
//    
//    ArrayList<int[]> tuple_index = get_valid_view_mappings(covering_sets_per_attribute, user_query.head.args, user_query, valid_view_mappings_per_head_var_copy, tuple_ids);
//    
////    add_suffix_token_seq(where_token_seqs);
////    
////    check_where_tokens(where_token_seqs, valid_view_mappings_per_head_var, where_why_token_seqs);
////    
////    checking_why_provenance_tokens(why_token_seqs, all_possible_view_mappings);
////
////    
////    checking_where_why_provenance_tokens(valid_view_mappings_per_head_var, where_why_token_seqs, all_possible_view_mappings);
//    
//    valid_view_mappings_schema_level = valid_view_mappings_per_head_var_copy;
//    
//    if(isclustering)
//      return cal_distances(tuple_index);
//    else
//      return null;
//    
//  }
//  
  static void output_view_mapping_id_mappings(ConcurrentHashMap<Tuple, Integer> tuple_ids)
  {
    Set<Tuple> view_mappings = tuple_ids.keySet();
    
    for(Tuple tuple:view_mappings)
    {
      System.out.println(tuple.name + "|" + tuple.mapSubgoals_str + tuple_ids.get(tuple));
    }
  }
  
  static double get_distance(int[] index1, int []index2)
  {
    double distance = 0;
    
    for(int i = 0; i<index1.length; i++)
    {
      distance += Math.pow((index1[i] - index2[i]), 2);
    }
    
    return -distance;
  }
  
  static double[][] cal_distances(ArrayList<int[]> tuple_index)
  {
    if(tuple_index.size() == 1)
    {
      double[][] distances = new double[tuple_index.size()][tuple_index.size()];
      
      for(int i = 0; i<tuple_index.size(); i++)
      {
        distances[i][i] = 0;
      }
      
      return distances;
    }
    
    
    double[][] distances = new double[tuple_index.size()][tuple_index.size()];
    
    double []similarity_values = new double[tuple_index.size() * tuple_index.size() - tuple_index.size()];
    
    int num = 0;
    
    for(int i = 0; i < tuple_index.size(); i++)
    {
      for(int j = 0; j<tuple_index.size(); j++)
      {
        if(i != j)
        {
          distances[i][j] = get_distance(tuple_index.get(i), tuple_index.get(j));
          
          similarity_values[num++] = distances[i][j];
        }
      }
    }
    
    Arrays.sort(similarity_values);
    
    for(int i = 0; i<tuple_index.size(); i++)
    {
      distances[i][i] = factor * similarity_values[similarity_values.length/2];
    }
    
    return distances;
  }
  
//  static ArrayList<Vector<Head_strs>> get_sel_tokens(Vector<Head_strs> sel_values)
//  {
//    ArrayList<Vector<Head_strs>> all_tokens = new ArrayList<Vector<Head_strs>>();
//    
//    for(int i = 0; i<sel_values.size(); i++)
//    {
//      
//      ArrayList<Integer> row_ids = tuple_why_prov_mappings.get(sel_values.get(i));
//      
//      for(int j = 0; j<row_ids.size(); j++)
//      {
//        all_tokens.add(all_why_tokens.get(row_ids.get(j)));
//      }
//      
//    }
//    
//    return all_tokens;
//  }
  
//  static HashSet<citation_view_vector> reasoning_multi_tuples(Vector<Head_strs> values, Query user_query, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
//  {
//    ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings = reasoning_valid_view_mappings_conjunctive_query_multi_tuples(user_query, values, c, pst);
//    
//    HashSet<citation_view_vector> covering_sets = reasoning_covering_set_multi_threads_multi_hops_conjunctive_query(valid_view_mappings, user_query.head.args, true);
//
//    return covering_sets;
//    
//  }
  
  static ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> reasoning_valid_view_mappings_conjunctive_query_multi_tuples(Query user_query, Vector<Head_strs> sel_values, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {    
    ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = new ConcurrentHashMap<Single_view, HashSet<Tuple>>(); 
        
    clone_view_mappings(all_possible_view_mappings, all_possible_view_mappings_copy);
        
    ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>>();
    
//    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head.args, all_possible_view_mappings);
//    
//    ArrayList<Vector<Head_strs>> all_tuples = get_sel_tokens(sel_values);//new ArrayList<Vector<Head_strs>>();
//
////    ConcurrentHashMap<Tuple, Integer> tuple_ids = evaluate_views(all_tuples, all_possible_view_mappings_copy, c, pst);
//    
//    ArrayList<HashSet<citation_view_vector>> covering_sets_per_attributes = new ArrayList<HashSet<citation_view_vector>>();
    
//    ArrayList<int[]> tuple_index = get_valid_view_mappings(covering_sets_per_attributes, user_query.head.args, all_possible_view_mappings_copy, valid_view_mappings_per_head_var, tuple_ids);
    
    return valid_view_mappings_per_head_var;
    
  }
  
  
  public static void init(String url, String usr_name, String passwd) throws ClassNotFoundException, SQLException
  {
    Query_provenance.connect(url, usr_name, passwd);
  }
  
  public static void reset() throws SQLException
  {
    Query_provenance.reset();
  }
  
  public static HashSet<Covering_set> reasoning(Query user_query, ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, boolean ifclustering, ResultSet rs, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
  {
//    String sql = new String();
//    
//    
//    if(!test_case)
//      sql = Query_converter.data2sql_with_why_token_columns(user_query);
//    else
//      sql = Query_converter.data2sql_with_why_token_columns_test(user_query);
//      
//    
//    System.out.println(sql);
    
    double start = 0.0;
    
    double end = 0.0;
    
    
    
    start = System.nanoTime();
    
    ArrayList<HashSet<Covering_set>> covering_sets_per_attributes = new ArrayList<HashSet<Covering_set>>();
    
    ConcurrentHashMap<Tuple, Integer> tuple_ids = new ConcurrentHashMap<Tuple, Integer>();
    
    double[][] distances = reasoning_valid_view_mappings_conjunctive_query(covering_sets_per_attributes, user_query, tuple_ids, ifclustering, rs, c, pst);
    
    end = System.nanoTime();
    
    view_mapping_time = (end - start) * 1.0/1000000000;
    
    start = System.nanoTime();
    
    HashSet<Covering_set> covering_sets;
    
    if(ifclustering)
      covering_sets = reasoning_covering_set_ap(distances, covering_sets_per_attributes);
    else
    {
      covering_sets = reasoning_covering_sets(distances, covering_sets_per_attributes);
    }
    
    end = System.nanoTime();
    
    covering_set_time = (end - start) * 1.0/1000000000;
    
    
    System.out.println(all_possible_view_mappings);
    
    System.out.println(valid_view_mappings_per_head_var);
    
    System.out.println(valid_view_mappings_schema_level);
//    cal_covering_sets_per_group(tuple_ids, covering_sets, user_query);
    
    return covering_sets;
  }
  
//  public static HashSet<Covering_set> reasoning(Query user_query, ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, boolean ifclustering, boolean is_materialized, Vector<String[]> provenance_instances, Connection c, PreparedStatement pst) throws SQLException, InterruptedException
//  {
////    String sql = new String();
////    
////    
////    if(!test_case)
////      sql = Query_converter.data2sql_with_why_token_columns(user_query);
////    else
////      sql = Query_converter.data2sql_with_why_token_columns_test(user_query);
////      
////    
////    System.out.println(sql);
//    
//    double start = 0.0;
//    
//    double end = 0.0;
//    
//    
//    
//    start = System.nanoTime();
//    
//    ArrayList<HashSet<Covering_set>> covering_sets_per_attributes = new ArrayList<HashSet<Covering_set>>();
//    
//    ConcurrentHashMap<Tuple, Integer> tuple_ids = new ConcurrentHashMap<Tuple, Integer>();
//    
//    double[][] distances = reasoning_valid_view_mappings_conjunctive_query(covering_sets_per_attributes, user_query, tuple_ids, ifclustering, provenance_instances, is_materialized, c, pst);
//    
//    end = System.nanoTime();
//    
//    view_mapping_time = (end - start) * 1.0/1000000000;
//    
//    start = System.nanoTime();
//    
//    HashSet<Covering_set> covering_sets;
//    
//    if(ifclustering)
//      covering_sets = reasoning_covering_set_ap(distances, covering_sets_per_attributes);
//    else
//    {
//      covering_sets = reasoning_covering_sets(distances, covering_sets_per_attributes);
//    }
//    
////    System.out.println(covering_sets_per_attributes);
//    
//    end = System.nanoTime();
//    
//    covering_set_time = (end - start) * 1.0/1000000000;
//    
////    cal_covering_sets_per_group(tuple_ids, covering_sets, user_query);
//    
//    return covering_sets;
//  }
//  
  public static HashSet<String> gen_citations(ConcurrentHashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy, HashSet<Covering_set> covering_sets, Connection c, PreparedStatement pst) throws SQLException, JSONException
  {
    HashSet<String> formatted_citations = new HashSet<String>();
    
    //Gen_citation.gen_citation_entire_query(all_possible_view_mappings_copy, covering_sets, tuple_valid_rows, all_why_tokens, max_num, c, pst);
    
    return formatted_citations;
  }
  
//  static HashSet<citation_view_vector> reasoning_covering_set_conjunctive_query(ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, ConcurrentHashMap<Tuple, Integer> tuple_ids)
//  {
//    
//    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(2));
//    
//    Vector<HashSet<citation_view_vector>> covering_sets = new Vector<HashSet<citation_view_vector>>();
//    
//    for(int i = 1; i<=loop_time; i++)
//    {
//      int j = 0;
//      
//      if(i == 1)
//      {
//        
//        for(j = 0; j<valid_view_mappings_per_head_var.size() + 2*i; j = j+2*i)
//        {
//          HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();
//          
//          for(int k = j; k<j+2*i && k < valid_view_mappings_per_head_var.size(); k++)
//          {
//            ConcurrentHashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
//            
//            Set<Single_view> views = valid_view_mappings.keySet();
//            
//            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
//            
//            for(Iterator iter = views.iterator(); iter.hasNext();)
//            {
//              Single_view view = (Single_view) iter.next();
//              
//              HashSet<Tuple> tuples = valid_view_mappings.get(view);
//              
//              all_tuples.addAll(tuples);
//              
//              
//            }
//            
//            view_com = join_views_curr_relation(all_tuples, view_com, args, tuple_ids);
//            
//          }
//          
//          if(!view_com.isEmpty())
//             covering_sets.add(view_com);
//        }
//        
//        
//      }
//      else
//      {
//        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(2*i));
//        
//        for(int k = 0; k<covering_sets.size(); k=k+2)
//        {
//          if(k + 1 < covering_sets.size())
//          {
//            HashSet<citation_view_vector> updated_covering_set = join_operation(covering_sets.get(k), covering_sets.get(k + 1));
//            
//            covering_sets.set(k/2, updated_covering_set);
//          }
//          else
//          {
//            covering_sets.set(k/2, covering_sets.get(k));
//          }
//        }
//        
//        int redundant_start = (covering_sets.size() + 1)/2;
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
//    return covering_sets.get(0);
//  }
  
//  static HashSet<citation_view_vector> reasoning_covering_set_multi_threads_multi_hops_conjunctive_query(ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, boolean multi_thread) throws InterruptedException
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
////            ConcurrentHashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
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
//      ConcurrentHashMap<Single_view, HashSet<Tuple>> view_mappings = valid_view_mappings_per_head_var.get(0);
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
//  static HashSet<citation_view_vector> reasoning_covering_set_multi_hops_conjunctive_query(ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, ConcurrentHashMap<Tuple, Integer> tuple_ids, boolean multi_thread) throws InterruptedException
//  {
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
//        for(j = 0; j<valid_view_mappings_per_head_var.size() + gap*i; j = j+gap*i)
//        {
//          HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();
//          
//          for(int k = j; k<j+gap*i && k < valid_view_mappings_per_head_var.size(); k++)
//          {
//            ConcurrentHashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
//            
//            Set<Single_view> views = valid_view_mappings.keySet();
//            
//            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
//            
//            for(Iterator iter = views.iterator(); iter.hasNext();)
//            {
//              Single_view view = (Single_view) iter.next();
//              
//              HashSet<Tuple> tuples = valid_view_mappings.get(view);
//              
//              all_tuples.addAll(tuples);
//              
//              
//            }
//            
//            view_com = join_views_curr_relation(all_tuples, view_com, args, tuple_ids);
//            
//            
//          }
//          
//          if(!view_com.isEmpty())
//            covering_sets.add(view_com);
//          
//        }
//              
//      }
//      else
//      {
//        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(gap*i));
//        
//        for(int k = 0; k<covering_sets.size(); k=k+gap)
//        {
//          
//          int start = 0;
//          
//          int end = 0;
//          
//          
//          if(k + gap - 1 < covering_sets.size())
//          {
//            
//            start = k;
//            
//            end = k + gap;
//          }
//          else
//          {
//            
//            start = k;
//            
//            end = covering_sets.size();
//            
//          }
//          
//          HashSet<citation_view_vector> resulting_covering_set = covering_sets.get(start);
//          
//          for(int p = start + 1; p<end; p++)
//          {
//            resulting_covering_set = join_operation(resulting_covering_set, covering_sets.get(p));
//
//          }
//          
//          covering_sets.set(k/gap, resulting_covering_set);
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
//      ConcurrentHashMap<Single_view, HashSet<Tuple>> view_mappings = valid_view_mappings_per_head_var.get(0);
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
  static HashSet<Covering_set> reasoning_covering_sets(double [][] distances, ArrayList<HashSet<Covering_set>> covering_sets_per_attributes)
  {
//    String [] names = new String[covering_sets_per_attributes.size()];
//    
//    for(int i = 0; i < covering_sets_per_attributes.size(); i++)
//    {
//      names[i] = "att" + i;
//    }
//    
//    ClusteringAlgorithm alg = new DefaultClusteringAlgorithm();
//    Cluster cluster = alg.performClustering(distances, names,covering_sets_per_attributes,
//        new AverageLinkageStrategy());
//    
//    return cluster.merge_children();
    
    HashSet<Covering_set> resulting_covering_sets = new HashSet<Covering_set>();
    
    for(int i = 0; i<covering_sets_per_attributes.size(); i++)
    {
      if(i == 0)
        resulting_covering_sets = covering_sets_per_attributes.get(i);
      else
        resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(i));
    }
    
//    DataProvider provider = new MatrixProvider(distances);
//    
//    AproBuilder builder = new AproBuilder();
//    builder.setThreads(1); // no parallelization
//    Apro apro = builder.build(provider);
//    
//    apro.setDebug(false);
//    
//    apro.run(200);
//    
//    ConcurrentHashMap<Integer, HashSet<citation_view_vector>> covering_sets_per_cluster = new ConcurrentHashMap<Integer, HashSet<citation_view_vector>>();
//    
//    System.out.println(apro.getExemplarSet().size());
//    
//    double start = System.nanoTime();
//
//    
//    for(int i = 0; i<apro.getExemplars().length; i++)
//    {
//      int exemplar = apro.getExemplars()[i];
//            
//      if(covering_sets_per_cluster.get(exemplar) == null)
//      {
//        covering_sets_per_cluster.put(exemplar, covering_sets_per_attributes.get(i));
//      }
//      else
//      {
//        HashSet<citation_view_vector> resulting_covering_sets = covering_sets_per_cluster.get(exemplar);
//        
//        resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(i));
//        
//        covering_sets_per_cluster.put(exemplar, resulting_covering_sets);
//      }
//      
//    }
//    
//    for(int i = 0; i<apro.getExemplars().length; i++)
//    {
//      int exemplar = apro.getExemplars()[i];
//            
//      if(covering_sets_per_cluster.get(exemplar) == null)
//      {
//        covering_sets_per_cluster.put(exemplar, covering_sets_per_attributes.get(i));
//      }
//      else
//      {
//        HashSet<citation_view_vector> resulting_covering_sets = covering_sets_per_cluster.get(exemplar);
//        
//        resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(i));
//        
//        covering_sets_per_cluster.put(exemplar, resulting_covering_sets);
//      }
//      
//    }
//    
//    double end_time = System.nanoTime();
//    
//    double time = (end_time - start)/1000000000;
//    
//    System.out.println(time);
//    
//    HashSet<citation_view_vector> resulting_covering_sets = null;
//    
//    Set<Integer> ids = covering_sets_per_cluster.keySet();
//    
//    for(Integer id : ids)
//    {
//      HashSet<citation_view_vector> curr_covering_sets = covering_sets_per_cluster.get(id);
//      
//      if(resulting_covering_sets == null)
//      {
//        resulting_covering_sets = curr_covering_sets;
//      }
//      else
//      {
//        resulting_covering_sets = join_operation(resulting_covering_sets, curr_covering_sets);
//      }
//    }
    
    return resulting_covering_sets;
    
  }
  
  
  static int get_next_cluster(double [][]distances, int curr_id, ConcurrentHashMap<Integer, Vector<Integer>> clusters, Vector<Integer> sorted_ids, Set<Integer> all_ids)
  {
    Vector<Integer> curr_cluster = clusters.get(curr_id);
    
    double min_max_cluster_distance = Double.MAX_VALUE;
    
    int min_max_cluster_id = -1;
    
    for(Integer id: all_ids)
    {
      if(!sorted_ids.contains(id) && id != curr_id)
      {
        Vector<Integer> curr_check_cluster = clusters.get(id);
        
        double max_distance = -1;
        
        for(int i = 0; i<curr_cluster.size(); i++)
        {
          for(int j = 0; j<curr_check_cluster.size(); j++)
          {
            if(distances[curr_cluster.get(i)][curr_check_cluster.get(j)] * (-1) > max_distance)
            {
              max_distance = distances[curr_cluster.get(i)][curr_check_cluster.get(j)] * (-1); 
            }
          }
        }
        
        if(max_distance < min_max_cluster_distance)
        {
          min_max_cluster_distance = max_distance;
          
          min_max_cluster_id = id;
        }
        
      }
    }
    
    return min_max_cluster_id;
  }
  
  static HashSet<Covering_set> reasoning_covering_set_ap(double [][] distances, ArrayList<HashSet<Covering_set>> covering_sets_per_attributes)
  {
//    String [] names = new String[covering_sets_per_attributes.size()];
//    
//    for(int i = 0; i < covering_sets_per_attributes.size(); i++)
//    {
//      names[i] = "att" + i;
//    }
//    
//    ClusteringAlgorithm alg = new DefaultClusteringAlgorithm();
//    Cluster cluster = alg.performClustering(distances, names,covering_sets_per_attributes,
//        new AverageLinkageStrategy());
//    
//    return cluster.merge_children();
    
    DataProvider provider = new MatrixProvider(distances);
    
    AproBuilder builder = new AproBuilder();
    builder.setDebug(false);
    
    builder.setThreads(1); // no parallelization
    Apro apro = builder.build(provider);
    apro.setDebug(false);
    
    apro.run(200);
    
//    ConcurrentHashMap<Integer, HashSet<citation_view_vector>> covering_sets_per_cluster = new ConcurrentHashMap<Integer, HashSet<citation_view_vector>>();
    
//    System.out.println(apro.getExemplarSet().size());
    
    double start = System.nanoTime();

    
    ConcurrentHashMap<Integer, Vector<Integer>> clusters = new ConcurrentHashMap<Integer, Vector<Integer>>();
    
    for(int i = 0; i<apro.getExemplars().length; i++)
    {
      int exemplar = apro.getExemplars()[i];
      
      if(clusters.get(exemplar) == null)
      {
        Vector<Integer> points = new Vector<Integer>();
        
        points.add(i);
        
        clusters.put(exemplar, points);
      }
      else
      {
        clusters.get(exemplar).add(i);
      }
    }


    double end_time = System.nanoTime();
    
    double time = (end_time - start)/1000000000;
    
    HashSet<Covering_set> resulting_covering_sets = null;
    
    Set<Integer> ids = clusters.keySet();
    
    if(sort_cluster)
    {
      Vector<Integer> sorted_ids = new Vector<Integer>();
      
      Integer curr_id = 0;
      
      for(Integer id:ids)
      {
        if(sorted_ids.isEmpty())
        {
          curr_id = id;
          
          sorted_ids.add(id);
        }
        else
        {
          curr_id = get_next_cluster(distances, curr_id, clusters, sorted_ids, ids);
          
          sorted_ids.add(curr_id);
        }
      }
      
      
      
      
      for(Integer id : sorted_ids)
      {
        Vector<Integer> points = clusters.get(id);
        
//        HashSet<citation_view_vector> curr_covering_sets = new HashSet<citation_view_vector>();
        
        for(int i = 0; i<points.size(); i++)
        {
          if(resulting_covering_sets == null)
          {
            resulting_covering_sets = covering_sets_per_attributes.get(points.get(i));
          }
          else
          {
            resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(points.get(i)));
          }
        }
        
        
//        if(resulting_covering_sets == null)
//        {
//          resulting_covering_sets = curr_covering_sets;
//        }
//        else
//        {
//          resulting_covering_sets = join_operation(resulting_covering_sets, curr_covering_sets);
//        }
      }
    }
    else
    {
      for(Integer id : ids)
      {
        Vector<Integer> points = clusters.get(id);
        
//        HashSet<citation_view_vector> curr_covering_sets = new HashSet<citation_view_vector>();
        
        for(int i = 0; i<points.size(); i++)
        {
          if(resulting_covering_sets == null)
          {
            resulting_covering_sets = covering_sets_per_attributes.get(points.get(i));
          }
          else
          {
            resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(points.get(i)));
          }
        }
        
        
//        if(resulting_covering_sets == null)
//        {
//          resulting_covering_sets = curr_covering_sets;
//        }
//        else
//        {
//          resulting_covering_sets = join_operation(resulting_covering_sets, curr_covering_sets);
//        }
      }
    }
    
    
//    Vector<Integer> sorted_ids = new Vector<Integer>();
//    
//    Integer curr_id = 0;
//    
//    for(Integer id:ids)
//    {
//      if(sorted_ids.isEmpty())
//      {
//        curr_id = id;
//        
//        sorted_ids.add(id);
//      }
//      else
//      {
//        curr_id = get_next_cluster(distances, curr_id, clusters, sorted_ids, ids);
//        
//        sorted_ids.add(curr_id);
//      }
//    }
//    
//    
//    
//    
//    for(Integer id : sorted_ids)
//    {
//      Vector<Integer> points = clusters.get(id);
//      
////      HashSet<citation_view_vector> curr_covering_sets = new HashSet<citation_view_vector>();
//      
//      for(int i = 0; i<points.size(); i++)
//      {
//        if(resulting_covering_sets == null)
//        {
//          resulting_covering_sets = covering_sets_per_attributes.get(points.get(i));
//        }
//        else
//        {
//          resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(points.get(i)));
//        }
//      }
//      
//      
////      if(resulting_covering_sets == null)
////      {
////        resulting_covering_sets = curr_covering_sets;
////      }
////      else
////      {
////        resulting_covering_sets = join_operation(resulting_covering_sets, curr_covering_sets);
////      }
//    }
//    for(int i = 0; i<apro.getExemplars().length; i++)
//    {
//      int exemplar = apro.getExemplars()[i];
//            
//      if(covering_sets_per_cluster.get(exemplar) == null)
//      {
//        covering_sets_per_cluster.put(exemplar, covering_sets_per_attributes.get(i));
//      }
//      else
//      {
//        HashSet<citation_view_vector> resulting_covering_sets = covering_sets_per_cluster.get(exemplar);
//        
//        resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(i));
//        
//        covering_sets_per_cluster.put(exemplar, resulting_covering_sets);
//      }
//      
//    }
    
//    for(int i = 0; i<apro.getExemplars().length; i++)
//    {
//      int exemplar = apro.getExemplars()[i];
//            
//      if(covering_sets_per_cluster.get(exemplar) == null)
//      {
//        covering_sets_per_cluster.put(exemplar, covering_sets_per_attributes.get(i));
//      }
//      else
//      {
//        HashSet<citation_view_vector> resulting_covering_sets = covering_sets_per_cluster.get(exemplar);
//        
//        resulting_covering_sets = join_operation(resulting_covering_sets, covering_sets_per_attributes.get(i));
//        
//        covering_sets_per_cluster.put(exemplar, resulting_covering_sets);
//      }
//      
//    }
    
    
    return resulting_covering_sets;
    
  }
  
//  static HashSet<citation_view_vector> reasoning_covering_set_conjunctive_query(ArrayList<ConcurrentHashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, ConcurrentHashMap<Tuple, Integer> tuple_ids, boolean multi_thread) throws InterruptedException
//  {
//    
//    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(2));
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
//        for(j = 0; j<valid_view_mappings_per_head_var.size() + 2*i; j = j+2*i)
//        {
//          HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();
//          
//          for(int k = j; k<j+2*i && k < valid_view_mappings_per_head_var.size(); k++)
//          {
//            ConcurrentHashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
//            
//            Set<Single_view> views = valid_view_mappings.keySet();
//            
//            HashSet<Tuple> all_tuples = new HashSet<Tuple>();
//            
//            for(Iterator iter = views.iterator(); iter.hasNext();)
//            {
//              Single_view view = (Single_view) iter.next();
//              
//              HashSet<Tuple> tuples = valid_view_mappings.get(view);
//              
//              all_tuples.addAll(tuples);
//              
//              
//            }
//            
//            view_com = join_views_curr_relation(all_tuples, view_com, args, tuple_ids);
//            
//          }
//          
//          if(!view_com.isEmpty())
//             covering_sets.add(view_com);
//        }
//        
//        
//      }
//      else
//      {
//        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(2*i));
//        
//        ArrayList<Calculate_covering_sets> cal_threads = new ArrayList<Calculate_covering_sets>(); 
//        
//        
//        for(int k = 0; k<covering_sets.size(); k=k+2)
//        {
//          if(k + 2 - 1 < covering_sets.size())
//          {
//            Calculate_covering_sets cal_thread = new Calculate_covering_sets(covering_sets, k, k + 2);
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
//            covering_sets.set(k/2, covering_sets.get(k));
//          }
//        }
//        
//        for(int p = 0; p<cal_threads.size(); p++)
//        {
//          cal_threads.get(p).join();
//        }
//        
//        for(int k = 0; k<covering_sets.size(); k = k + 2)
//        {
//          
//          if(k + 2 - 1 < covering_sets.size())
//          {
//            HashSet<citation_view_vector> updated_covering_set = cal_threads.get(k/2).get_reasoning_result();
//            
//            covering_sets.set(k/2, updated_covering_set);
//          }
//          
//        }
//        
//        
//        int redundant_start = (covering_sets.size() + 1)/2;
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
//    return covering_sets.get(0);
//  }
  
  public static HashSet<Covering_set> join_operation(HashSet<Covering_set> c_combinations, HashSet<Covering_set> insert_citations)
  {
/*      if(i == 0)
      {

          c_combinations.addAll(insert_citations);
          
          return c_combinations;
      }
      else*/
    
    if(insert_citations.isEmpty())
      return c_combinations;
    
    if(c_combinations.isEmpty())
      return insert_citations;
    
      {
          
          HashSet<Covering_set> updated_c_combinations = new HashSet<Covering_set>();
          
          for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
          {
              
              Covering_set curr_combination1 = (Covering_set) iter.next();
              
              HashSet<Covering_set> iterated_covering_sets = new HashSet<Covering_set>();
                              
              for(Iterator it = insert_citations.iterator(); it.hasNext();)
              {
                  
                  Covering_set curr_combination2 = (Covering_set)it.next(); 
                  
                  iterated_covering_sets.add(curr_combination2);
                  
                  Covering_set new_citation_vec = curr_combination2.clone();
                  
//                  if(new_citation_vec.c_vec.size() == 1)
//                  {
//                    for(citation_view c :new_citation_vec.c_vec)
//                    {
//                      if(c.get_name().equals("v8"))
//                      {
//                        System.out.println(c.get_table_name_string() + "::" + c.get_view_tuple());
//                      }
//                    }
//                  }
                  
                  Covering_set new_covering_set = curr_combination1.merge(new_citation_vec);
                  
//                  if(curr_combination1.toString().equals("v14*v15*v29*v4"))
//                  {
//                    System.out.println("merge_covering_sets::" + new_covering_set.toString() + "::tables::" + new_covering_set.table_names + "::attributes::" + new_covering_set.head_variables);
//                  }
//                  
//                  if(new_covering_set.toString().equals("v14*v15*v29*v4*v8"))
//                  {
//                    System.out.println("merge_covering_sets::" + new_covering_set.toString() + "::tables::" + new_covering_set.table_names + "::attributes::" + new_covering_set.head_variables);
//                  }
                  
                  remove_duplicate(updated_c_combinations, new_covering_set);
                  
//                  System.out.println(new_covering_set + "|||||" + updated_c_combinations);
              }
          }
          
          return updated_c_combinations;
          
      }
  }
  
//  public static HashSet<citation_view_vector> join_views_curr_relation(HashSet<Tuple> tuples, HashSet<citation_view_vector> curr_view_com, Vector<Argument> args, ConcurrentHashMap<Tuple, Integer> tuple_ids)
//  {
//      if(curr_view_com.isEmpty())
//      {
//          if(tuples.isEmpty())
//              return new HashSet<citation_view_vector>();
//          else
//          {
//              HashSet<citation_view_vector> new_view_com = new HashSet<citation_view_vector>();
//              
//              for(Tuple tuple:tuples)
//              {
//                  
//                  Tuple valid_tuple = (Tuple) tuple.clone();
//                  
//                  valid_tuple.args.retainAll(args);
//                                    
//                  if(valid_tuple.lambda_terms.size() > 0)
//                  {
//                      
//                      citation_view_parametered c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple, query_subgoal_id_mappings, query_arg_id_mappings, tuple_ids);
//                      
//                      citation_view_vector curr_views = new citation_view_vector(c);
//                      
//                      remove_duplicate(new_view_com, curr_views);
//                  }   
//                  else
//                  {
//                      
//                      citation_view_unparametered c = new citation_view_unparametered(valid_tuple.name, valid_tuple, query_subgoal_id_mappings, query_arg_id_mappings, tuple_ids);
//                      
//                      citation_view_vector curr_views = new citation_view_vector(c);
//                      
//                      remove_duplicate(new_view_com, curr_views);
//                  }
//              }
//              
//              return new_view_com;
//          }
//      }
//      
//      else
//      {
//          HashSet<citation_view_vector> new_view_com = new HashSet<citation_view_vector>();
//          
//          for(Tuple tuple:tuples)
//          {
//              Tuple valid_tuple = (Tuple)tuple.clone();
//              
//              valid_tuple.args.retainAll(args);
//              
//              citation_view c = null;
//              
//              if(valid_tuple.lambda_terms.size() > 0)
//              {
//                  
//                  c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple);
//              }   
//              else
//              {
//                  
//                  c = new citation_view_unparametered(valid_tuple.name, valid_tuple);
//              }
//              
//              for(Iterator iter = curr_view_com.iterator(); iter.hasNext();)
//              {
//                  citation_view_vector old_view_com = (citation_view_vector)iter.next();
//                  
//                  citation_view_vector old_view_com_copy = old_view_com.clone(); 
//                  
//                  citation_view_vector view_com = citation_view_vector.merge(old_view_com_copy, c);
//                  
////                  HashSet<String> string_list = new HashSet<String>();
////                  
////                  for(int j = 0; j<view_com.c_vec.size(); j++)
////                  {
////                      string_list.add(view_com.c_vec.get(j).get_name());
////                  }
////                  
////                if(string_list.contains("v4") && string_list.contains("v8") && string_list.contains("v11") && string_list.contains("v6") && string_list.contains("v14") && string_list.contains("v20"))
////                {
////                    int y = 0;
////                    
////                    y++;
////                }
////                if(string_list.contains("v4") && string_list.contains("v8"))
////                {
////                    int y = 0;
////                    
////                    y++;
////                }
//                  
//                  remove_duplicate(new_view_com, view_com);
//              }
//          }
//          
//          return new_view_com;
//      }
//  }
//  
//  public static HashSet<citation_view_vector> remove_duplicate_arg(HashSet<citation_view_vector> c_combinations, citation_view_vector c_view)
//  {
//      int i = 0;
//      
//      if(c_combinations.contains(c_view))
//          return c_combinations;
//              
//      for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
//      {
////        String str = (String) iter.next();
//                      
//          citation_view_vector c_combination = (citation_view_vector) iter.next();
//          {
//              {
//                  citation_view_vector curr_combination = c_view;
//                  if(view_vector_contains(c_combination, curr_combination)&& curr_combination.head_variables.containsAll(c_combination.head_variables) && c_combination.index_vec.size() > curr_combination.index_vec.size())
//                  {
//                      iter.remove();                      
//                  }
//                  
//                  if(view_vector_contains(curr_combination, c_combination) && c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
//                  {
//                      break;
//                  }
//              }
//              
//          }
//          
//          i++;
//      }
//      
//      
//      if(i >= c_combinations.size())
//          c_combinations.add(c_view);
//      
//              
//      return c_combinations;
//  }
  
  public static HashSet<Covering_set> remove_duplicate(HashSet<Covering_set> c_combinations, Covering_set c_view)
  {
      int i = 0;
      
      if(c_combinations.contains(c_view))
      {
        return c_combinations;
      }
                    
      boolean removed = false;
      
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
                  
                  if(Covering_set.contains(c_combination.tuple_index, curr_combination.tuple_index) && Covering_set.contains(curr_combination.arg_name_index,  c_combination.arg_name_index) && Covering_set.contains(curr_combination.table_name_index, c_combination.table_name_index))
                  {
                      iter.remove();   
                      
                      
                      
                  }
                  
//                  if(curr_combination.c_vec.containsAll(c_combination.c_vec) && c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
                  if(Covering_set.contains(curr_combination.tuple_index, c_combination.tuple_index) && Covering_set.contains(c_combination.arg_name_index,  curr_combination.arg_name_index) && Covering_set.contains(c_combination.table_name_index, curr_combination.table_name_index))
                  {
                    
                    removed = true;
                    
                      break;
                  }
              }
              
          }
          
          i++;
      }
      
      if(!removed)
      {
        c_combinations.add(c_view);
        
      }
              
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
  
  static ConcurrentHashMap<Single_view, HashSet<Tuple>> get_all_possible_view_mappings(ConcurrentHashMap<String, Integer> subgoal_id_mappings, Query q)
  {
    Database canDb = CoreCover.constructCanonicalDB(q.body, q.subgoal_name_mapping);
    
    ConcurrentHashMap<Single_view, HashSet<Tuple>> view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
    
    for(int i = 0; i<view_objs.size(); i++)
    {
      Single_view view = view_objs.get(i);
      
//      System.out.println(view.subgoal_name_id_mappings);
      
      view.build_view_mappings(q.body, canDb, subgoal_id_mappings, q.head.args);
      
      if(!view.view_mappings.isEmpty())
        view_mappings.put(view, view.view_mappings);
      
    }
    
    return view_mappings;
  }
  
  private static void printResult(ResultSet rs) throws SQLException {
    /********************************************************************************/
    System.out.println();
    System.out.println("-------------------------------------------------------------------------------");
    for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        System.out.print(rs.getMetaData().getColumnLabel(i) + "\t|");
//    System.out.println();
//    System.out.println("-------------------------------------------------------------------------------");
//    
//    while(rs.next()) {      
//        for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
//            System.out.print(rs.getString(i) + "\t|");
//        System.out.println();
//    }
//    System.out.println("-------------------------------------------------------------------------------");
//    System.out.println();
//    System.out.println();
}
  
}
