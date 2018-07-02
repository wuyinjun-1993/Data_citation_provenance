package edu.upenn.cis.citation.views;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import javax.swing.text.View;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.CoreCover;
import edu.upenn.cis.citation.Corecover.Database;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.Operation.Operation;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;

public class Single_view {
  
  public static HashMap<String, Vector<Integer>> relation_primary_key_mappings = new HashMap<String, Vector<Integer>>(); 
  
  public String token_string = new String ();
  
  public String view_name = new String();
  
  public Vector<Lambda_term> lambda_terms = new Vector<Lambda_term>();
  
  public HashMap<String, String> subgoal_name_mappings = new HashMap<String, String>();
  
  public HashSet<Tuple> view_mappings = new HashSet<Tuple>();
  
  public HashMap<Tuple, Vector<Integer>> view_mapping_view_why_prov_token_col_ids_mapping = new HashMap<Tuple, Vector<Integer>>();
  
  public HashMap<Tuple, Vector<Integer>> view_mapping_q_why_prov_token_col_ids_mapping = new HashMap<Tuple, Vector<Integer>>();
  
  public HashMap<Tuple, Vector<int[][]>> view_mapping_condition_ids_mappings = new HashMap<Tuple, Vector<int[][]>>();
  
  public HashMap<Tuple, Vector<int[]>> view_mapping_lambda_term_ids_mappings = new HashMap<Tuple, Vector<int[]>>();
  
  public HashMap<Tuple, Vector<int[]>> view_mapping_q_grouping_attr_ids_mappings = new HashMap<Tuple, Vector<int[]>>();
  
  public HashMap<Tuple, Vector<int[]>> view_mapping_view_grouping_attr_ids_mappings = new HashMap<Tuple, Vector<int[]>>();
  
  public HashMap<Tuple, int[]> view_mapping_query_head_var_attr_in_view_head_ids_mappings = new HashMap<Tuple, int[]>();
  
  public HashMap<String, Integer> subgoal_name_id_mappings = new HashMap<String, Integer>();

  //subgoals
  public Vector<Subgoal> subgoals = new Vector<Subgoal>();
  
  public Vector<Conditions> conditions = new Vector<Conditions>();;
    
  public Vector<String> operation = new Vector<String>();
  
  //heads
  public Subgoal head;
  
  //mappings between why provenance tokens and lambda variables
  public HashMap<String, Vector<String>> why_prov_lambda_variable_mappings = new HashMap<String, Vector<String>>();
  
  //mappings between subgoal and lambda terms
  public HashMap<String, Vector<Lambda_term>> subgoal_lambda_term_mappings = new HashMap<String, Vector<Lambda_term>>();
  
  //mappings between why provenance tokens and ids in the why_provenance vector
  public HashMap<String, Vector<String[]>> why_prov_ids_mappings = new HashMap<String, Vector<String[]>>();
  
  //vector to store why and where provenances
  public Vector<String []> why_where_provs = new Vector<String []>();
  
  public String token_sequence = new String();
  
  public static String [] numeric_data_type = {"smallint","integer", "bigint", "decimal", "numeric","real","double precision","serial", "bigserial"};
  
  //each cluster is id of subgoals
  public Vector<HashSet<Integer>> cluster_subgoal_ids = new Vector<HashSet<Integer>>();
  
  public Vector<HashSet<Integer>> cluster_condition_ids = new Vector<HashSet<Integer>>();
  
  public boolean has_having_clause = false;
  
  public HashMap<String, Query> citation_queries = new HashMap<String, Query>();
  
  public String local_with_clause = null;
  
  //construction function for conjunctive queries
  
//  void init_condition_ids(Vector<Conditions> conditions, HashMap<String, Integer> subgoal_names)
//  {
//    for(int i = 0; i<conditions.size(); i++)
//    {
//      Conditions condition = conditions.get(i);
//      
//      String subgoal1 = condition.subgoal1;
//      
//      int subgoal_id1 = subgoal_names.get(subgoal1);
//      
//      Argument arg1 = condition.arg1;
//      
//      int arg_id1 = subgoals.get(subgoal_id1).args.indexOf(arg1);
//      
//      Argument arg2 = condition.arg2;
//      
//      int subgoal_id2 = -1;
//      
//      int arg_id2 = -1;
//      
//      if(!arg2.isConst())
//      {
//        String subgoal2 = condition.subgoal2;
//        
//        subgoal_id2 = subgoal_names.get(subgoal2);
//        
//        arg_id2 = subgoals.get(subgoal_id2).args.indexOf(arg2);
//      }
//      
//      int [][]ids = new int[2][2];
//      
//      ids[0][0] = subgoal_id1;
//      
//      ids[0][1] = arg_id1;
//      
//      ids[1][0] = subgoal_id2;
//      
//      ids[1][1] = arg_id2;
//      
//      condition_ids.add(ids);
//      
//    }
//  }
//  
  
  public static void build_index(Vector<String> arguments, String view_name, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "create index on " + view_name + "(";
    
    for(int i = 0; i<arguments.size(); i++)
    {
      if(i >= 1)
        query += ",";
      
      query += arguments.get(i);
    }
    
    query += ")";
    
    pst = c.prepareStatement(query);
    
    pst.execute();
    
  }
  
  
  static Vector<String> get_relation_columns(String table, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "select column_name from information_schema.columns where table_name = '" + table + "'";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    Vector<String> cols = new Vector<String>();
    
    while(rs.next())
    {
      cols.add(rs.getString(1));
    }
    
    return cols;
  }
  
  public static void get_relation_primary_key(Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "select tc.table_name, kc.column_name "
        + "from information_schema.table_constraints tc "
        + "join information_schema.key_column_usage kc "
        + "on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema "
        + " and kc.constraint_name = tc.constraint_name where tc.constraint_type = 'PRIMARY KEY' ";
//        + "and kc.position_in_unique_constraint is not null";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    HashMap<String, Vector<String>> relation_attr_mappings = new HashMap<String, Vector<String>>();
    
    while(rs.next())
    {
      String table_name = rs.getString(1);
      
      Vector<String> all_col_names = relation_attr_mappings.get(table_name);
      
      if(all_col_names == null)
      {
        all_col_names = get_relation_columns(table_name, c, pst);
        
        relation_attr_mappings.put(table_name, all_col_names);
      }
      
      String col_name = rs.getString(2);
      
      int id = all_col_names.indexOf(col_name);
      
//      System.out.println(table_name + "::" + id); 
      
      Vector<Integer> col_name_ids = relation_primary_key_mappings.get(table_name);
      
      if(col_name_ids == null)
      {
        col_name_ids = new Vector<Integer>();
        
        col_name_ids.add(id);
        
        relation_primary_key_mappings.put(table_name, col_name_ids);
      }
      else
        relation_primary_key_mappings.get(table_name).add(id);
    }
  }
  
  
  public static void clear_views_in_database(Connection c, PreparedStatement pst) throws SQLException
  {
//    String sql = "select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false))";
    
    String sql = "SELECT oid::regclass::text FROM pg_class WHERE relkind = 'm'";
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    while(rs.next())
    {
      String view_name = rs.getString(1);
      
      String drop_sql = "drop MATERIALIZED view " + view_name;
      
      pst = c.prepareStatement(drop_sql);
      
      pst.execute();
    }
  }
  
  public static void materilization(Single_view view, Connection c, PreparedStatement pst) throws SQLException
  {
    if(!view.head.has_agg)
      return;
    
    Vector<String> indexed_cols = new Vector<String>();
    
    Vector<String> grouping_attrs_strings = new Vector<String>();
    
    Vector<String> provenance_attrs_strings = new Vector<String>();
    
    String sql = Query_converter.datalog2sql_view_conjunction(view, indexed_cols, grouping_attrs_strings, provenance_attrs_strings);
    
    System.out.println(indexed_cols);
    
    String view_query = "create MATERIALIZED view " + view.view_name + " as (" + sql + ")";
    
    System.out.println(view_query);
    
    pst = c.prepareStatement(view_query);
    
    pst.execute();
    
//    build_index(provenance_attrs_strings, view.view_name, c, pst);
//    build_index_on_materilized_views_for_grouping_attributes(view, c, pst);
//    
    build_index_on_materilized_views_for_provenance_cols(view, indexed_cols, c, pst);
  }
  
  public static void materilization2(Single_view view, Connection c, PreparedStatement pst) throws SQLException
  {
    if(!view.head.has_agg)
      return;
    
    Vector<String> indexed_cols = new Vector<String>();
    
    Vector<String> grouping_attrs_strings = new Vector<String>();
    
    Vector<String> provenance_attrs_strings = new Vector<String>();
    
    String sql = Query_converter.datalog2sql_materializations(view, relation_primary_key_mappings, false);
    
    System.out.println(indexed_cols);
    
    String view_query = "create MATERIALIZED view " + view.view_name + " as (" + sql + ")";
    
    System.out.println(view_query);
    
    pst = c.prepareStatement(view_query);
    
    pst.execute();
    
//    build_index(provenance_attrs_strings, view.view_name, c, pst);
//    build_index_on_materilized_views_for_grouping_attributes(view, c, pst);
//    
//    build_index_on_materilized_views_for_provenance_cols(view, indexed_cols, c, pst);
  }
  
//  static void build_index_on_materilized_views(Single_view view, Vector<String> indexed_cols, Connection c, PreparedStatement pst) throws SQLException
//  {
//    for(int i = 0; i < indexed_cols.size(); i++)
//    {
//      String sql = "create UNIQUE index " + view.view_name + "_" + indexed_cols.get(i) + " on " + view.view_name + "(" + indexed_cols.get(i) + ")";
//      
//      pst = c.prepareStatement(sql);
//      
//      pst.execute();
//    }
//  }
  static void build_index_on_materilized_views_for_grouping_attributes(Single_view view, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "create index on " + view.view_name + "(";
    
    for(int i = 0; i<view.head.args.size(); i++)
    {
      if(i >= 1)
        sql += ",";
      
      Argument arg = (Argument) view.head.args.get(i);
      
      sql += arg.relation_name + "_" + arg.attribute_name;
    }
    
    sql += ")";
    
    System.out.println(sql);
    
    pst = c.prepareStatement(sql);
    
    pst.execute();
  }
  
  
  static void build_index_on_materilized_views_for_provenance_cols(Single_view view, Vector<String> indexed_cols, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "create UNIQUE index " + view.view_name + "_index" + " on " + view.view_name + "(";
    
    for(int i = 0; i < indexed_cols.size(); i++)
    {
      if(i >= 1)
        sql += ",";
      
      
      sql += indexed_cols.get(i);
      
//      String sql = "create UNIQUE index " + view.view_name + "_" + indexed_cols.get(i) + " on " + view.view_name + "(" + indexed_cols.get(i) + ")";
      
      
    }
    
    sql += ")";
    
    System.out.println(sql);
    
    pst = c.prepareStatement(sql);
    
    pst.execute();
  }
  
  public void load_citation_queries(HashMap<String, String> view_citation_query_mappings, HashMap<String, Query> name_citation_query_mappings)
  {
    for(Entry<String, String> view_citation_query_mapping: view_citation_query_mappings.entrySet())
    {
      citation_queries.put(view_citation_query_mapping.getKey(), name_citation_query_mappings.get(view_citation_query_mapping.getValue()));
    }
  }
  
  void cluster_relational_subgoals()
  {
//    Vector<HashSet<Integer>> cluster_ids = new Vector<HashSet<Integer>>();
    
    for(int j = 0; j<conditions.size(); j++)
    {
      
      Conditions condition = conditions.get(j);
      
      if(condition.agg_function1 != null || condition.agg_function2 != null)
        continue;
      
      String subgoal1 = conditions.get(j).subgoal1.get(0);
      
//      String subgoal2 = conditions.get(j).subgoal2.get(0);
      
      int id1 = subgoal_name_id_mappings.get(subgoal1);
      
      int id2 = -1;
      
      if(conditions.get(j).subgoal2.size() > 0)
        id2 = subgoal_name_id_mappings.get(conditions.get(j).subgoal2.get(0));
      
//      if(subgoal2 != null && !subgoal2.isEmpty())
//        id2 = subgoal_name_id_mappings.get(subgoal2);
      
      
      int i = 0;
      
      int matched_cluster_id1 = -1;
      
      int matched_cluster_id2 = -1;
      
      for(i = 0; i<cluster_subgoal_ids.size(); i++)
      {
        HashSet<Integer> curr_cluster = cluster_subgoal_ids.get(i);
        
        
        
        for(Integer id:curr_cluster)
        {
          if(id == id1)
          {
            matched_cluster_id1 = i;
            
          }
          
          if(id == id2)
          {
            matched_cluster_id2 = i;
            
          }
        }
      }
      
      if(matched_cluster_id1 < 0 && matched_cluster_id2 < 0)
      {
        HashSet<Integer> new_clusters = new HashSet<Integer>();
        
        new_clusters.add(id1);
        
        if(id2 >= 0)
          new_clusters.add(id2);
        
        HashSet<Integer> curr_cluster_condition_ids = new HashSet<Integer>();
        
        curr_cluster_condition_ids.add(j);
        
      }
      else
      {
        if(matched_cluster_id1 < 0 && matched_cluster_id2 >= 0)
        {
          cluster_subgoal_ids.get(matched_cluster_id2).add(id1);
        }
        else
        {
          if(matched_cluster_id2 < 0 && matched_cluster_id1 >= 0)
          {
            
            if(id2 >= 0)
              cluster_subgoal_ids.get(matched_cluster_id1).add(id2);
          }
          else
          {
            cluster_subgoal_ids.get(matched_cluster_id1).addAll(cluster_subgoal_ids.get(matched_cluster_id2));
            
            cluster_subgoal_ids.removeElementAt(matched_cluster_id2);
            
          }
        }
      }
      
      
    }
    
  }
  
  public Single_view(Query view, String view_name, Connection c, PreparedStatement pst) throws SQLException
  {
    subgoals = view.body;
    
    conditions = view.conditions;
    
    for(int i = 0; i<conditions.size(); i++)
    {
      Conditions condition = conditions.get(i);
      
      if(condition.agg_function1 != null || condition.agg_function2 != null)
      {
        has_having_clause = true;
      }
      
    }
    
    head = view.head;
        
    this.lambda_terms = view.lambda_term;
    
    this.subgoal_name_mappings = new HashMap<>();
    
    this.subgoal_name_mappings.putAll(view.subgoal_name_mapping);
    
    this.view_name = view_name;
    
    //build mappings between subgoal and lambda terms
    build_subgoal_lambda_term_mappings_conjunctive_query(view);
        
    for(int i = 0; i<subgoals.size();i++)
    {
      subgoal_name_id_mappings.put(subgoals.get(i).name, i);
    }
    
    cluster_relational_subgoals();
    
    local_with_clause = gen_local_with_clause(this);
    
//    init_condition_ids(conditions, subgoal_name_id_mappings);
    
//    String sql = Query_converter.data2sql_with_token_columns(view);
//        
//    pst = c.prepareStatement(sql);
//    
//    ResultSet rs = pst.executeQuery();
//    
//    ResultSetMetaData meta_data = rs.getMetaData();
//    
//    while(rs.next())
//    {
//      store_why_where_prov_tokens(rs, view);
//    }
//    
//    System.out.println(token_sequence);
  }
  
  void store_why_where_prov_tokens(ResultSet rs, Query view) throws SQLException
  {
    
    String [] why_where_prov = new String[view.head.args.size() + view.body.size()];
    
    String curr_token_sequence = "(";
    
    int num = 0;
    
    for(int i = 0; i<view.head.size(); i++)
    {
      
      if(i >= 1)
        curr_token_sequence += ",";
      
      String where_token = rs.getString(i + 1);
      
      why_where_prov[num++] = where_token;
      
      curr_token_sequence += where_token;     
      
    }
    
    for(int i = 0; i<view.body.size(); i++)
    {
      String why_prov_token = rs.getString(i + 1 + view.head.args.size());
      
      if(why_prov_ids_mappings.get(why_prov_token) == null)
      {
        Vector<String[]> ids = new Vector<String[]>();
        
        ids.add(why_where_prov);
        
        why_prov_ids_mappings.put(why_prov_token, ids);
      }
      else
      {
        why_prov_ids_mappings.get(why_prov_token).add(why_where_prov);
      }
      
      why_where_prov[num++] = why_prov_token;
      
      curr_token_sequence += "," + why_prov_token;
    }
    
    why_where_provs.add(why_where_prov);
    
    token_sequence += curr_token_sequence + ")";
  }
  
  void build_subgoal_lambda_term_mappings_conjunctive_query(Query view)
  {
    for(int i = 0; i<view.lambda_term.size(); i++)
    {
      Lambda_term l_term = view.lambda_term.get(i);
      
      String relation_name = l_term.table_name;
      
      String arg_name = l_term.arg_name.substring(l_term.arg_name.indexOf(init.separator) + 1, l_term.arg_name.length());
      
      if(subgoal_lambda_term_mappings.get(relation_name) == null)
      {
        Vector<Lambda_term> arg_names = new Vector<Lambda_term>();
        
        arg_names.add(l_term);
        
        subgoal_lambda_term_mappings.put(relation_name, arg_names);
      }
      else
      {
        subgoal_lambda_term_mappings.get(relation_name).add(l_term);
      }
      
    }
  }
  
  public void build_view_mappings(Vector<Subgoal> subgoals, HashMap<String, String> subgoal_name_mappings)
  {
    Database canDb = CoreCover.constructCanonicalDB(subgoals, subgoal_name_mappings);
    
    view_mappings = CoreCover.computeViewTuples(canDb, this);
    
    for(Iterator iter = view_mappings.iterator(); iter.hasNext();)
    {
      Tuple tuple = (Tuple) iter.next();
      
      Vector<Integer> v_why_col_ids = get_view_why_token_column_ids(tuple);
      
      view_mapping_view_why_prov_token_col_ids_mapping.put(tuple, v_why_col_ids);
      
      Vector<Integer> q_why_col_ids = get_q_why_token_column_ids(tuple, v_why_col_ids, subgoals);
      
      view_mapping_q_why_prov_token_col_ids_mapping.put(tuple, q_why_col_ids);
    }
    
  }
  
  static boolean build_grouping_attr_id_mappings(Vector<Argument> view_grouping_attrs, Vector<Argument> query_grouping_attrs, HashMap<Tuple, Vector<int[]>> view_mapping_grouping_attr_ids_mappings, HashMap<Tuple, int[]> view_mapping_query_head_var_attr_in_view_head_ids_mappings, Tuple tuple, ConcurrentHashMap<String, Integer> subgoal_id_mappings, Vector<Subgoal> subgoals)
  {
    Vector<int[]> ids = new Vector<int[]>();
    
    for(int i = 0; i<view_grouping_attrs.size(); i++)
    {
      Argument arg = view_grouping_attrs.get(i);
      
      String rel_name = arg.relation_name;//name.substring(0, arg.name.indexOf(init.separator));
      
      Object rel_name_in_query = tuple.mapSubgoals_str.get(rel_name);
      
      if(rel_name_in_query == null)
        continue;
      
      int subgoal_id = subgoal_id_mappings.get((String)rel_name_in_query);
      
      int arg_id = subgoals.get(subgoal_id).args.indexOf(arg);
      
      int[] curr_ids = new int[2];
      
      curr_ids[0] = subgoal_id;
      
      curr_ids[1] = arg_id;
      
      ids.add(curr_ids);
    }
    view_mapping_grouping_attr_ids_mappings.put(tuple, ids);
    
    int[] query_attr_view_head_ids = new int[query_grouping_attrs.size()];
    
    for(int i = 0; i < query_grouping_attrs.size(); i++)
    {
      Argument arg = query_grouping_attrs.get(i);
      
      query_attr_view_head_ids[i] = view_grouping_attrs.indexOf(tuple.reverse_phi.apply(arg));
    }
    
    view_mapping_query_head_var_attr_in_view_head_ids_mappings.put(tuple, query_attr_view_head_ids);
    
    return true;
  }
  
  static Vector<Argument> get_query_attrs_reverse_mapped_attrs(Vector<Argument> args, Tuple tuple)
  {
    Vector<Argument> reverse_mapped_args = new Vector<Argument>();
    
    for(int i = 0; i < args.size(); i++)
    {
      reverse_mapped_args.add(tuple.reverse_phi.apply(args.get(i)));
    }
    
    return reverse_mapped_args;
  }
  
  public void build_view_mappings(Vector<Subgoal> subgoals, Database canDb, ConcurrentHashMap<String, Integer> subgoal_id_mappings, Vector<Argument> q_head_vars)
  {
    
    view_mappings = CoreCover.computeViewTuples(canDb, this);
    
    for(Iterator iter = view_mappings.iterator(); iter.hasNext();)
    {
      Tuple tuple = (Tuple) iter.next();
      
      if(!build_grouping_attr_id_mappings(head.args, q_head_vars, view_mapping_view_grouping_attr_ids_mappings, view_mapping_query_head_var_attr_in_view_head_ids_mappings, tuple, subgoal_id_mappings, subgoals))
        continue;
      
      
//      Vector<Integer> row_ids = new Vector<Integer>();
//      
//      tuple_valid_rows.put(tuple, row_ids);
      
      Vector<Integer> v_why_col_ids = get_view_why_token_column_ids(tuple);
      
      view_mapping_view_why_prov_token_col_ids_mapping.put(tuple, v_why_col_ids);
      
      Vector<Integer> q_why_col_ids = get_q_why_token_column_ids(tuple, v_why_col_ids, subgoals);
      
      view_mapping_q_why_prov_token_col_ids_mapping.put(tuple, q_why_col_ids);
      
      HashMap<Integer, Integer> v_why_col_id_q_why_col_id_mappings = new HashMap<Integer, Integer>();
      
      for(int i = 0; i<v_why_col_ids.size(); i++)
      {
        v_why_col_id_q_why_col_id_mappings.put(q_why_col_ids.get(i), i);
      }
      
      Vector<int[][]> ids = new Vector<int[][]>();
      
      for(int i = 0; i<tuple.conditions.size(); i++)
      {
        Conditions condition = tuple.conditions.get(i);
        
        if(condition.agg_function1 != null || condition.agg_function2 != null)
          continue;
        
        int subgoal_id1 = -1;
        
        int arg_id1 = -1;
        
        Integer q_subgoal_id1 = -1;
        
        if(condition.get_mapping1)
        {
          String subgoal1 = condition.subgoal1.get(0);
          
          subgoal_id1 = subgoal_id_mappings.get(subgoal1);
          
          q_subgoal_id1 = v_why_col_id_q_why_col_id_mappings.get(subgoal_id1);
          
          if(q_subgoal_id1 == null)
          {
            q_subgoal_id1 = -1;
          }
                  
          Argument arg1 = condition.arg1.get(0);
          
          arg_id1 = subgoals.get(subgoal_id1).args.indexOf(arg1);
        }
        
        
        
        
        Argument arg2 = condition.arg2.get(0);
        
        int subgoal_id2 = -1;
        
        int arg_id2 = -1;
        
        Integer q_subgoal_id2 = -1;
        
        if(!arg2.isConst() && condition.get_mapping2)
        {
          String subgoal2 = condition.subgoal2.get(0);
          
          subgoal_id2 = subgoal_id_mappings.get(subgoal2);
          
          q_subgoal_id2 = v_why_col_id_q_why_col_id_mappings.get(subgoal_id2);
          
          if(q_subgoal_id2 == null)
          {
            q_subgoal_id2 = -1;
          }
          
          arg_id2 = subgoals.get(subgoal_id2).args.indexOf(arg2);
        }
        
        int [][] curr_ids = new int[2][2];
        
        curr_ids[0][0] = q_subgoal_id1;
        
        curr_ids[0][1] = arg_id1;
        
        curr_ids[1][0] = q_subgoal_id2;
        
        curr_ids[1][1] = arg_id2;
        
        if(q_subgoal_id1 >= 0 || q_subgoal_id2 >= 0)
          ids.add(curr_ids);
      }
      
      Vector<int[]> l_ids = new Vector<int[]>();
      
      for(int i = 0; i<tuple.lambda_terms.size(); i++)
      {
        Lambda_term l_term = tuple.lambda_terms.get(i);
        
        String subgoal1 = l_term.table_name;
        
        int subgoal_id1 = subgoal_id_mappings.get(subgoal1);
        
        Integer q_subgoal_id1 = v_why_col_id_q_why_col_id_mappings.get(subgoal_id1);
        
        Argument arg1 = l_term.arg;
        
        int arg_id1 = subgoals.get(subgoal_id1).args.indexOf(arg1);
        
        int[] curr_ids = new int[2];
        
        curr_ids[0] = q_subgoal_id1;
        
        curr_ids[1] = arg_id1;
        
        l_ids.add(curr_ids);
      }
      
      view_mapping_lambda_term_ids_mappings.put(tuple, l_ids);
      
      view_mapping_condition_ids_mappings.put(tuple, ids);

//      System.out.println(tuple);
    }
    
  }
  
  public Vector<Integer> get_view_why_token_column_ids(Tuple tuple)
  {
    HashMap<String, String> subgoal_mappings = tuple.mapSubgoals_str;
    
    Vector<Integer> ids = new Vector<Integer>();
    
    for(int i = 0; i<subgoals.size(); i++)
    {
      String name = subgoals.get(i).name;
      
      if(subgoal_mappings.get(name)!=null)
      {
        ids.add(i);
      }
      
    }
    
    return ids;
  }
  
  public Vector<Integer> get_q_why_token_column_ids(Tuple tuple, Vector<Integer> v_ids, Vector<Subgoal> q_subgoals)
  {
    Vector<Integer> q_ids = new Vector<Integer>();
    
    for(int i = 0; i<v_ids.size(); i++)
    {
      int v_id = v_ids.get(i);
      
      Subgoal subgoal = this.subgoals.get(v_id);
      
      Subgoal q_subgoal = (Subgoal) tuple.mapSubgoals.get(subgoal);
      
      int q_id = q_subgoals.indexOf(q_subgoal);
      
      q_ids.add(q_id);
      
    }
    
    return q_ids;
  }

  public void check_why_provenance_tokens(HashSet<Tuple> curr_view_mappings, Vector<String> q_why_tokens)
  {
    
    for(Iterator iter = curr_view_mappings.iterator(); iter.hasNext();)
    {
      Tuple tuple = (Tuple) iter.next();
      
      Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
      
      Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
      
      int num = 0;
      
      String token_seq = ".*\\(.*";
      
      for(int i = 0; i<subgoals.size(); i++)
      {
        if(i >= 1)
          token_seq += ",";
        
        if(i == v_why_column_ids.get(num))
        {          
          token_seq += "(" + q_why_tokens.get(q_why_column_ids.get(num)) + ")";
                 
          num++;
        }
        else
        {
          token_seq += ".*";
        }
      }
      
      token_seq += "\\).*";
      
      if(!token_string.matches(token_seq))
        iter.remove();   
      
    }
    
    
  }
  
  public boolean check_provenance_tokens(String q_why_token)
  {      
      if(!token_sequence.matches(q_why_token))
        return false;
      
      return true;
    
    
  }
  
  public boolean check_where_provenance_token(String where_token)
  {
    System.out.println();
    
    System.out.println(token_sequence);
    
    System.out.println(where_token);
    
    System.out.println();
    
    if(!token_sequence.matches(where_token))
      return false;
    
    return true;
    
  }
  
  public void check_where_why_provenance_tokens(HashSet<Tuple> curr_view_mappings, String where_token, Vector<String> q_why_tokens)
  {
    
    for(Iterator iter = curr_view_mappings.iterator(); iter.hasNext();)
    {
      Tuple tuple = (Tuple) iter.next();
      
      Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
      
      Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
      
      int num = 0;
      
      String token_seq = ".*\\(.*("+ where_token +",).*,";
      
      for(int i = 0; i<subgoals.size(); i++)
      {
        if(i >= 1)
          token_seq += ",";
        
        if(i == v_why_column_ids.get(num))
        {          
          token_seq += "(" + q_why_tokens.get(q_why_column_ids.get(num)) + ")";
                 
          num++;
        }
        else
        {
          token_seq += ".*";
        }
      }
      
      token_seq += "\\).*";
      
      if(!token_string.matches(token_seq))
        iter.remove();   
      
    }
    
    
  }
  
  public String get_q_why_provenance_token_seq(Vector<String> q_why_tokens, Tuple tuple)
  {    
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    int num = 0;
    
    String why_token_seq = ".*?\\(.*?";
    
    for(int i = 0; i<subgoals.size(); i++)
    {
      if(i >= 1)
        why_token_seq += ",";
      
      if(i == v_why_column_ids.get(num))
      {          
        why_token_seq += "(" + q_why_tokens.get(q_why_column_ids.get(num)) + ")";
               
        num++;
      }
      else
      {
        why_token_seq += ".*?";
      }
    }
    
    why_token_seq += "\\).*?";
    
    return why_token_seq;
    
  }
  
  void evaluate_single_subgoal_args(Head_strs values, Subgoal subgoal)
  {
    Vector<Argument> args = subgoal.args;
    
    for(int i = 0; i<args.size(); i++)
    {
      String value = values.head_vals.get(i);
      
      args.get(i).set_value(value);
    }
  }
  
  public void reset_values()
  {
    for(int i = 0; i<subgoals.size(); i++)
    {
      Vector<Argument> args = subgoals.get(i).args;
      
      for(int j = 0; j<args.size(); j++)
      {
        args.get(j).value = null;
      }
    }
  }
  
  public Vector<Head_strs> get_values_from_why_tokens(Tuple tuple, Vector<Head_strs> values)
  {
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Head_strs> provenances = new Vector<Head_strs>();
    
    
    for(Integer q_why_col_id : q_why_column_ids)
    {
      provenances.add(values.get(q_why_col_id));
    }
    
    return provenances;
    
  }
  
  public void evaluate_args(Vector<Head_strs> values, Tuple tuple)
  {
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<int[][]> condition_ids = view_mapping_condition_ids_mappings.get(tuple);
    
    for(int i = 0; i<condition_ids.size();i++)
    {
      int [][]ids = condition_ids.get(i);
      
      int subgoal_id1 = ids[0][0];
      
      int subgoal_id2 = ids[1][0];
      
      if(subgoal_id1 >= 0)
      {
        Head_strs curr_values = values.get(q_why_column_ids.get(subgoal_id1));
        
        Argument arg1 = (Argument) subgoals.get(v_why_column_ids.get(subgoal_id1)).args.get(ids[0][1]);
        
        arg1.set_value(curr_values.head_vals.get(ids[0][1]).toLowerCase());
      }
      
      if(subgoal_id2 >= 0)
      {
        Head_strs curr_values = values.get(q_why_column_ids.get(subgoal_id2));
        
        Argument arg2 = (Argument) subgoals.get(v_why_column_ids.get(subgoal_id2)).args.get(ids[1][1]);
        
        arg2.set_value(curr_values.head_vals.get(ids[1][1]).toLowerCase());
      }
    }
    
//    for(int i = 0; i<q_why_column_ids.size(); i++)
//    {
//      Head_strs curr_values = values.get(q_why_column_ids.get(i));
//      
//      evaluate_single_subgoal_args(curr_values, subgoals.get(v_why_column_ids.get(i)));
//      
//    }
    

  }

  String clean_boolean_type(Argument arg, String string)
  {
    if(arg.data_type.equals("boolean"))
    {
      if(string.equals("t"))
      {
        return "true";
      }
      if(string.equals("f"))
      {
        return "false";
      }
    }
    return string;
  }
  
  public Head_strs evaluate_args_with_provenance(Vector<Head_strs> values, Tuple tuple)
  {
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<String> view_provenance_values = new Vector<String>();
    
    for(int i = 0; i<q_why_column_ids.size(); i++)
    {
      Subgoal subgoal = subgoals.get(v_why_column_ids.get(i));
      
//      for(int j = 0; j<subgoal.args.size(); j++)
      Vector<Integer> primary_key_ids = Single_view.relation_primary_key_mappings.get(tuple.query.subgoal_name_mappings.get(subgoal.name));
      
//      for(int j = 0; j<subgoal.args.size(); j++)
      for(int j = 0; j < primary_key_ids.size(); j ++)
      {
        view_provenance_values.add(clean_boolean_type((Argument) subgoal.args.get(primary_key_ids.get(j)), values.get(q_why_column_ids.get(i)).head_vals.get(primary_key_ids.get(j))));
//        view_provenance_values.add(values.get(q_why_column_ids.get(i)).head_vals.get(j));
      }
      
    }
    return new Head_strs(view_provenance_values);
    
  }
  
  public String evaluate_args_with_provenance2(Vector<Head_strs> values, Tuple tuple)
  {
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    String view_provenance_values = new String();
    
    int count = 0;
    for(int i = 0; i<q_why_column_ids.size(); i++)
    {
      if(i >= 1)
        view_provenance_values += ",";
      
      Subgoal subgoal = subgoals.get(v_why_column_ids.get(i));
      String md5_value = new String();
      for(int j = 0; j<subgoal.args.size(); j++)
      {
        if(j >= 1)
          md5_value += init.separator;
        md5_value += (clean_boolean_type((Argument) subgoal.args.get(j), values.get(q_why_column_ids.get(i)).head_vals.get(j)));
//        view_provenance_values.add(values.get(q_why_column_ids.get(i)).head_vals.get(j));
      }
      view_provenance_values += "'" + MD5.getMD5(md5_value) + "'";
    }

    return "(" + view_provenance_values + ")";
  }
  
  public ArrayList<String> evaluate_args_with_provenance2(Vector<Head_strs> values, Tuple tuple, Vector<Integer> q_why_column_ids, Vector<Integer> v_why_column_ids)
  {
    ArrayList<String> query_provenance_values = new ArrayList<String>();
    for(int i = 0; i<q_why_column_ids.size(); i++)
    {
      Subgoal subgoal = subgoals.get(v_why_column_ids.get(i));
      String md5_value = new String();
      for(int j = 0; j<subgoal.args.size(); j++)
      {
        if(j >= 1)
          md5_value += init.separator;
        md5_value += (clean_boolean_type((Argument) subgoal.args.get(j), values.get(q_why_column_ids.get(i)).head_vals.get(j)));
//        view_provenance_values.add(values.get(q_why_column_ids.get(i)).head_vals.get(j));
      }
      query_provenance_values.add(MD5.getMD5(md5_value));
    }

    return query_provenance_values;
  }
  
  static String gen_local_with_clause(Single_view view)
  {
    return Query_converter.gen_with_clause(view.subgoals, view.subgoal_name_mappings);
  }
  
  public String evaluate_view_grouping_attrs(Vector<Head_strs> values, Tuple tuple, Query query)
  {
    Vector<int[]> view_grouping_attr_ids = view_mapping_view_grouping_attr_ids_mappings.get(tuple);
//    Vector<String> grouping_attr_values = new Vector<String>();
    
//    System.out.println(values);
    
    String grouping_value_condition_string = "(";
//    String grouping_value_condition_string = new String();
    for(int i = 0; i<view_grouping_attr_ids.size(); i++)
    {
      int[] subgoal_arg_ids = view_grouping_attr_ids.get(i);
      if(i >= 1)
        grouping_value_condition_string += ",";
      
      Subgoal subgoal = (Subgoal) query.body.get(subgoal_arg_ids[0]);
      Argument arg = (Argument) subgoal.args.get(subgoal_arg_ids[1]);
//      System.out.println(i + "::" + subgoal_arg_ids[0] + "::" + subgoal_arg_ids[1] + "::"+ values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]) + "'");
      String curr_value = values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]);
      if(arg.data_type.equals("boolean"))
      {
        if(curr_value.equals("t"))
        {
          curr_value = "true";
        }
        if(curr_value.equals("f"))
        {
          curr_value = "false";
        }
      }
      grouping_value_condition_string += "'" + MD5.getMD5(curr_value) + "'";
    }
    grouping_value_condition_string += ")";
        
//    grouping_value_condition_string += ")";
    
    return grouping_value_condition_string;
  }
  
  public String evaluate_view_grouping_attrs2(Vector<Head_strs> values, Tuple tuple, Query query)
  {
    Vector<int[]> view_grouping_attr_ids = view_mapping_view_grouping_attr_ids_mappings.get(tuple);
//    Vector<String> grouping_attr_values = new Vector<String>();
    
//    System.out.println(values);
    
    String grouping_value_condition_string = "(";
//    String grouping_value_condition_string = new String();
    for(int i = 0; i<view_grouping_attr_ids.size(); i++)
    {
      int[] subgoal_arg_ids = view_grouping_attr_ids.get(i);
      if(i >= 1)
        grouping_value_condition_string += ",";
      
      Subgoal subgoal = (Subgoal) query.body.get(subgoal_arg_ids[0]);
      Argument arg = (Argument) subgoal.args.get(subgoal_arg_ids[1]);
//      System.out.println(i + "::" + subgoal_arg_ids[0] + "::" + subgoal_arg_ids[1] + "::"+ values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]) + "'");
      String curr_value = values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]);
      if(arg.data_type.equals("boolean"))
      {
        if(curr_value.equals("t"))
        {
          curr_value = "true";
        }
        if(curr_value.equals("f"))
        {
          curr_value = "false";
        }
      }
      
      if(curr_value.contains("'"))
        curr_value = curr_value.replaceAll("'", "''");
      
      grouping_value_condition_string += "'" + (curr_value) + "'";
    }
    grouping_value_condition_string += ")";
        
//    grouping_value_condition_string += ")";
    
    return grouping_value_condition_string;
  }
  
  public String evaluate_view_grouping_attrs(Vector<Head_strs> values, Tuple tuple, Query query, Vector<String> relation_seqs)
  {
    Vector<int[]> view_grouping_attr_ids = view_mapping_view_grouping_attr_ids_mappings.get(tuple);
//    Vector<String> grouping_attr_values = new Vector<String>();
    
//    System.out.println(values);
    
    String grouping_value_condition_string = "(";
//    String grouping_value_condition_string = new String();
    HashMap<String, String> relation_name_value_mappings = new HashMap<String, String>();
    for(int i = 0; i<view_grouping_attr_ids.size(); i++)
    {
      int[] subgoal_arg_ids = view_grouping_attr_ids.get(i);
      
      
      Subgoal subgoal = (Subgoal) query.body.get(subgoal_arg_ids[0]);
      Argument arg = (Argument) subgoal.args.get(subgoal_arg_ids[1]);
      String curr_value = values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]);
      if(arg.data_type.equals("boolean"))
      {
        if(curr_value.equals("t"))
        {
          curr_value = "true";
        }
        if(curr_value.equals("f"))
        {
          curr_value = "false";
        }
      }
      if(relation_name_value_mappings.get(subgoal.name) == null)
      {
        relation_name_value_mappings.put(subgoal.name, curr_value);
      }
      else
      {
        relation_name_value_mappings.put(subgoal.name, relation_name_value_mappings.get(subgoal.name) + init.separator + curr_value);
      }
      
//      System.out.println(i + "::" + subgoal_arg_ids[0] + "::" + subgoal_arg_ids[1] + "::"+ values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]) + "'");
      
      
    }
    
    
    for(int i = 0; i<relation_seqs.size(); i++)
    {
      if(i >= 1)
        grouping_value_condition_string += ",";
      String value_arr = relation_name_value_mappings.get(relation_seqs.get(i));
      grouping_value_condition_string += "'" + MD5.getMD5(value_arr) + "'";
      
    }
    
    grouping_value_condition_string += ")";
        
//    grouping_value_condition_string += ")";
    
    return grouping_value_condition_string;
  }
  
  public String evaluate_view_grouping_attrs(Vector<Head_strs> values, Tuple tuple, String grouping_value_condition_string)
  {
    Vector<int[]> view_grouping_attr_ids = view_mapping_view_grouping_attr_ids_mappings.get(tuple);
//    Vector<String> grouping_attr_values = new Vector<String>();
    
//    System.out.println(values);
    
    grouping_value_condition_string += "(";
    for(int i = 0; i<view_grouping_attr_ids.size(); i++)
    {
      int[] subgoal_arg_ids = view_grouping_attr_ids.get(i);
      if(i >= 1)
        grouping_value_condition_string += ",";
      
//      System.out.println(i + "::" + subgoal_arg_ids[0] + "::" + subgoal_arg_ids[1] + "::"+ values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]) + "'");
      
      grouping_value_condition_string += "'" + values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]) + "'";
    }
    grouping_value_condition_string += ")";
    
    return grouping_value_condition_string;
//    for(int i = 0; i<view_grouping_attr_ids.size(); i++)
//    {
//      int[] subgoal_arg_ids = view_grouping_attr_ids.get(i);
//      
//      grouping_attr_values.add(values.get(subgoal_arg_ids[0]).head_vals.get(subgoal_arg_ids[1]));
//    }
//    
//    Head_strs group_attr_values = new Head_strs(grouping_attr_values);
//    
//    return group_attr_values;
    
    
//    curr_view_grouping_attr_values.add(grouping_attr_values);
  }
  
  boolean check_condition_satisfiability_fully_evaluated(Argument arg1, Argument arg2, Operation op)
  {
    HashSet<String> data_type_set = new HashSet<String>(Arrays.asList(numeric_data_type));

    if(data_type_set.contains(arg1.data_type) || data_type_set.contains(arg2.data_type))
    {
      double value1 = Double.valueOf(arg1.value);
      
      double value2 = Double.valueOf(arg2.value);
      
      if(value1 == value2 && op.get_op_name().equals("="))
      {
        return true;
      }
      
      if(value1 > value2 && (op.get_op_name().equals(">")))
      {
        return true;
      }
      
      if(value1 >= value2 && (op.get_op_name().equals(">=")))
      {
        return true;
      }
      
      if(value1 < value2 && (op.get_op_name().equals("<")))
      {
        return true;
      }
      
      if(value1 <= value2 && (op.get_op_name().equals("<=")))
      {
        return true;
      }
      
      if(value1 != value2 && (op.get_op_name().equals("<>")))
      {
        return true;
      }
      
      return false;
    }
    else
    {
      String value1 = arg1.value;
      
      String value2 = arg2.value;
      
      if(value1.equals(value2) && op.get_op_name().equals("="))
      {
        return true;
      }
      
      if(value1.compareToIgnoreCase(value2) > 0 && (op.get_op_name().equals(">")))
      {
        return true;
      }
      
      if(value1.compareToIgnoreCase(value2) < 0 && (op.get_op_name().equals("<")))
      {
        return true;
      }
      
      if(value1.compareToIgnoreCase(value2) <= 0 && (op.get_op_name().equals("<=")))
      {
        return true;
      }
      
      if(value1.compareToIgnoreCase(value2) >= 0 && (op.get_op_name().equals(">=")))
      {
        return true;
      }
      
      if(!value1.equals(value2) && (op.get_op_name().equals("<>")))
      {
        return true;
      }
      
      return false;
    }
  }
  
  public boolean check_condition_satisfiability(Conditions condition)//, HashMap<String, ArrayList<Conditions>> undertermined_conditions, boolean first)
  {
    Argument arg1 = condition.arg1.get(0);
    
    Argument arg2 = condition.arg2.get(0);
    
    if((arg1.value != null && arg2.value != null))
    {
      if(condition.agg_function1 == null && condition.agg_function2 == null)
        return check_condition_satisfiability_fully_evaluated(condition.arg1.get(0), condition.arg2.get(0), condition.op);    
    }
    
    
    if(arg1.value != null && arg2.value == null)
    {
//      Argument new_arg = new Argument(arg1.value);
//      
//      new_arg.data_type = arg1.data_type;
//      
//      new_arg.value = arg1.value;
//      
//      Conditions new_condition = new Conditions(arg2, condition.subgoal2, condition.op, new_arg, condition.subgoal1);
//      
//      condition.swap_args();
//      
//      if(undertermined_conditions.get(condition.subgoal1) == null)
//      {
//        ArrayList<Conditions> conditions = new ArrayList<Conditions>();
//        
//        conditions.add(condition);
//        
//        undertermined_conditions.put(condition.subgoal1, conditions);
//      }
//      else
//      {
//        
//        if(first)
//          undertermined_conditions.get(condition.subgoal1).add(condition);
//      }
      
      return true;
    }
    
    
    if(arg1.value == null && arg2.value != null)
    {
      
//      Argument new_arg = new Argument(arg2.value);
//      
//      new_arg.data_type = arg2.data_type;
//      
//      new_arg.value = arg2.value;
//      
//      Conditions new_condition = new Conditions(arg1, condition.subgoal1, condition.op, new_arg, condition.subgoal2);
      
//      if(undertermined_conditions.get(condition.subgoal1) == null)
//      {
//        ArrayList<Conditions> conditions = new ArrayList<Conditions>();
//        
//        conditions.add(condition);
//        
//        undertermined_conditions.put(condition.subgoal1, conditions);
//      }
//      else
//      {
//        if(first)
//          undertermined_conditions.get(condition.subgoal1).add(condition);
//      }
      
      return true;
    }
    
    
    if(arg1.value == null && arg2.value == null)
    {
      
//      if(undertermined_conditions.get(condition.subgoal1) == null)
//      {
//        ArrayList<Conditions> conditions = new ArrayList<Conditions>();
//        
//        conditions.add(condition);
//        
//        undertermined_conditions.put(condition.subgoal1, conditions);
//      }
//      else
//      {
//        if(first)
//          undertermined_conditions.get(condition.subgoal1).add(condition);
//      }
      
      return true;
    }
    
    return true;
  }
  
  public void reset_values(Tuple tuple)
  {
    Vector<int[][]> ids = view_mapping_condition_ids_mappings.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    for(int i = 0; i<ids.size(); i++)
    {
      int [][] curr_ids = ids.get(i);
      
      int subgoal_id1 = curr_ids[0][0];
      
      int subgoal_id2 = curr_ids[1][0];
      
      if(subgoal_id1 >= 0)
      {
        Argument arg1 = (Argument) subgoals.get(v_why_column_ids.get(subgoal_id1)).args.get(curr_ids[0][1]);
        
        arg1.set_value(null);
      }
      
      if(subgoal_id2 >= 0)
      {
        Argument arg2 = (Argument) subgoals.get(v_why_column_ids.get(subgoal_id2)).args.get(curr_ids[1][1]);
        
        arg2.set_value(null);
      }
    }
    
  }
  
  void input_relation_attr(HashMap<String, HashMap<String, Vector<Integer>>> relation_attr_value_mappings, HashMap<String, Vector<String>> all_values, String relation_name, String attribute, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "select " + attribute + " from " + relation_name;
    
    pst= c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    HashMap<String, Vector<Integer>> value_id_mappings = new HashMap<String, Vector<Integer>>();
    
    int rid = 0;
    
    Vector<String> values = new Vector<String>();
    
    while(rs.next())
    {
      String value = (String) rs.getString(1);
      
      value = "'" + value + "'";
      
      if(value_id_mappings.get(value) == null)
      {
        Vector<Integer> rids = new Vector<Integer>();
        
        rids.add(rid);
        
        value_id_mappings.put(value, rids);
      }
      else
      {
        value_id_mappings.get(value).add(rid);
      }
      
      rid++;
      
      values.add(value);
    }
    
    sql = "select data_type from information_schema.columns where table_name = '" + relation_name + "' and column_name = '" + attribute + "'";
    
    pst = c.prepareStatement(sql);
    
    rs = pst.executeQuery();
    
    String data_type = new String();
    
    if(rs.next())
    {
      data_type = rs.getString(1);
    }
    
    values.add(data_type);
    
    all_values.put(relation_name + init.separator + attribute, values);
  }
  
  String gen_sql_partial_evaluated_condition(String origin_subgoal_name, Vector<Conditions> conditions)
  {
    String sql = "select exists (select * from " + origin_subgoal_name + " where ";
    
    for(int i = 0; i<conditions.size(); i++)
    {
      Conditions condition = conditions.get(i);
      
      if(i >= 1)
        sql += " and ";
      
      sql += condition.arg1.get(0).attribute_name;//.name.substring(condition.arg1.get(0).name.indexOf(init.separator) + 1, condition.arg1.get(0).name.length());
      
      sql += condition.op;
      
      sql += condition.arg2.get(0).value;
    }
    
    sql += ")";
    
    return sql;
  }
  
  public boolean check_undertermined_conditions(HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings, HashMap<String, Vector<String>> all_values, HashMap<String, Vector<Conditions>> undetermined_conditions, Connection c, PreparedStatement pst) throws SQLException
  {
    Set<String> tables = undetermined_conditions.keySet();
    
    for(Iterator iter = tables.iterator(); iter.hasNext();)
    {
      String subgoal_name = (String) iter.next();
      
      String origin_subgoal_name = subgoal_name_mappings.get(subgoal_name);
      
      Vector<Conditions> conditions = undetermined_conditions.get(origin_subgoal_name);
     
      String sql = gen_sql_partial_evaluated_condition(origin_subgoal_name, conditions);
      
      pst = c.prepareStatement(sql);
      
      ResultSet rs = pst.executeQuery();
      
      boolean b = false;
      
      if(rs.next())
        b = rs.getBoolean(1);
      
      if(!b)
        return false;
      
//      Vector<Integer> rids = null;
//      
//      for(int i = 0; i<conditions.size(); i++)
//      {
//        Conditions condition = conditions.get(i);
//        
//        Argument arg1 = condition.arg1;
//        
//        String arg_name = arg1.name.substring(arg1.name.indexOf(init.separator) + 1, arg1.name.length());
//        
//        HashMap<String, Vector<Integer>> value_rid_mappings = rel_attr_value_mappings.get(origin_subgoal_name + init.separator + arg_name); 
//        
//        if(value_rid_mappings == null)
//        {
//          input_relation_attr(rel_attr_value_mappings, all_values, origin_subgoal_name, arg_name, c, pst);
//        }
//        
//        if(condition.op.get_op_name().equals("="))
//        {
//          Vector<Integer> curr_rids = value_rid_mappings.get(condition.arg2.name);
//          
//          if(curr_rids == null)
//          {
//            return false;
//          }
//          else
//          {
//            if(rids == null)
//            {
//              rids = curr_rids;
//            }
//            else
//            {
//              rids.retainAll(curr_rids);
//              
//              if(rids.isEmpty())
//                return false;
//            }
//          }
//        }
//        
//      }
//      
//      for(int i = 0; i<conditions.size(); i++)
//      {
//        Conditions condition = conditions.get(i);
//        
//        Argument arg1 = condition.arg1;
//        
//        String arg_name = arg1.name.substring(arg1.name.indexOf(init.separator) + 1, arg1.name.length()); 
//                 
//        Vector<String> curr_values = all_values.get(origin_subgoal_name + init.separator + arg_name);
//        
//        arg1.data_type = curr_values.get(curr_values.size() - 1);
//        
//        for(int j = 0; j<rids.size(); j++)
//        {
//          int curr_rid = rids.get(j);       
//          
//          arg1.value = curr_values.get(curr_rid);
//          
//          if(!check_condition_satisfiability_fully_evaluated(condition.arg1, condition.arg2, condition.op))
//          {
//            rids.removeElementAt(j);
//            
//            j--;
//          }
//        }
//        
//        if(rids.isEmpty())
//          return false;
//      }
    }
    
    return true;
  }
  
  public void get_partial_mapping_values(Tuple tuple, ArrayList<String[][]> partial_mapping_values, int row_id)
  {
    for(int i = 0; i<tuple.cluster_patial_mapping_condition_ids.size(); i++)
    {
      HashSet<Integer> cluster_ids = tuple.cluster_patial_mapping_condition_ids.get(i);
      
      System.out.println(cluster_ids);
      
      int j = 0;
      
      HashSet<String> partial_join_mapped_attribute_names = new HashSet<String>();
      
      for(Integer condition_id : cluster_ids)
      {
        Conditions condition = conditions.get(condition_id);
        
        System.out.println(condition);
        
        Argument arg2 = condition.arg2.get(0);
        
        Argument arg1 = condition.arg1.get(0);
        
        if(arg2.value != null)
        {
          if(!partial_join_mapped_attribute_names.contains(arg2.name))
          {
            partial_join_mapped_attribute_names.add(arg2.name);
            
            partial_mapping_values.get(i)[row_id][j] = arg2.value;
            
            j++;

          }
        }
        
        if(arg1.value != null)
        {
          if(!partial_join_mapped_attribute_names.contains(arg1.name))
          {
            partial_join_mapped_attribute_names.add(arg1.name);
            
            partial_mapping_values.get(i)[row_id][j] = arg1.value;
            
            j++;

          }
          
        }
        
        
      }
      
    }
  }
  
  public boolean check_validity(Tuple tuple)//ArrayList<ArrayList<String>> partial_mapping_values, HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings, HashMap<String, ArrayList<Conditions>> undermined_table_conditions_mappings, HashMap<String, ArrayList<ArrayList<String>>> undetermined_table_arg_value_mappings, boolean first, Connection c, PreparedStatement pst) throws SQLException
  {
    
    boolean satisfiable = true; 
    
    for(int i = 0; i<conditions.size(); i++)
    {
      Conditions condition = conditions.get(i);
      
      if(!check_condition_satisfiability(condition))
      {
        satisfiable = false;
        
        break;
      }
      
    }
    

    
//    Set<String> undetermined_relations = undermined_table_conditions_mappings.keySet();
//    
//    for(String relation_name: undetermined_relations)
//    {
//      ArrayList<Conditions> curr_conditions = undermined_table_conditions_mappings.get(relation_name);
//      
//      ArrayList<ArrayList<String>> related_values = undetermined_table_arg_value_mappings.get(relation_name);     
//      
//      for(int i = 0; i<curr_conditions.size(); i++)
//      {
////        String subgoal2 = curr_conditions.get(i).subgoal2;
//
//        Argument arg2 = curr_conditions.get(i).arg2;
//                
//        if(related_values == null)
//        {
//          related_values = new ArrayList<ArrayList<String>>();
//          
//          ArrayList<String> curr_values = new ArrayList<String>();
//          
//          curr_values.add(arg2.value);
//          
//          related_values.add(curr_values);
//          
//          undetermined_table_arg_value_mappings.put(relation_name, related_values);
//          
//        }
//        else
//        {
//          if(i == 0)
//          {
//            ArrayList<String> curr_values = new ArrayList<String>();
//            
//            curr_values.add(arg2.value);
//            
//            related_values.add(curr_values);
//            
////            undetermined_table_arg_value_mappings.put(relation_name, related_values);
//          }
//          else
//          {
//            related_values.get(related_values.size() - 1).add(arg2.value);
//          }
//          
////          if(related_values.get(subgoal2) == null)
////          {
////            ArrayList<ArrayList<String>> values = new ArrayList<ArrayList<String>>();
////            
////            ArrayList<String> curr_values = new ArrayList<String>();
////            
////            curr_values.add(arg2.value);
////            
////            values.add(curr_values);
////            
////            related_values.put(subgoal2, values);
////          }
////          else
////          {
////            ArrayList<ArrayList<String>> curr_values = related_values.get(subgoal2);
////            
////            if(curr_values.get(curr_values.size() - 1).size() < arg_size)
////            {
////              curr_values.get(curr_values.size() - 1).add(arg2.value);
////            }
////            else
////            {
////              ArrayList<String> curr_value = new ArrayList<String>();
////              
////              curr_values.add(curr_value);
////            }
////          }
//        }
//      }
//      
//      
//    }
    
//    HashMap<String, Vector<String>> all_values = new HashMap<String, Vector<String>>();
    
    
//    if(!check_undertermined_conditions(rel_attr_value_mappings, all_values , undetermined_conditions, c, pst))
//      return false;
    
    
    return satisfiable;
  }
  
  public HashSet<Head_strs> get_lambda_values(Tuple tuple, ArrayList<Vector<Head_strs>> why_tokens, HashSet<Integer> valid_row_ids)
  {
    Vector<int[]> ids = view_mapping_lambda_term_ids_mappings.get(tuple);
    
//    Vector<Integer> v_subgoal_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> q_subgoal_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    HashSet<Head_strs> lambda_values = new HashSet<Head_strs>();
    
    for(Integer id: valid_row_ids)
    {
      Vector<Head_strs> curr_why_tokens = why_tokens.get(id);
      
      Vector<String> curr_l_values = new Vector<String>();
      
      for(int j = 0; j<ids.size(); j++)
      {
        int [] curr_ids = ids.get(j);
        
//        Subgoal subgoal = subgoals.get(v_subgoal_ids.get(curr_ids[0]));
//        
//        Argument arg = (Argument) subgoal.args.get(curr_ids[1]);
        
        Head_strs why_token = curr_why_tokens.get(q_subgoal_ids.get(curr_ids[0]));
        
        String value = why_token.head_vals.get(curr_ids[1]);
        
//        arg.set_value(value);
        
        curr_l_values.add(value);
        
      }
      
      Head_strs curr_lambda_values = new Head_strs(curr_l_values);
      
      lambda_values.add(curr_lambda_values);
    }
    
    return lambda_values;
    
  }
  
//  public String check_valid_view_mappings(Vector<String> Head_str, Tuple tuple)
//  {    
//    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
//    
//    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
//    
//    int num = 0;
//    
//    String why_token_seq = ".*?\\(.*?";
//    
//    for(int i = 0; i<subgoals.size(); i++)
//    {
//      if(i >= 1)
//        why_token_seq += ",";
//      
//      if(i == v_why_column_ids.get(num))
//      {          
//        why_token_seq += "(" + q_why_tokens.get(q_why_column_ids.get(num)) + ")";
//               
//        num++;
//      }
//      else
//      {
//        why_token_seq += ".*?";
//      }
//    }
//    
//    why_token_seq += "\\).*?";
//    
//    return why_token_seq;
//    
//  }
  
  public String get_q_where_why_provenance_token_seq(String q_where_token, Vector<String> q_why_tokens, Tuple tuple)
  {    
    Vector<Integer> q_why_column_ids = view_mapping_q_why_prov_token_col_ids_mapping.get(tuple);
    
    Vector<Integer> v_why_column_ids = view_mapping_view_why_prov_token_col_ids_mapping.get(tuple);
    
    int num = 0;
    
    String why_token_seq = ".*?\\(.*?(" + q_where_token + "),.*?";
    
    for(int i = 0; i<subgoals.size(); i++)
    {
      if(i >= 1)
        why_token_seq += ",";
      
      if(i == v_why_column_ids.get(num))
      {          
        why_token_seq += "(" + q_why_tokens.get(q_why_column_ids.get(num)) + ")";
               
        num++;
      }
      else
      {
        why_token_seq += ".*?";
      }
    }
    
    why_token_seq += "\\).*?";
    
    return why_token_seq;
    
  }
  
  public HashMap<String, Integer> get_citation_queries(Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "select citation2query.citation_block, citation2query.query_id from view_table join citation2view on (view_table.view = citation2view.view) join citation2query on (citation2query.citation_view_id = citation2view.citation_view_id) where view_table.name = '" + view_name + "'";

    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    HashMap<String, Integer> citation_query_ids = new HashMap<String, Integer>();
    
    while(rs.next())
    {
      String block_name = rs.getString(1);
      
     int query_id = rs.getInt(2);
     
     citation_query_ids.put(block_name, query_id);
    }
    
    return citation_query_ids;
  }
  
  @Override
  public String toString()
  {
    return this.view_name + init.separator + subgoal_name_mappings;
  }
  
  public Vector<Subgoal> getBody()
  {
    return subgoals;
  }

  public Subgoal getHead() {
    // TODO Auto-generated method stub
    return head;
  }

  public String getName() {
    // TODO Auto-generated method stub
    return view_name;
  }

  public boolean isDistVar(Argument arg) {
    // TODO Auto-generated method stub
    if (arg.isConst()) return true;
    return  head.getArgs().contains(arg);
  }

  public int getCount() {
    // TODO Auto-generated method stub
    return view_mappings.size();
  }

  public Vector getDistVars() {
    // TODO Auto-generated method stub
    return head.args;
  }
  
  public Vector getUsefulArgs(int index) {
    Vector usefulArgs = (Vector) head.getArgs().clone();
    for (int i = index + 1; i < subgoals.size(); i ++) {
      Subgoal subgoal = subgoals.elementAt(i);

      Vector subgoalArgs = subgoal.getArgs();
      for (int j = 0; j < subgoalArgs.size(); j ++) {
    Argument arg = (Argument) subgoalArgs.elementAt(j);
    if (!arg.isConst() && !usefulArgs.contains(arg)) // no constant
      usefulArgs.add(arg);
      }
    }

    return usefulArgs;
  }

}
