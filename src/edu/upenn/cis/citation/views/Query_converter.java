package edu.upenn.cis.citation.views;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.query_view_generators.query_generator;

public class Query_converter {

  static String default_parser = ".";
  public static String[] numeric_data_type = {"smallint", "integer", "bigint", "decimal", "numeric", "real", "double precision", "serial", "bigserial"};
  public static HashSet<String> numeric_data_type_set = new HashSet<String>(Arrays.asList(numeric_data_type));
  public static String datalog2sql(Query query, boolean isPro_query)
  {
      
      String sql = new String();

      String[] sel_item = get_sel_item(query.head);
      
      String sel_agg_item = get_agg_item_in_select_clause(query.head, isPro_query);
      
      String having_clause = get_having_clauses(query.conditions, isPro_query);
      
      String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, isPro_query);
      
      String condition = get_condition(query.conditions, isPro_query);
             
      if(!sel_item[0].isEmpty())
      {
        sql = "select " + sel_item[0];
        if(!sel_agg_item.isEmpty())
          sql += "," + sel_agg_item;
      }
      else
      {
        if(!sel_agg_item.isEmpty())
          sql += "select " + sel_agg_item;
      }
      
      
      sql += " from " + citation_table;
      
      if(condition != null && !condition.isEmpty())
          sql += " where " + condition;
      
      
      if(!sel_agg_item.isEmpty())
      {
        if(!sel_item[0].isEmpty())
          sql += " group by " + sel_item[1];
      }
      
      if(!having_clause.isEmpty())
        sql += " having " + having_clause;
      
      return sql;
  }
  
  static void get_view_subgoal_primary_keys(Vector<String> provenance_attr_strings, Vector<String> indexed_attributes, Vector<Subgoal> subgoals, HashMap<String, String> relation_name_mappings, HashMap<String, Vector<Integer>> relation_primary_key_mappings, Vector<String> relation_attr_pairs, Vector<String> relation_attr_name)
  {
    for(int k = 0; k<subgoals.size(); k++)
    {
      Vector<Integer> col_names = relation_primary_key_mappings.get(relation_name_mappings.get(subgoals.get(k).name)); 
      
      for(int p = 0; p < col_names.size(); p ++)
      {
        Argument arg = (Argument) subgoals.get(k).args.get(col_names.get(p));
        
        String curr_rel_attr_pair = arg.relation_name + "." + arg.attribute_name;
        
        String curr_rel_attr_name = arg.relation_name + "_" + arg.attribute_name; 
        
        indexed_attributes.add(curr_rel_attr_name);
        
        provenance_attr_strings.add(curr_rel_attr_name);
        
        if(!relation_attr_name.contains(curr_rel_attr_name))
        {
          relation_attr_name.add(curr_rel_attr_name);
          
          relation_attr_pairs.add(curr_rel_attr_pair);
          
        }
        
      }
    }
  }
  
  static void get_view_having_clause_args(Vector<Conditions> conditions, Vector<String> relation_attr_pairs, Vector<String> relation_attr_name)
  {
    for(int i = 0; i<conditions.size(); i++)
    {
      Conditions condition = conditions.get(i);
      
      Vector<Argument> args1 = condition.arg1;
      
      Vector<Argument> args2 = condition.arg2;
      
      if(condition.agg_function1 != null || condition.agg_function2 != null)
      {
        for(int k = 0; k<args1.size(); k++)
        {
          String curr_rel_attr_pair = args1.get(k).relation_name + "." + args1.get(k).attribute_name;
          
          String curr_rel_attr_name = args1.get(k).relation_name + "_" + args1.get(k).attribute_name; 
          
          if(!relation_attr_name.contains(curr_rel_attr_name))
          {
            relation_attr_name.add(curr_rel_attr_name);
            
            relation_attr_pairs.add(curr_rel_attr_pair);
          }
        }
        
        for(int k = 0; k<args2.size(); k++)
        {
          String curr_rel_attr_pair = args2.get(k).relation_name + "." + args2.get(k).attribute_name;
          
          String curr_rel_attr_name = args2.get(k).relation_name + "_" + args2.get(k).attribute_name; 
          
          if(!relation_attr_name.contains(curr_rel_attr_name))
          {
            relation_attr_name.add(curr_rel_attr_name);
            
            relation_attr_pairs.add(curr_rel_attr_pair);
          }
        }
      }
    }
  }
  
  static String get_view_sel_items(Single_view view, Vector<String> indexed_cols, Vector<String> grouping_attrs_strings, Vector<String> provenance_attrs_strings)
  {
    Vector<String> relation_attr_pairs = new Vector<String>();
    
    Vector<String> relation_attr_name = new Vector<String>();
    
    for(int i = 0 ; i<view.head.args.size(); i++)
    {
      Argument arg = (Argument) view.head.args.get(i);
      
      relation_attr_pairs.add(arg.relation_name + "." + arg.attribute_name);
      
      relation_attr_name.add(arg.relation_name + "_" + arg.attribute_name);
      
      grouping_attrs_strings.add(arg.relation_name + "_" + arg.attribute_name);
    }
    
    get_view_subgoal_primary_keys(provenance_attrs_strings, indexed_cols, view.subgoals, view.subgoal_name_mappings, Single_view.relation_primary_key_mappings, relation_attr_pairs, relation_attr_name);
    
//    get_view_having_clause_args(view.conditions, relation_attr_pairs, relation_attr_name);
    
    String string = new String();
    
    for(int i = 0 ; i<relation_attr_pairs.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      string += relation_attr_pairs.get(i) + " as " + relation_attr_name.get(i);
    }
    
    return string;
  }
  
  
  public static String datalog2sql_view_conjunction(Single_view view, Vector<String> indexed_cols, Vector<String> grouping_attrs_strings, Vector<String> provenance_attrs_strings)
  {
    String with_clause = datalog2sql(view, false);
    
    String sql = "with temp_" + view.view_name + " as (" + with_clause + ") ";

    String sel_item = get_view_sel_items(view, indexed_cols, grouping_attrs_strings, provenance_attrs_strings);
    
//    String sel_agg_item = get_agg_item_in_select_clause(view.head, isPro_query);
    
//    String having_clause = get_having_clauses(view.conditions, isPro_query);
    
    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, false);
    
    String condition = get_condition(view.conditions, false);
           
    sql += "select " + sel_item;
    
    sql += " from " + citation_table + ", temp_" + view.view_name;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition + " and " + gen_join_condition_connecting_with_clause(view);
    else
      sql += " where " + gen_join_condition_connecting_with_clause(view);
    return sql;

  }
  
  public static String datalog2sql_view_conjunction_virtual(Single_view view, Vector<String> indexed_cols, Vector<String> grouping_attrs_strings, Vector<String> provenance_attrs_strings)
  {
    String with_clause = datalog2sql(view, false);
    
    String sql = "with temp_" + view.view_name + " as (" + with_clause + ") ";

    String sel_item = get_view_sel_items(view, indexed_cols, grouping_attrs_strings, provenance_attrs_strings);
    
//    String sel_agg_item = get_agg_item_in_select_clause(view.head, isPro_query);
    
//    String having_clause = get_having_clauses(view.conditions, isPro_query);
    
    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, false);
    
    String condition = get_condition(view.conditions, false);
           
    sql += ", " + view.view_name + " as (select " + sel_item;
    
    sql += " from " + citation_table + ", temp_" + view.view_name;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition + " and " + gen_join_condition_connecting_with_clause(view);
    else
      sql += " where " + gen_join_condition_connecting_with_clause(view);
    
    sql += ")";
    return sql;

  }
  
  static String gen_join_condition_connecting_with_clause(Single_view view)
  {
    String string = new String();
    
    for(int i = 0; i<view.head.args.size(); i++)
    {
      if(i >= 1)
        string += " and ";
      
      Argument arg = (Argument) view.head.args.get(i);
      
      string += arg.relation_name + "." + arg.attribute_name + " = " + "temp_" + view.view_name + "." + arg.relation_name + "_" + arg.attribute_name;
    }
    
    return string;
  }
  
  public static String datalog2sql(Single_view view, boolean isPro_query)
  {
      
      String sql = new String();

      String[] sel_item = get_sel_item(view.head);
      
      String sel_agg_item = get_agg_item_in_select_clause(view.head, isPro_query);
      
      String having_clause = get_having_clauses(view.conditions, isPro_query);
      
      String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, isPro_query);
      
      String condition = get_condition(view.conditions, isPro_query);
             
      if(!sel_item[0].isEmpty())
      {
        sql = "select " + sel_item[0];
        if(!sel_agg_item.isEmpty())
          sql += "," + sel_agg_item;
      }
      else
      {
        if(!sel_agg_item.isEmpty())
          sql += sel_agg_item;
      }
      
      
      sql += " from " + citation_table;
      
      if(condition != null && !condition.isEmpty())
          sql += " where " + condition;
      
      
      if(!sel_agg_item.isEmpty())
        sql += " group by " + sel_item[1];
      
      if(!having_clause.isEmpty())
        sql += " having " + having_clause;
      
      return sql;
  }
  
  
  static String get_provenance_agg(Vector<Subgoal> subgoals, HashMap<String, String> subgoal_name_mappings, HashMap<String, Vector<Integer>> relation_primary_key_mappings)
  {
    int count = 0;
    
    String sql = "array_agg(";
    
    for(int i = 0; i<subgoals.size(); i++)
    {
      Subgoal subgoal = subgoals.get(i);
      
      String origin_name = subgoal_name_mappings.get(subgoal.name);
      
      Vector<Integer> arg_ids = relation_primary_key_mappings.get(origin_name);
      
      for(int k = 0; k<arg_ids.size(); k++)
      {
        if(count >= 1)
          sql += "||'" + Query_provenance.separator_input + "'||";
        
        Argument arg = (Argument) subgoal.args.get(arg_ids.get(k));
        
        sql += "cast (" + subgoal.name + "." + arg.attribute_name + " as text)";
        
        count ++;
      }
    }
    
    sql += ")" + " as provenance";
    
    return sql;
  }
  
  public static String datalog2sql_materializations(Single_view view, HashMap<String, Vector<Integer>> relation_primary_key_mappings, boolean isPro_query)
  {
      
      String sql = new String();

      String[] sel_item = get_sel_item(view.head);
      
//      String sel_agg_item = get_agg_item_in_select_clause(view.head, isPro_query);
      
      String sel_agg_prov = get_provenance_agg(view.subgoals, view.subgoal_name_mappings, relation_primary_key_mappings);
      
      String having_clause = get_having_clauses(view.conditions, isPro_query);
      
      String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, isPro_query);
      
      String condition = get_condition(view.conditions, isPro_query);
             
      if(!sel_item[0].isEmpty())
      {
        sql = "select " + sel_item[0] + "," + sel_agg_prov;
//        if(!sel_agg_item.isEmpty())
//          sql += "," + sel_agg_item;
      }
      else
      {
        sql += sel_agg_prov;
      }
//      else
//      {
//        if(!sel_agg_item.isEmpty())
//          sql += sel_agg_item;
//      }
      
      
      sql += " from " + citation_table;
      
      if(condition != null && !condition.isEmpty())
          sql += " where " + condition;
      
      
      if(!sel_item[0].isEmpty())
        sql += " group by " + sel_item[1];
      
      if(!having_clause.isEmpty())
        sql += " having " + having_clause;
      
      return sql;
  }
  
  
  public static String data2sql_with_token_columns(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_token_columns(query);
        
    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, false);
    
    String condition = get_condition(query.conditions, false);
            
    sql = "select " + sel_item;
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    return sql;
  }
  
  static String get_prov_cols(Vector<Subgoal> subgoals, boolean isAgg)
  {
    String str = new String();
    
    if(isAgg)    
      str = "array_agg(array[";
    else
      str = "array[";
    
    for(int i = 0; i<subgoals.size(); i++)
    {
      if(i >= 1)
        str += ",";
      
      Subgoal subgoal = subgoals.get(i);
      
//      if(!isAgg)      
        str += subgoal.name + "." + init.prov_col;
//      else
//        str += "array_agg(" + subgoal.name + "." + init.prov_col + ")";
    }
    
    if(isAgg)
      str += "]) as " + init.prov_col;
    else
      str += "] as " + init.prov_col;
    
    return str;
  }
  
  public static String data2sql_with_provenance_col(Query query)
  {
    String sql = new String();

    String[] sel_item = get_sel_item_with_why_token_columns2(query.head.args, true);
        
    String prov_cols = get_prov_cols(query.body, query.head.has_agg);
    
    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, true);
    
    String condition = get_condition(query.conditions, true);
            
    sql = "select " + sel_item[0];
    
    if(query.head.size() > 0)
      sql += "," + prov_cols;
    else
      sql += prov_cols;
    
    
    if(query.head.has_agg)
    {
//      if(query.head.args.size() > 0)
//        sql += ",";
      sql += "," + get_agg_item_in_select_clause(query.head, true);
    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    
    if(query.head.has_agg && query.head.args.size() > 0)
    {
      sql += " group by " + sel_item[1];
      
      String having_clause = get_having_clauses(query.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  public static String data2sql_with_provenance_col_with_condition_string(Single_view view, String condition_string)
  {
    String sql = new String();

    String[] sel_item = get_sel_item_with_why_token_columns2(view.head.args, true);
        
    String prov_cols = get_prov_cols(view.subgoals, view.head.has_agg);
    
    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, true);
    
    String condition = get_condition(view.conditions, true);
            
    sql = "select " + sel_item[0];
    
    if(view.head.size() > 0)
      sql += "," + prov_cols;
    else
      sql += prov_cols;
    
    
//    if(view.head.has_agg)
//    {
////      if(query.head.args.size() > 0)
////        sql += ",";
//      sql += "," + get_agg_item_in_select_clause(view.head, true);
//    }
    
    sql += " from " + citation_table + condition_string;
    
    if(condition != null && !condition.isEmpty())
        sql += " and " + condition;
    
    
//    if(view.head.has_agg && view.head.args.size() > 0)
//    {
//      sql += " group by " + sel_item[1];
//      
//      String having_clause = get_having_clauses(view.conditions, true);
//      
//      if(!having_clause.isEmpty())
//      {
//        sql += " having " + having_clause;
//      }
//      
//    }
    
    return sql;
  }
  
  public static String data2sql_with_provenance_col(Single_view view)
  {
    String sql = new String();

    String[] sel_item = get_sel_item_with_why_token_columns2(view.head.args, true);
        
    String prov_cols = get_prov_cols(view.subgoals, view.head.has_agg);
    
    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, true);
    
    String condition = get_condition(view.conditions, true);
            
    sql = "select " + sel_item[0];
    
    if(view.head.size() > 0)
      sql += "," + prov_cols;
    else
      sql += prov_cols;
    
    
//    if(view.head.has_agg)
//    {
////      if(query.head.args.size() > 0)
////        sql += ",";
//      sql += "," + get_agg_item_in_select_clause(view.head, true);
//    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    
    if(view.head.has_agg && view.head.args.size() > 0)
    {
      sql += " group by " + sel_item[1];
      
      String having_clause = get_having_clauses(view.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  
  public static String data2sql_with_provenance_col_create_materialized(Single_view view)
  {
    String sql = new String();

    String[] sel_item = get_sel_item_with_why_token_columns2(view.head.args, true);
        
    String prov_cols = get_prov_cols(view.subgoals, view.head.has_agg);
    
    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, true);
    
    String condition = get_condition(view.conditions, true);
            
    sql = "select " + sel_item[0];
    
    if(view.head.size() > 0)
      sql += "," + prov_cols;
    else
      sql += prov_cols;
    
    
//    if(view.head.has_agg)
//    {
////      if(query.head.args.size() > 0)
////        sql += ",";
//      sql += "," + get_agg_item_in_select_clause(view.head, true);
//    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    
    if(view.head.has_agg && view.head.args.size() > 0)
    {
      sql += " group by " + sel_item[1];
      
      String having_clause = get_having_clauses(view.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  public static String construct_query_base_relations(StringBuilder sb, Set<String> query_prov_sets, Subgoal subgoal, String relation_name)
  {
    sb.append("select *");
    
//    for(int i = 0; i < subgoal.args.size(); i++)
//    {
//      if(i > 0)
//        sb.append(",");
//      
//      Argument arg = (Argument) subgoal.args.get(i);
//      
//      sb.append(arg.attribute_name);
//    }
    
    sb.append(" from ");
    
    sb.append(relation_name);
    
//    sb.append(" where ");
//    
//    sb.append(init.prov_col);
//    
//    sb.append(" in (");
//    
//    int count = 0;
//    
//    for(String query_prov: query_prov_sets)
//    {
//      if(count > 0)
//        sb.append(",");
//      
//      sb.append("('" + query_prov + "')");
//      
//      count++;
//    }
//    
//    sb.append(")");
    
    String sql = sb.toString();
    
    return sql;
  }
  
  public static String data2sql_with_provenance_col_materialized(Single_view view)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(view.view_name, view.head.args);
        
//    String prov_cols = get_prov_cols(view.subgoals, view.head.has_agg);
//    
//    String citation_table = get_relations_without_citation_table(view.subgoals, view.subgoal_name_mappings, true);
//    
//    String condition = get_condition(view.conditions, true);
            
    sql = "select " + sel_item;
    
    if(view.head.size() > 0)
      sql += "," + view.view_name + "." + init.prov_col;
    else
      sql += view.view_name + "." + init.prov_col;
    
    
//    if(view.head.has_agg)
//    {
////      if(query.head.args.size() > 0)
////        sql += ",";
//      sql += "," + get_agg_item_in_select_clause(view.head, true);
//    }
    
    sql += " from " + view.view_name;
    
//    if(condition != null && !condition.isEmpty())
//        sql += " where " + condition;
//    
//    
//    if(view.head.has_agg && view.head.args.size() > 0)
//    {
//      sql += " group by " + sel_item[1];
//      
//      String having_clause = get_having_clauses(view.conditions, true);
//      
//      if(!having_clause.isEmpty())
//      {
//        sql += " having " + having_clause;
//      }
//      
//    }
    
    return sql;
  }
  
  
  public static String data2sql_with_why_token_columns(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query.head.args, true);
        
    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, true);
    
    String condition = get_condition(query.conditions, true);
            
    sql = "select " + sel_item;
    
    if(query.head.has_agg)
    {
      if(query.head.args.size() > 0)
        sql += ",";
      
      sql += get_agg_item_in_select_clause(query.head, true);
    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    
    if(query.head.has_agg && query.head.args.size() > 0)
    {
      sql += " group by " + sel_item;
      
      String having_clause = get_having_clauses(query.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return "PROVENANCE OF (" + sql + ")";
  }
  
  public static String get_having_clauses(Vector<Conditions> conditions, boolean isProv_query)
  {
    String string = new String();
    
    int count = 0;
    
    for(int i = 0; i<conditions.size(); i++)
    {
      if(conditions.get(i).agg_function1 != null || conditions.get(i).agg_function2 != null)
      {
        if(count >= 1)
          string += " and ";
        
        string += get_single_having_condition_str(conditions.get(i), default_parser, isProv_query);
        
        count ++;
        
      }
    }
    
    return string;
  }
  
  static String get_having_clauses(Single_view query, boolean isProv_query)
  {
    String string = new String();
    
    int count = 0;
    
    for(int i = 0; i<query.conditions.size(); i++)
    {
      if(query.conditions.get(i).agg_function1 != null || query.conditions.get(i).agg_function2 != null)
      {
        if(count >= 1)
          string += " and ";
        
        string += get_single_having_condition_str(query.conditions.get(i), default_parser, isProv_query);
        
        count ++;
        
      }
    }
    
    return string;
  }
  
  public static String data2sql_with_provenance_col_test(Query query)
  {
    String sql = new String();

    String[] sel_item = get_sel_item_with_why_token_columns2(query.head.args, true);
    
    String prov_cols = get_prov_cols(query.body, query.head.has_agg);
    
    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, true);
    
    String condition = get_condition(query.conditions, true);
            
    sql = "select " + sel_item[0];
    
    if(query.head.size() > 0)
      sql += "," + prov_cols;
    else
      sql += prov_cols;
    
    
    if(query.head.has_agg)
    {
//      if(query.head.args.size() > 0)
//        sql += ",";
      sql += "," + get_agg_item_in_select_clause(query.head, true);
    }
    
//    String sel_item = get_sel_item_with_why_token_columns(query.head.args, true);
//        
//    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, true);
//    
//    String condition = get_condition(query.conditions, true);
//            
//    sql = "select " + sel_item;
//    
//    if(query.head.has_agg)
//    {
//      if(query.head.args.size() > 0)
//        sql += ",";
//      sql += get_agg_item_in_select_clause(query.head, true);
//    }
//    
    sql += " from " + citation_table;
    
    String condition_str = new String();
    
    if(condition != null && !condition.isEmpty())
    {
      condition_str += " where " + condition;
    }

    if(!condition_str.isEmpty())
    {
      condition_str += " and (" + query.lambda_term.get(0).arg_name + ")";
    }
    else
    {
      condition_str += " where " + query.lambda_term.get(0).arg_name;
    }
    
    sql += condition_str;
    
    if(query.head.has_agg && query.head.args.size() > 0)
    {
      sql += " group by " + sel_item[1];
      
      String having_clause = get_having_clauses(query.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  public static String data2sql_with_why_token_columns_test(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query.head.args, true);
        
    String citation_table = get_relations_without_citation_table(query.body, query.subgoal_name_mapping, true);
    
    String condition = get_condition(query.conditions, true);
            
    sql = "select " + sel_item;
    
    if(query.head.has_agg)
    {
      if(query.head.args.size() > 0)
        sql += ",";
      sql += get_agg_item_in_select_clause(query.head, true);
    }
    
    sql += " from " + citation_table;
    
    String condition_str = new String();
    
    if(condition != null && !condition.isEmpty())
    {
      condition_str += " where " + condition;
    }

    if(!condition_str.isEmpty())
    {
      condition_str += " and (" + query.lambda_term.get(0).arg_name + ")";
    }
    else
    {
      condition_str += " where " + query.lambda_term.get(0).arg_name;
    }
    
    sql += condition_str;
    
    if(query.head.has_agg && query.head.args.size() > 0)
    {
      sql += " group by " + sel_item;
      
      String having_clause = get_having_clauses(query.conditions, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return "PROVENANCE OF (" + sql + ")";
  }
  
  static String get_view_subgoal_copies_sql(String [] view_subgoal_copies)
  {
    String string = new String();
    
    for(String view_subgoal_copy : view_subgoal_copies)
    {
      string += "," + view_subgoal_copy;
    }
    
    return string;
  }
  
  static String get_view_subgoal_copy_join_conditions(Vector<Subgoal> subgoals, Vector<Integer> view_subgoal_ids)
  {
    String string = new String();
    
    int count = 0;
    
    for(int i = 0; i<view_subgoal_ids.size(); i++)
    {
      Subgoal subgoal = subgoals.get(view_subgoal_ids.get(i));
      
      for(int j = 0; j<subgoal.args.size(); j++)
      {
        if(count >= 1)
          string += " and ";
        
        Argument arg = (Argument) subgoal.args.get(j);
        
        String arg_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
        
        string += subgoal.name + "_copy." + arg.name.replace("|", "_") + " = " + subgoal.name + "." + arg_name;       
        
        count ++;
      }
      
    }
    
    return string;
    
  }
  
  static String get_grouping_value_conditions(Vector<Head_strs> grouping_values, Vector<Argument> grouping_args)
  {
    
    Vector<String> grouping_arg_strings = new Vector<String>();
    
    for(int i = 0; i<grouping_args.size(); i++)
    {
      Argument grouping_arg = grouping_args.get(i);
      
//      String grouping_arg_string = grouping_arg.name.replace("|", ".");
      String grouping_arg_string = grouping_arg.relation_name + "." + grouping_arg.attribute_name;//.name.replace("|", ".");
      
      grouping_arg_strings.add(grouping_arg_string);
    }
    
    String clause = new String();
    
    for(int i = 0; i<grouping_values.size(); i++)
    {
      Head_strs grouping_value = grouping_values.get(i);
      
      if(i >= 1)
        clause += " or ";
      
      clause += "(";
      
      for(int j = 0; j<grouping_value.head_vals.size(); j++)
      {
        if(j >= 1)
          clause += " and ";
        
        clause += grouping_arg_strings.get(j) + "=" + grouping_value.head_vals.get(j); 
      }
      
      clause += ")";
      
    }
    
    return "(" + clause + ")";
  }
  
  public static String data2sql_partial_instantiation_with_grouping_values(Single_view view, Vector<Argument> selected_args, Vector<Vector<Argument>> grouping_args, Vector<String> agg_functions, Vector<Head_strs> grouping_values, Vector<Subgoal> subgoals, Vector<Integer> view_subgoal_ids)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(selected_args, false);
        
    String citation_table = get_relations_without_citation_table(view, false);
    
    String condition = get_condition(view, false);
    
    String grouping_attr_value_clause = get_grouping_value_conditions(grouping_values, selected_args);
    
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      sql += get_agg_item_in_select_clause(grouping_args, agg_functions, false);
    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
    {
      sql += " where " + condition + " and " + grouping_attr_value_clause;
    }
    else
    {
      sql += " where " + grouping_attr_value_clause;
    }
    
    
    if(view.head.has_agg)
    {
      sql += " group by (" + sel_item + ")";
      
      String having_clause = get_having_clauses(view, false);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  static String get_grouping_attribute_values(Vector<String[]> grouping_attr_values, Vector<Argument> head_args)
  {
    String string = new String();
    
    for(int i = 0; i<grouping_attr_values.size(); i++)
    {
      if(i >= 1)
        string += " OR ";
      
      string += "(";
      
      for(int j = 0; j<head_args.size(); j++)
      {
        if(j >= 1)
          string += " and ";
        
        Argument arg = head_args.get(j);
        
//        string += arg.name.replace(init.separator, ".") + "='" + grouping_attr_values.get(i)[j] + "'";
        string += arg.relation_name + "." + arg.attribute_name + "='" + grouping_attr_values.get(i)[j] + "'";
      }
      
      string += ")";
    }
    
    return string;
  }
  
  public static String data2sql_partial_instantiation_for_having_clause_check(Single_view view, Vector<String[]> grouping_attr_values)
  {
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);
    
    String condition = get_condition(view, false);
    
    String grouping_attri_value_condition = get_grouping_attribute_values(grouping_attr_values, view.head.args);
    
    sql += "select " + sel_item;
    
    if(view.head.has_agg)
    {
      sql += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
    }
    
    sql += " from " + citation_table + " where " + grouping_attri_value_condition;
    
    if(condition != null && !condition.isEmpty())
    {
      sql += " and (" + condition + ")";
    }
    
    if(view.head.has_agg)
    {
      sql += " group by (" + sel_item + ")";
      
      String having_clause = get_having_clauses(view, false);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  static String get_grouping_attr_condition(Tuple tuple, Single_view view, Set<Head_strs> grouping_values, Vector<Argument> query_groupng_attrs)
  {
    String string = new String();
    
    int count = 0;
    
    Vector<String> view_head_arg_names = new Vector<String>();
    for(int i = 0; i<view.head.args.size(); i++)
    {
      
      Argument head_arg = (Argument) view.head.args.get(i);
      
//      Argument view_head_arg = tuple.reverse_phi.apply(head_arg);
      if(tuple.phi.apply(head_arg) != null)
      {
        
//        view_head_arg_names.add(head_arg.name.replace(init.separator, "."));
        view_head_arg_names.add(head_arg.relation_name + "." + head_arg.attribute_name);
        
//        if(num >= 1)
//          string += " and ";
//        
//        string += head_arg.name.replace(init.separator, ".") + "='" + h.head_vals.get(num++) + "'";
      }
      
    }
    
    
    for(Head_strs h: grouping_values)
    {
      if(count >= 1)
        string += " or ";
    
      string += "(";
      
      int num = 0;
      
      for(int i = 0; i<view_head_arg_names.size(); i++)
      {
        
        if(i >= 1)
          string += " and ";
        string += view_head_arg_names.get(i) + "='" + h.head_vals.get(num++) + "'";
        
//        Argument head_arg = (Argument) view.head.args.get(i);
//        
////        Argument view_head_arg = tuple.reverse_phi.apply(head_arg);
//        if(tuple.phi.apply(head_arg) != null)
//        {
//          if(num >= 1)
//            string += " and ";
//          
//          string += head_arg.name.replace(init.separator, ".") + "='" + h.head_vals.get(num++) + "'";
//        }
        
      }
      string += ")";
      System.out.println(count);
      count++;
    }
    System.out.println("done");
    return string;
  }
  
  public static String data2sql_check_having_clause(Tuple tuple, Single_view view, Set<Head_strs> grouping_values, Vector<Argument> q_grouping_attrs, String grouping_value_condition_string)
  {
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    String grouping_attr_condition_string = grouping_value_condition_string;//get_grouping_attr_condition(tuple, view, grouping_values, q_grouping_attrs);
    
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        sql += ",";
      
      sql += "count(*),"; 
      sql += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
    }
    
    sql += " from " + citation_table;
    
    if(view.head.args.size() > 0)
    {
      sql += " where (" + grouping_attr_condition_string + ")";
      
      if(condition != null && !condition.isEmpty())
      {
        sql += " and " + condition;
      }

    }
    else
    {
      if(condition != null && !condition.isEmpty())
      {
        sql += " where " + condition;
      }
    }
    
    
    
    
    if(view.head.has_agg && view.head.args.size() > 0)
      sql += " group by (" + sel_item + ")";
    
    if(!having_clause.isEmpty())
    {
      sql += " having " + having_clause;
    }
    
    return sql;
  }
  
  static HashMap<String, String> gen_with_clause_for_checking_having_clauses(Tuple tuple, Single_view view, int[] query_head_attr_view_head_ids)
  {
    HashMap<String, String> sql_clauses = new HashMap<String, String>();
    
    String sql = new String();
    
    String sel_item = get_sel_item_by_attribute(view.head.args);//get_sel_item_with_why_token_columns_encoded(view.head.args, query_head_attr_view_head_ids);
    
    String group_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String agg_attributes = new String();
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    sql_clauses.put("with_head", view.view_name);
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        agg_attributes += ",";
      
//      agg_attributes += "count(*),"; 
      agg_attributes += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
      
    }
    sql_clauses.put("with_select_agg", agg_attributes);
    sql_clauses.put("with_select", sel_item);
    sql_clauses.put("with_from", citation_table);
    if(condition != null && !condition.isEmpty())
    {
      sql_clauses.put("with_where", condition);
    }
    if(view.head.has_agg && view.head.args.size() > 0)
    {
      sql_clauses.put("with_group_by", group_item);
    }
    
    if(!having_clause.isEmpty())
    {
      sql_clauses.put("with_having", having_clause);
    }
    return sql_clauses;
  }
  
  static String get_select_item_by_subgoals(Vector<Subgoal> subgoals, Vector<Integer> subgoal_ids)
  {
    String res = new String();
    for(int i = 0; i<subgoal_ids.size(); i++)
    {
      Subgoal subgoal = subgoals.get(subgoal_ids.get(i));
      if(i >= 1)
        res += ",";
      
      String string = "md5(";
      for(int j = 0; j < subgoal.args.size(); j++)
      {
        if(j >= 1)
          string += "||'" + init.separator + "'||";
        Argument arg = (Argument) subgoal.args.get(j);
//        String arg_name = arg.name.replace(init.separator, ".");
        String arg_name = arg.relation_name + "." + arg.attribute_name;
        
        string += arg_name;
      }
      string += ")";
      res += string;
    }
    return res;
  }
  
  static String get_join_condition_with_view(Single_view view)
  {
    String string = new String();
    for(int i = 0; i<view.head.args.size(); i++)
    {
      if(i >= 1)
        string += " and ";
      
      Argument arg = (Argument) view.head.args.get(i);
      String arg_name1 = arg.relation_name + "." + arg.attribute_name;//.name.replace(init.separator, ".");
      String arg_name2 = arg.relation_name + "_" + arg.attribute_name;//arg.name.replace(init.separator, "_");
      string += arg_name1 + " = " + view.view_name + "." + arg_name2;
    }
    return string;
  }
  
  public static HashMap<String, String> data2sql_check_having_clause2(Tuple tuple, Single_view view, int[] query_head_attr_view_head_ids)
  {
    HashMap<String, String> with_clauses_sql_clauses = gen_with_clause_for_checking_having_clauses(tuple, view, query_head_attr_view_head_ids);
    
    HashMap<String, String> entire_sql_clauses = new HashMap<String, String>();
    
    entire_sql_clauses.putAll(with_clauses_sql_clauses);
    String relations = get_relations_without_citation_table(view, false) + "," + view.view_name;
    entire_sql_clauses.put("from", relations);
    String head_variables = get_select_item_by_subgoals(view.subgoals, view.view_mapping_view_why_prov_token_col_ids_mapping.get(tuple)) + "," + get_sel_item_with_why_token_columns_encoded(view.head.args, query_head_attr_view_head_ids);
    entire_sql_clauses.put("select", head_variables);
    String join_condition = get_join_condition_with_view(view);
    String condition = get_condition(view, false);
    if(condition != null && !condition.isEmpty())
    {
      if(!join_condition.isEmpty())
        condition += " and " + join_condition;
      entire_sql_clauses.put("where", condition);
    }
    else
    {
      if(!join_condition.isEmpty())
        entire_sql_clauses.put("where", join_condition);
    }
    return entire_sql_clauses;
//    sql += " from " + citation_table;
//    
//    if(view.head.args.size() > 0)
//    {
//      sql += " where (" + grouping_attr_condition_string + ")";
//      
//      if(condition != null && !condition.isEmpty())
//      {
//        sql += " and " + condition;
//      }
//
//    }
//    else
  }
  
  public static HashMap<String, String> data2sql_check_having_clause(Tuple tuple, Single_view view, int[] query_head_attr_view_head_ids)
  {
    HashMap<String, String> sql_clauses = new HashMap<String, String>();
    
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns_encoded(view.head.args, query_head_attr_view_head_ids);
    
    String group_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String agg_attributes = new String();
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        agg_attributes += ",";
      
      agg_attributes += "count(*)"; 
//      agg_attributes += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
      
    }
    sql_clauses.put("select_agg", agg_attributes);
    sql_clauses.put("select", sel_item);
    sql_clauses.put("from", citation_table);
    if(condition != null && !condition.isEmpty())
    {
      sql_clauses.put("where", condition);
    }
    if(view.head.has_agg && view.head.args.size() > 0)
    {
      sql_clauses.put("group_by", group_item);
    }
    
    if(!having_clause.isEmpty())
    {
      sql_clauses.put("having", having_clause);
    }
    return sql_clauses;
//    sql += " from " + citation_table;
//    
//    if(view.head.args.size() > 0)
//    {
//      sql += " where (" + grouping_attr_condition_string + ")";
//      
//      if(condition != null && !condition.isEmpty())
//      {
//        sql += " and " + condition;
//      }
//
//    }
//    else
  }
  
  public static HashMap<String, String> data2sql_check_having_clause1(Tuple tuple, Single_view view, int[] query_head_attr_view_head_ids)
  {
    HashMap<String, String> sql_clauses = new HashMap<String, String>();
    
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns(view.head.args, query_head_attr_view_head_ids);
    
    String group_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String agg_attributes = new String();
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        agg_attributes += ",";
      
      agg_attributes += "count(*),"; 
      agg_attributes += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
      
    }
    sql_clauses.put("select_agg", agg_attributes);
    sql_clauses.put("select", sel_item);
    sql_clauses.put("from", citation_table);
    if(condition != null && !condition.isEmpty())
    {
      sql_clauses.put("where", condition);
    }
    if(view.head.has_agg && view.head.args.size() > 0)
    {
      sql_clauses.put("group_by", group_item);
    }
    
    if(!having_clause.isEmpty())
    {
      sql_clauses.put("having", having_clause);
    }
    return sql_clauses;
//    sql += " from " + citation_table;
//    
//    if(view.head.args.size() > 0)
//    {
//      sql += " where (" + grouping_attr_condition_string + ")";
//      
//      if(condition != null && !condition.isEmpty())
//      {
//        sql += " and " + condition;
//      }
//
//    }
//    else
  }
  
  static String get_view_provenance_clause(Single_view view, Set<Head_strs> provenance_values, Vector<Integer> view_why_prov_ids)
  {
    String string = new String();
    
//    Set<Head_strs> q_head_values = provenance_values.keySet();
    
    int count = 0;
    
//    for(Head_strs q_head_value: q_head_values)
    {
      for(Head_strs provenance_value : provenance_values)
      {
        if(count >= 1)
          string += " or ";
        
        string += "(";
        
        int arg_count = 0;
        
        for(int j = 0; j<view_why_prov_ids.size(); j++)
        {
          int subgoal_id = view_why_prov_ids.get(j);
          
          Subgoal subgoal = view.subgoals.get(subgoal_id);
          
          for(int k = 0; k<subgoal.args.size(); k++)
          {
            if(arg_count >= 1)
              string += " and ";
            
            Argument arg = (Argument) subgoal.args.get(k);
            
            String arg_name = arg.relation_name + "." + arg.attribute_name;//arg.name.replace(init.separator, ".");
            
            string += arg_name + "='" + provenance_value.head_vals.get(arg_count++) + "'";
          }
          
        }
        
        count++;
        
        string += ")";
      }
    }
    
    
    
    return string;
  }
  
  public static String data2sql_compute_count_grouping_values(Single_view view, Set<Head_strs> provenance_values, Vector<Integer> view_why_prov_ids)
  {
    String sql = new String();
    
    String sel_item = new String();
    
    for(int i = 0; i<view_why_prov_ids.size(); i++)
    {
      if(i >= 1)
        sel_item += ",";
      
      sel_item += get_sel_item_with_why_token_columns(view.subgoals.get(view_why_prov_ids.get(i)).args, false);
    }
    
    String citation_table = get_relations_without_citation_table(view, false);
    
    String condition = get_condition(view, false);
    
    String grouping_attr_condition_string = get_view_provenance_clause(view, provenance_values, view_why_prov_ids);
    
    sql += "select " + sel_item + ", count(*) from " + citation_table + " where (" + grouping_attr_condition_string + ")";
    
    if(condition != null && !condition.isEmpty())
    {
      sql += " and " + condition;
    }
    
    sql += " group by (" + sel_item + ")";
    
    return sql;
  }
  
  public static HashMap<String, String> data2sql_compute_count_grouping_values(Single_view view, Vector<Integer> view_why_prov_ids)
  {
    
    HashMap<String, String> sql_clauses = new HashMap<String, String>();
    String sql = new String();
    
    
    Vector<Subgoal> subgoals = new Vector<Subgoal>();
    Vector<Argument> all_args = new Vector<Argument>();
    for(int i = 0; i<view_why_prov_ids.size(); i++)
    {
      subgoals.add(view.subgoals.get(view_why_prov_ids.get(i)));
      all_args.addAll(view.subgoals.get(view_why_prov_ids.get(i)).args);
//      if(i >= 1)
//        sel_item += ",";
//      
//      sel_item += get_sel_item_with_why_token_columns(view.subgoals.get(view_why_prov_ids.get(i)).args, false);
    }
    
    String sel_item = get_sel_item_with_why_token_columns_encoded_with_subgoals(subgoals);
    
    String group_item = get_sel_item_with_why_token_columns(all_args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);
    
    String condition = get_condition(view, false);
    
//    String grouping_attr_condition_string = get_view_provenance_clause(view, provenance_values, view_why_prov_ids);
    
    sql_clauses.put("select", sel_item);
    
    sql_clauses.put("select_agg", ", count(*)");
    
    sql_clauses.put("from", citation_table);
    
//    sql += "select " + sel_item + ", count(*) from " + citation_table + " where (" + grouping_attr_condition_string + ")";
    
    if(condition != null && !condition.isEmpty())
    {
      sql_clauses.put("where", condition);
      sql += " and " + condition;
    }
    
    sql_clauses.put("group_by", group_item);
//    sql += " group by (" + sel_item + ")";
    
    return sql_clauses;
  }
  
  public static String data2sql_check_count_grouping_values(Single_view view, Set<Head_strs> provenance_values, Vector<Integer> view_why_prov_ids)
  {
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    String grouping_attr_condition_string = get_view_provenance_clause(view, provenance_values, view_why_prov_ids);
    
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        sql += ",";
      
      sql += "count(*),";
      
      sql += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
    }
    
    sql += " from " + citation_table + " where (" + grouping_attr_condition_string + ")";
    
    if(condition != null && !condition.isEmpty())
    {
      sql += " and " + condition;
    }
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        sql += " group by " + sel_item;
    }
    
    if(!having_clause.isEmpty())
    {
      sql += " having " + having_clause;
    }
    
    return sql;
  }
  
  public static HashMap<String, String> data2sql_check_count_grouping_values(Single_view view)
  {
    HashMap<String, String> sql_clauses = new HashMap<String, String>();
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns_encoded(view.head.args);
    
    String group_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
//    String grouping_attr_condition_string = get_view_provenance_clause(view, provenance_values, view_why_prov_ids);
    String agg_attributes = new String();
    sql_clauses.put("select", sel_item);
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
        agg_attributes += ",";
      
      agg_attributes += "count(*)";
      
//      agg_attributes += get_agg_item_in_select_clause(view.head.agg_args, view.head.agg_function, false);
    }
    
    sql_clauses.put("select_agg", agg_attributes);
    sql_clauses.put("from", citation_table);
//    sql += " from " + citation_table + " where (" + grouping_attr_condition_string + ")";
    
    if(condition != null && !condition.isEmpty())
    {
      sql_clauses.put("where", condition);
      sql += " and " + condition;
    }
    
    if(view.head.has_agg)
    {
      if(view.head.args.size() > 0)
      {
        sql_clauses.put("group_by", group_item);
      }
//        sql += " group by " + sel_item;
    }
    
    if(!having_clause.isEmpty())
    {
      sql_clauses.put("having", having_clause);
//      sql += " having " + having_clause;
    }
    
    return sql_clauses;
  }
  
  public static String data2sql_partial_instantiation_with_provenance_values(Single_view view, Vector<Argument> selected_args, Vector<Vector<Argument>> grouping_args, Vector<String> agg_functions, String[] view_subgoal_copies, Vector<Subgoal> subgoals, Vector<Integer> view_subgoal_ids)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(selected_args, false);
        
    String citation_table = get_relations_without_citation_table(view, false);
    
    String view_subgoal_copy_sql = get_view_subgoal_copies_sql(view_subgoal_copies);
    
    String condition = get_condition(view, false);
    
    String join_condition = get_view_subgoal_copy_join_conditions(subgoals, view_subgoal_ids);
            
    sql = "select " + sel_item;
    
    if(view.head.has_agg)
    {
      sql += get_agg_item_in_select_clause(grouping_args, agg_functions, false);
    }
    
    sql += " from " + citation_table + view_subgoal_copy_sql;
    
    if(condition != null && !condition.isEmpty())
    {
      sql += " where " + condition + " and " + join_condition;
    }
    else
    {
      sql += " where " + join_condition;
    }
    
    
    if(view.head.has_agg)
    {
      sql += " group by (" + sel_item + ")";
      
      String having_clause = get_having_clauses(view, false);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return sql;
  }
  
  static String[] get_sel_item(Subgoal head)
  {
    String[] strings = new String[2];
    
      String str = new String();
      
//    System.out.println("head::" + q.head);
      String alias_str = new String();
      
      for(int i = 0; i<head.size(); i++)
      {
          Argument arg = (Argument) head.args.get(i);
          
          if(i >= 1)
          {
            str += ",";
            alias_str += ",";
          }
          
          str += arg.relation_name + "." + arg.attribute_name + " as " + arg.relation_name + "_" + arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          alias_str += arg.relation_name + "_" + arg.attribute_name;
          
      }
      
      strings[0] = str;
      
      strings[1] = alias_str;
      return strings;
  }
  
  static String[] get_sel_item_with_why_token_columns2(Vector<Argument> args, boolean isProv_query)
  {
      String[] str = new String[2];
      
      str[0] = new String();
      
      str[1] = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
          {
            str[0] += ",";
            str[1] += ",";
          }
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          if(isProv_query)
          {
            str[0] += "\"" + arg.relation_name + "\".\"" + attr_name + "\"" + " as " + arg.relation_name + "_" + attr_name;
            str[1] += "\"" + arg.relation_name + "\".\"" + attr_name + "\"";
          }
          else
          {
            str[0] += arg.relation_name + "." + attr_name  + " as " + arg.relation_name + "_" + attr_name;
            str[1] += arg.relation_name + "." + attr_name;
          }
          
      }
      
//      for(int i = 0; i<q.body.size(); i++)
//      {
//        str += ",";
//        
//        Subgoal subgoal = (Subgoal) q.body.get(i);
//        
//        str += subgoal.name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(subgoal.name)) + init.provenance_column_suffix + "\""; 
//      }
      
//      for(int i = 0; i<q.lambda_term.size(); i++)
//      {
//        Lambda_term l_term = q.lambda_term.get(i);
//        
//        str += "," + l_term.table_name + "." + l_term.name;
//        
//      }
      
      return str;
  }
  
  static String get_sel_item_with_why_token_columns(String view_name, Vector<Argument> args)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
          {
            str += ",";
          }
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
          str += view_name + "." + arg.relation_name + "_" + attr_name;
          
      }
      
      return str;
  }
  
  
  static String get_sel_item_with_why_token_columns(Vector<Argument> args, boolean isProv_query)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
              str += ",";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          if(isProv_query)
            str += "\"" + arg.relation_name + "\".\"" + attr_name + "\"" + " as " + arg.relation_name + "_" + attr_name;
          else
            str += arg.relation_name + "." + attr_name  + " as " + arg.relation_name + "_" + attr_name;
          
      }
      
//      for(int i = 0; i<q.body.size(); i++)
//      {
//        str += ",";
//        
//        Subgoal subgoal = (Subgoal) q.body.get(i);
//        
//        str += subgoal.name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(subgoal.name)) + init.provenance_column_suffix + "\""; 
//      }
      
//      for(int i = 0; i<q.lambda_term.size(); i++)
//      {
//        Lambda_term l_term = q.lambda_term.get(i);
//        
//        str += "," + l_term.table_name + "." + l_term.name;
//        
//      }
      
      return str;
  }
  
  static String get_sel_item_with_why_token_columns(Vector<Argument> args, int[] query_head_var)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
              str += ",";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += arg.relation_name + "." + attr_name;
          
      }
      
      str += ",";
      
      for(int i = 0; i<query_head_var.length; i++)
      {
        Argument arg = (Argument) args.get(query_head_var[i]);
        
        if(i >= 1)
            str += ",";
        
        String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
        
//        str += arg.relation_name + "." + attr_name;
        
        str += arg.relation_name + "." + attr_name;
      }
      
//      for(int i = 0; i<q.body.size(); i++)
//      {
//        str += ",";
//        
//        Subgoal subgoal = (Subgoal) q.body.get(i);
//        
//        str += subgoal.name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(subgoal.name)) + init.provenance_column_suffix + "\""; 
//      }
      
//      for(int i = 0; i<q.lambda_term.size(); i++)
//      {
//        Lambda_term l_term = q.lambda_term.get(i);
//        
//        str += "," + l_term.table_name + "." + l_term.name;
//        
//      }
      
      return str;
  }
  
  static String get_sel_item_with_why_token_columns_encoded(Vector<Argument> args)
  {
      String str = "md5(";
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
              str += "||'" + init.separator + "'||";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += arg.relation_name + "." + attr_name;
          
      }
      
      str += ")";
      return str;
  }
  
  static String get_sel_item_with_why_token_columns_encoded_with_subgoals(Vector<Subgoal> subgoals)
  {
      String str = "(";
      
      for(int i = 0; i<subgoals.size(); i++)
      {
        if(i >= 1)
          str += ",";
        Subgoal subgoal = subgoals.get(i);
        String curr_value = new String();
        for(int j = 0; j<subgoal.args.size(); j++)
        {
          if(j >= 1)
            curr_value += "||'" + init.separator + "'||";
          
          Argument arg = (Argument) subgoal.args.get(j);
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          curr_value += arg.relation_name + "." + attr_name;
        }
        
        str += "''''||md5(" + curr_value + ")||''''";
      }
      
      str += ")";
      
//    System.out.println("head::" + q.head);
      
//      for(int i = 0; i<args.size(); i++)
//      {
//          Argument arg = (Argument) args.get(i);
//          
//          if(i >= 1)
//              str += "||'" + init.separator + "'||";
//          
//          String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
//          
////          str += arg.relation_name + "." + attr_name;
//          
//          str += arg.relation_name + "." + attr_name;
//          
//      }
//      
//      str += ")";
      return str;
  }
  
//  static String get_sel_item_with_why_token_columns_single_head_attr_encoded(Vector<Argument> args, int[] query_head_attr_view_head_ids)
//  {
//      String str = new String();
//      
////    System.out.println("head::" + q.head);
//      
//      for(int i = 0; i<args.size(); i++)
//      {
//          Argument arg = (Argument) args.get(i);
//          
//          if(i >= 1)
//              str += ",";
//          
//          String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
//          
////          str += arg.relation_name + "." + attr_name;
//          
//          str += "md5(" + arg.relation_name + "." + attr_name + ") as " + arg.name.replace(init.separator, "_");
//          
//      }
//      
////      str += ")";
//      
//      return str;
//  }
  
  static String get_sel_item_with_why_token_columns_encoded(Vector<Argument> args, int[] query_head_attr_view_head_ids)
  {
      String str = "md5(";
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<args.size(); i++)
      {
          Argument arg = (Argument) args.get(i);
          
          if(i >= 1)
              str += "||'" + init.separator + "'||";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += arg.relation_name + "." + attr_name;
          
      }
      
      str += "), md5(";
      
      for(int i = 0; i<query_head_attr_view_head_ids.length; i++)
      {
        Argument arg = args.get(query_head_attr_view_head_ids[i]);
        
        if(i >= 1)
          str += "||'" + init.separator + "'||";
        
        String attr_name = arg.attribute_name;//.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
        
//        str += arg.relation_name + "." + attr_name;
        
        str += arg.relation_name + "." + attr_name;
        
      }
      
      str += ")";
      
      return str;
  }
  
  static String get_sel_item_by_attribute(Vector<Argument> args)
  {
    String string = new String();
    for(int i = 0; i<args.size(); i++)
    {
      if(i >= 1)
        string += ",";
      Argument arg = args.get(i);
      String arg_name = arg.relation_name + "." + arg.attribute_name;//arg.name.replace(init.separator, ".");
//      string += arg_name + " as " + arg.name.replace(init.separator, "_");
      string += arg_name + " as " + arg.relation_name + "_" + arg.attribute_name;
    }
    return string;
  }
  
  static String get_sel_item_with_why_token_columns(Single_view q)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<q.head.size(); i++)
      {
          Argument arg = (Argument) q.head.args.get(i);
          
          if(i >= 1)
              str += ",";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += "\"" + arg.relation_name + "." + attr_name + "\"";
          
      }
      
      
      return str;
  }
  
  static String get_agg_item_in_select_clause(Subgoal head, boolean isProv_query)
  {
    String string = new String();
    
    for(int i = 0; i < head.agg_args.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      String agg_attr_string = get_agg_attr_string(head.agg_args.get(i), isProv_query);
      
      String agg_function = (String) head.agg_function.get(i);
      
      string += agg_function + "(" + agg_attr_string + ")"; 
      
//      Argument arg = (Argument) q.head.agg_args.get(i);
//      
//      String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
//      
//      String agg_function = (String) q.head.agg_function.get(i);
//      
//      if(isProv_query)
//        string += agg_function + "(" + "\"" + arg.relation_name + "\".\"" + attr_name + "\"" + ")";
//      else
//        string += agg_function + "(" + arg.relation_name + "." + attr_name + ")";
    }
    
    return string;
    
    
  }
  
  public static String get_agg_attr_string(Vector<Argument> agg_attributes, boolean isProv_query)
  {
    String string = new String();
    
    
    if(agg_attributes.size() <= 1)
    {
      Argument arg = (Argument) agg_attributes.get(0);      
      
      String attr_name = arg.attribute_name;//.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(isProv_query)
        string += "\"" + arg.relation_name + "\".\"" + attr_name + "\"";
      else
        string += arg.relation_name + "." + attr_name;
    }
    else
    {
      string += "'' ||";
      
      for(int i = 0; i<agg_attributes.size(); i++)
      {
        if(i >= 1)
          string += "||";
        
        Argument arg = (Argument) agg_attributes.get(i);
        
        String attr_name = arg.attribute_name;//.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
        
        if(isProv_query)
          string += "\"" + arg.relation_name + "\".\"" + attr_name + "\"";
        else
          string += arg.relation_name + "." + attr_name;
      }
    }
    return string;
  }
  
  static String get_agg_item_in_select_clause(Vector<Vector<Argument>> agg_attributes, Vector<String> agg_functions, boolean isProv_query)
  {
    String string = new String();
    
    for(int i = 0; i < agg_attributes.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
//      Argument arg = (Argument) agg_attributes.get(i);
//      
//      String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      String agg_attr_string = get_agg_attr_string(agg_attributes.get(i), isProv_query);
      
      String agg_function = (String) agg_functions.get(i);
      
      string += agg_function + "(" + agg_attr_string + ")"; 
      
//      if(isProv_query)
//        string += agg_function + "(" + "\"" + arg.relation_name + "." + attr_name + "\"" + ")";
//      else
//        string += agg_function + "(" + arg.relation_name + "." + attr_name + ")";
    }
    
    return string;
    
    
  }
  
  static String get_sel_item_with_token_columns(Query q)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<q.head.size(); i++)
      {
          Argument arg = (Argument) q.head.args.get(i);
          
          if(i >= 1)
              str += ",";
          
          String attr_name = arg.attribute_name;//arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += arg.relation_name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(arg.relation_name), attr_name) + init.provenance_column_suffix + "\"";
          
      }
      
      for(int i = 0; i<q.body.size(); i++)
      {
        str += ",";
        
        Subgoal subgoal = (Subgoal) q.body.get(i);
        
        str += subgoal.name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(subgoal.name)) + init.provenance_column_suffix + "\""; 
      }
      
      for(int i = 0; i<q.lambda_term.size(); i++)
      {
        Lambda_term l_term = q.lambda_term.get(i);
        
        str += "," + l_term.table_name + "." + l_term.arg_name;
        
      }
      
      return str;
  }
  
  public static String get_relations_without_citation_table(Vector<Subgoal> body, HashMap<String, String> subgoal_name_mapping, boolean isProv_query)
  {
      String str = new String();
      
      for(int i = 0; i<body.size(); i++)
      {
          if(i >= 1)
              str += ",";
          
          Subgoal subgoal = (Subgoal) body.get(i);
          
          if(isProv_query)
            str += "\"" + subgoal_name_mapping.get(subgoal.name) + "\"" + " " + "\"" + subgoal.name + "\"";
          else
            str += subgoal_name_mapping.get(subgoal.name) + " " + subgoal.name;
          
//        str += "," + q.subgoal_name_mapping.get(subgoal.name) + populate_db.suffix + " " + subgoal.name + populate_db.suffix; 
      }
      
      return str;
      
  }
  
  public static String get_relations_without_citation_table(Single_view q, boolean isProv_query)
  {
      String str = new String();
      
      for(int i = 0; i<q.subgoals.size(); i++)
      {
          if(i >= 1)
              str += ",";
          
          Subgoal subgoal = (Subgoal) q.subgoals.get(i);
          
          if(isProv_query)
            str += "\"" + "_tmp_" + q.subgoal_name_mappings.get(subgoal.name) + "\"" + " " + "\"" + subgoal.name + "\"";
          else
            str += "_tmp_" + q.subgoal_name_mappings.get(subgoal.name) + " " + subgoal.name;
          
//        str += "," + q.subgoal_name_mapping.get(subgoal.name) + populate_db.suffix + " " + subgoal.name + populate_db.suffix; 
      }
      
      return str;
      
  }
  
  static String gen_with_clause(Vector<Subgoal> subgoals, HashMap<String, String> subgoal_mappings)
  {
    String string = "with ";
    HashMap<String, Vector<String>> unique_relations = new HashMap<String, Vector<String>>();
    HashMap<String, Vector<String>> types = new HashMap<String, Vector<String>>();
    for(int i = 0; i<subgoals.size(); i++)
    {
      Subgoal subgoal = subgoals.get(i);
      String origin_name = subgoal_mappings.get(subgoals.get(i).name);
      if(unique_relations.get(origin_name) == null)
      {
        Vector<String> arg_list = new Vector<String>();
        Vector<String> type_list = new Vector<String>(); 
        for(int j = 0; j<subgoal.args.size(); j++)
        {
          Argument arg = (Argument) subgoal.args.get(j);
          String arg_name = arg.attribute_name;//.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          arg_list.add(arg_name);
          type_list.add(arg.data_type);
        }
        unique_relations.put(origin_name, arg_list);
        types.put(origin_name, type_list);
      }
      
    }
    int count = 0;
    for(Entry<String, Vector<String>> entry: unique_relations.entrySet())
    {
      String relation_name = entry.getKey();
      Vector<String> arg_names = entry.getValue();
      Vector<String> type_list = types.get(relation_name);
      if(count > 0)
        string += ",";
      
      string += "_tmp_" + relation_name + " as (select ";
      for(int i = 0; i<arg_names.size(); i++)
      {
        String arg_name_string = arg_names.get(i);
        String data_type = type_list.get(i);
        if(i >= 1)
          string += ",";
        if(!numeric_data_type_set.contains(data_type))
          string += "case when "+ arg_name_string +" is null then 'null' else cast (" + arg_name_string +" as text) end";
        else
          string += "case when "+ arg_name_string +" is null then 'nan' else cast (" + arg_name_string +" as double precision) end";
      }
      string += " from " + relation_name + ")";
      
      count ++;
    }
    return string;
  }
  
  public static String get_condition(Vector<Conditions> conditions, boolean isPro_query)
  {
      String str = new String();
      
      int count = 0;
      
      for(int i = 0; i<conditions.size(); i++)
      {
          
          
//        str += q.conditions.get(i).subgoal1 + "." + q.conditions.get(i).arg1 + q.conditions.get(i).op;
//        
//        if(q.conditions.get(i).subgoal2 == null || q.conditions.get(i).subgoal2.isEmpty())
//        {
//            str += q.conditions.get(i).arg2;
//        }
//        else
//        {
//            str += q.conditions.get(i).subgoal2 + "." + q.conditions.get(i).arg2;
//        }
          if(conditions.get(i).agg_function1 == null && conditions.get(i).agg_function2 == null)
          {
            if(count >= 1)
              str += " and ";
            
            str += get_single_condition_str(conditions.get(i), default_parser, isPro_query);
            
            count ++;
            
          }
      }
      
      return str;
  }
  
  public static String get_condition(Single_view q, boolean isProv_query)
  {
      String str = new String();
      
      int count = 0;
      
      for(int i = 0; i<q.conditions.size(); i++)
      {
          
          
//        str += q.conditions.get(i).subgoal1 + "." + q.conditions.get(i).arg1 + q.conditions.get(i).op;
//        
//        if(q.conditions.get(i).subgoal2 == null || q.conditions.get(i).subgoal2.isEmpty())
//        {
//            str += q.conditions.get(i).arg2;
//        }
//        else
//        {
//            str += q.conditions.get(i).subgoal2 + "." + q.conditions.get(i).arg2;
//        }
          if(q.conditions.get(i).agg_function1 == null && q.conditions.get(i).agg_function2 == null)
          {
            if(count >= 1)
              str += " and ";
            
            str += get_single_condition_str(q.conditions.get(i), default_parser, isProv_query);
            
            count ++;
            
          }
      }
      
      return str;
  }
  
  static String convert_condition_arg2string(Vector<Argument> args, String parser, boolean isProv_query)
  {
    String string = new String();
    
    for(int i = 0; i<args.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      String arg = null;
      
      if(isProv_query)
      {
//        String [] rel_arg_pairs = args.get(i).name.split("\\|");
//        arg = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
        arg = "\"" + args.get(i).relation_name + "\""+ parser + "\"" + args.get(i).attribute_name + "\"";
      }
      else
        arg = args.get(i).relation_name + parser + args.get(i).attribute_name;//.replace("|", ".");
      
      string += arg;
    }
    
    return string;
  }
  
  static String convert_condition_arg2string_as_single_arg(Vector<Argument> args, String parser, boolean isProv_query)
  {

    String string = null;
    
    if(args.size() <= 1)
    {
      string = new String();
    }
    else
    {
      string = "''||";
    }
    
    for(int i = 0; i<args.size(); i++)
    {
      if(i >= 1)
        string += "||";
      
      String arg = null;
      
      if(isProv_query)
      {
//        String [] rel_arg_pairs = args.get(i).name.split("\\|");
//        arg = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
        arg = "\"" + args.get(i).relation_name + "\"" + parser + "\"" + args.get(i).attribute_name + "\"";
      }
      else
        arg = args.get(i).relation_name + parser + args.get(i).attribute_name;//.replace("|", ".");
      
      string += arg;
    }
    
    return string;
  }
  
  public static String get_single_condition_str(Conditions condition, String parser, boolean isProv_query)
  {
      String str = new String();
      
      String arg1 = null;
      
//      if(isProv_query)
//      {
//        String [] rel_arg_pairs = condition.arg1.name.split("\\|");
//        arg1 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
//      }
//      else
//        arg1 = condition.arg1.name.replace("|", ".");
      
      arg1 = convert_condition_arg2string(condition.arg1, default_parser, isProv_query);
      
      str += arg1 + condition.op;
      
      if(condition.arg2.size() == 1 && condition.arg2.get(0).isConst())
      {
          
          String arg2 = condition.arg2.get(0).toString();
          
          if(arg2.length() > 2)
          {
              arg2 = arg2.substring(1, arg2.length() - 1).replaceAll("'", "''");
              str += "'" + arg2 + "'";
          }
          else
          {
              str += arg2;
          }
                      
          
          
          
      }
      else
      {
        
        String arg2 = convert_condition_arg2string(condition.arg2, parser, isProv_query);
        
        str += arg2;
        
//        String [] rel_arg_pairs = condition.arg2.name.split("\\|");
//             
//        if(isProv_query)
//          str += "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
//        else
//          str += rel_arg_pairs[0] + "." + rel_arg_pairs[1];
      }
      
      return str;
  }
  
  public static String get_single_having_condition_str(Conditions condition, String parser, boolean isPro_query)
  {
      String str = new String();
      
      String arg1 = convert_condition_arg2string_as_single_arg(condition.arg1, parser, isPro_query);
      
//      if(isPro_query)
//      {
//        String [] rel_arg_pairs = condition.arg1.name.split("\\|");
//        
//        arg1 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
//      }
//      else
//        arg1 = condition.arg1.name.replace("|", ".");
      
      if(condition.agg_function1 != null)
        arg1 = condition.agg_function1 + "(" + arg1 + ")";
      
      str += arg1 + condition.op;
      
      if(condition.arg2.size() == 1 && condition.arg2.get(0).isConst())
      {
          
          String arg2 = condition.arg2.get(0).toString();
          
          if(arg2.length() > 2)
          {
              arg2 = arg2.substring(1, arg2.length() - 1).replaceAll("'", "''");
              str += "'" + arg2 + "'";
          }
          else
          {
              str += arg2;
          }
                      
          
          
          
      }
      else
      {
        
        String arg2 = convert_condition_arg2string_as_single_arg(condition.arg2, parser, isPro_query);
        
//        if(isPro_query)
//        {
//          String [] rel_arg_pairs = condition.arg2.name.split("\\|");
//          arg2 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
//        }
//        else
//          arg2 = condition.arg2.name.replace("|", ".");
        
        if(condition.agg_function2 != null)
          arg2 = condition.agg_function2 + "(" + arg2 + ")";
             
          str += arg2;
      }
      
      return str;
  }
  
}
