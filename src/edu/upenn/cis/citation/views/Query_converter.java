package edu.upenn.cis.citation.views;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.citation_view.Head_strs;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;

public class Query_converter {

  public static String datalog2sql(Query query, boolean isPro_query)
  {
      
      String sql = new String();

      String sel_item = get_sel_item(query);
          
      String citation_table = get_relations_without_citation_table(query, isPro_query);
      
      String condition = get_condition(query, isPro_query);
              
      sql = "select " + sel_item;
      
      sql += " from " + citation_table;
      
      if(condition != null && !condition.isEmpty())
          sql += " where " + condition;
      
      return sql;
  }
  
  public static String data2sql_with_token_columns(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_token_columns(query);
        
    String citation_table = get_relations_without_citation_table(query, false);
    
    String condition = get_condition(query, false);
            
    sql = "select " + sel_item;
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    return sql;
  }
  
  public static String data2sql_with_why_token_columns(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query.head.args, true);
        
    String citation_table = get_relations_without_citation_table(query, true);
    
    String condition = get_condition(query, true);
            
    sql = "select " + sel_item;
    
    if(query.head.has_agg)
    {
      if(query.head.args.size() > 0)
        sql += ",";
      
      sql += get_agg_item_in_select_clause(query);
    }
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    
    if(query.head.has_agg && query.head.args.size() > 0)
    {
      sql += " group by " + sel_item;
      
      String having_clause = get_having_clauses(query, true);
      
      if(!having_clause.isEmpty())
      {
        sql += " having " + having_clause;
      }
      
    }
    
    return "PROVENANCE OF (" + sql + ")";
  }
  
  static String get_having_clauses(Query query, boolean isProv_query)
  {
    String string = new String();
    
    int count = 0;
    
    for(int i = 0; i<query.conditions.size(); i++)
    {
      if(query.conditions.get(i).agg_function1 != null || query.conditions.get(i).agg_function2 != null)
      {
        if(count >= 1)
          string += " and ";
        
        string += get_single_having_condition_str(query.conditions.get(i), isProv_query);
        
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
        
        string += get_single_having_condition_str(query.conditions.get(i), isProv_query);
        
        count ++;
        
      }
    }
    
    return string;
  }
  
  public static String data2sql_with_why_token_columns_test(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query.head.args, true);
        
    String citation_table = get_relations_without_citation_table(query, true);
    
    String condition = get_condition(query, true);
            
    sql = "select " + sel_item;
    
    if(query.head.has_agg)
    {
      sql += get_agg_item_in_select_clause(query);
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
    
    if(query.head.has_agg)
    {
      sql += " group by " + sel_item;
      
      String having_clause = get_having_clauses(query, true);
      
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
        
        String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
        
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
      
      String grouping_arg_string = grouping_arg.name.replace("|", ".");
      
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
  
  public static String data2sql_partial_instantiation_with_grouping_values(Single_view view, Vector<Argument> selected_args, Vector<Argument> grouping_args, Vector<String> agg_functions, Vector<Head_strs> grouping_values, Vector<Subgoal> subgoals, Vector<Integer> view_subgoal_ids)
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
        
        string += arg.name.replace(init.separator, ".") + "='" + grouping_attr_values.get(i)[j] + "'";
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
  
  static String get_grouping_attr_condition(Tuple tuple, Single_view view, Set<Head_strs> grouping_values)
  {
    String string = new String();
    
    int count = 0;
    
    for(Head_strs h: grouping_values)
    {
      if(count >= 1)
        string += " or ";
    
      string += "(";
      
      int num = 0;
      
      for(int i = 0; i<view.head.args.size(); i++)
      {
        
        Argument head_arg = (Argument) view.head.args.get(i);
        
        if(tuple.phi.apply(head_arg) != null)
        {
          if(num >= 1)
            string += " and ";
          
          string += head_arg.name.replace(init.separator, ".") + "='" + h.head_vals.get(num++) + "'";
        }
        
      }
      string += ")";
      
      count++;
    }
    return string;
  }
  
  public static String data2sql_check_having_clause(Tuple tuple, Single_view view, Set<Head_strs> grouping_values)
  {
    String sql = new String();
    
    String sel_item = get_sel_item_with_why_token_columns(view.head.args, false);
    
    String citation_table = get_relations_without_citation_table(view, false);

    String condition = get_condition(view, false);
    
    String having_clause = get_having_clauses(view, false);
    
    String grouping_attr_condition_string = get_grouping_attr_condition(tuple, view, grouping_values);
    
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
            
            String arg_name = arg.name.replace(init.separator, ".");
            
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
  
  public static String data2sql_partial_instantiation_with_provenance_values(Single_view view, Vector<Argument> selected_args, Vector<Argument> grouping_args, Vector<String> agg_functions, String[] view_subgoal_copies, Vector<Subgoal> subgoals, Vector<Integer> view_subgoal_ids)
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
  
  static String get_sel_item(Query q)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<q.head.size(); i++)
      {
          Argument arg = (Argument) q.head.args.get(i);
          
          if(i >= 1)
              str += ",";
          
          str += arg.relation_name + "." + arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
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
          
          String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          if(isProv_query)
            str += "\"" + arg.relation_name + "\".\"" + attr_name + "\"";
          else
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
  
  static String get_sel_item_with_why_token_columns(Single_view q)
  {
      String str = new String();
      
//    System.out.println("head::" + q.head);
      
      for(int i = 0; i<q.head.size(); i++)
      {
          Argument arg = (Argument) q.head.args.get(i);
          
          if(i >= 1)
              str += ",";
          
          String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
//          str += arg.relation_name + "." + attr_name;
          
          str += "\"" + arg.relation_name + "." + attr_name + "\"";
          
      }
      
      
      return str;
  }
  
  static String get_agg_item_in_select_clause(Query q)
  {
    String string = new String();
    
    for(int i = 0; i < q.head.agg_args.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      Argument arg = (Argument) q.head.agg_args.get(i);
      
      String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      String agg_function = (String) q.head.agg_function.get(i);
      
      string += agg_function + "(" + "\"" + arg.relation_name + "\".\"" + attr_name + "\"" + ")";
    }
    
    return string;
    
    
  }
  
  static String get_agg_item_in_select_clause(Vector<Argument> agg_attributes, Vector<String> agg_functions, boolean isProv_query)
  {
    String string = new String();
    
    for(int i = 0; i < agg_attributes.size(); i++)
    {
      if(i >= 1)
        string += ",";
      
      Argument arg = (Argument) agg_attributes.get(i);
      
      String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      String agg_function = (String) agg_functions.get(i);
      
      if(isProv_query)
        string += agg_function + "(" + "\"" + arg.relation_name + "." + attr_name + "\"" + ")";
      else
        string += agg_function + "(" + arg.relation_name + "." + attr_name + ")";
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
          
          String attr_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
          
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
  
  public static String get_relations_without_citation_table(Query q, boolean isProv_query)
  {
      String str = new String();
      
      for(int i = 0; i<q.body.size(); i++)
      {
          if(i >= 1)
              str += ",";
          
          Subgoal subgoal = (Subgoal) q.body.get(i);
          
          if(isProv_query)
            str += "\"" + q.subgoal_name_mapping.get(subgoal.name) + "\"" + " " + "\"" + subgoal.name + "\"";
          else
            str += q.subgoal_name_mapping.get(subgoal.name) + " " + subgoal.name;
          
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
            str += "\"" + q.subgoal_name_mappings.get(subgoal.name) + "\"" + " " + "\"" + subgoal.name + "\"";
          else
            str += q.subgoal_name_mappings.get(subgoal.name) + " " + subgoal.name;
          
//        str += "," + q.subgoal_name_mapping.get(subgoal.name) + populate_db.suffix + " " + subgoal.name + populate_db.suffix; 
      }
      
      return str;
      
  }
  
  public static String get_condition(Query q, boolean isPro_query)
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
            
            str += get_single_condition_str(q.conditions.get(i), isPro_query);
            
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
            
            str += get_single_condition_str(q.conditions.get(i), isProv_query);
            
            count ++;
            
          }
      }
      
      return str;
  }
  
  public static String get_single_condition_str(Conditions condition, boolean isProv_query)
  {
      String str = new String();
      
      String arg1 = null;
      
      if(isProv_query)
      {
        String [] rel_arg_pairs = condition.arg1.name.split("\\|");
        arg1 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
      }
      else
        arg1 = condition.arg1.name.replace("|", ".");
      
      str += arg1 + condition.op;
      
      if(condition.arg2.isConst())
      {
          
          String arg2 = condition.arg2.toString();
          
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
        
        String [] rel_arg_pairs = condition.arg2.name.split("\\|");
             
        if(isProv_query)
          str += "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
        else
          str += rel_arg_pairs[0] + "." + rel_arg_pairs[1];
      }
      
      return str;
  }
  
  public static String get_single_having_condition_str(Conditions condition, boolean isPro_query)
  {
      String str = new String();
      
      String arg1 = null;
      
      if(isPro_query)
      {
        String [] rel_arg_pairs = condition.arg1.name.split("\\|");
        
        arg1 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
      }
      else
        arg1 = condition.arg1.name.replace("|", ".");
      
      if(condition.agg_function1 != null)
        arg1 = condition.agg_function1 + "(" + arg1 + ")";
      
      str += arg1 + condition.op;
      
      if(condition.arg2.isConst())
      {
          
          String arg2 = condition.arg2.toString();
          
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
        
        String arg2 = null;
        
        if(isPro_query)
        {
          String [] rel_arg_pairs = condition.arg2.name.split("\\|");
          arg2 = "\"" + rel_arg_pairs[0] + "\".\"" + rel_arg_pairs[1] + "\"";
        }
        else
          arg2 = condition.arg2.name.replace("|", ".");
        
        if(condition.agg_function2 != null)
          arg2 = condition.agg_function2 + "(" + arg2 + ")";
             
          str += arg2;
      }
      
      return str;
  }
  
}
