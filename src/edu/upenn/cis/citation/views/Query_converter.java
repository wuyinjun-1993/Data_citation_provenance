package edu.upenn.cis.citation.views;

import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.init.MD5;
import edu.upenn.cis.citation.init.init;

public class Query_converter {

  public static String datalog2sql(Query query)
  {
      
      String sql = new String();

      String sel_item = get_sel_item(query);
          
      String citation_table = get_relations_without_citation_table(query);
      
      String condition = get_condition(query);
              
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
        
    String citation_table = get_relations_without_citation_table(query);
    
    String condition = get_condition(query);
            
    sql = "select " + sel_item;
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    return sql;
  }
  
  public static String data2sql_with_why_token_columns(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query);
        
    String citation_table = get_relations_without_citation_table(query);
    
    String condition = get_condition(query);
            
    sql = "select " + sel_item;
    
    sql += " from " + citation_table;
    
    if(condition != null && !condition.isEmpty())
        sql += " where " + condition;
    
    return sql;
  }
  
  public static String data2sql_with_why_token_columns_test(Query query)
  {
    String sql = new String();

    String sel_item = get_sel_item_with_why_token_columns(query);
        
    String citation_table = get_relations_without_citation_table(query);
    
    String condition = get_condition(query);
            
    sql = "select " + sel_item;
    
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
  
  static String get_sel_item_with_why_token_columns(Query q)
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
          
          str += arg.relation_name + "." + attr_name;
          
      }
      
      for(int i = 0; i<q.body.size(); i++)
      {
        str += ",";
        
        Subgoal subgoal = (Subgoal) q.body.get(i);
        
        str += subgoal.name + ".\"c" + init.separator + MD5.get_MD5_encoding(q.subgoal_name_mapping.get(subgoal.name)) + init.provenance_column_suffix + "\""; 
      }
      
//      for(int i = 0; i<q.lambda_term.size(); i++)
//      {
//        Lambda_term l_term = q.lambda_term.get(i);
//        
//        str += "," + l_term.table_name + "." + l_term.name;
//        
//      }
      
      return str;
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
  
  public static String get_relations_without_citation_table(Query q)
  {
      String str = new String();
      
      for(int i = 0; i<q.body.size(); i++)
      {
          if(i >= 1)
              str += ",";
          
          Subgoal subgoal = (Subgoal) q.body.get(i);
          
          str += q.subgoal_name_mapping.get(subgoal.name) + " " + subgoal.name;
          
//        str += "," + q.subgoal_name_mapping.get(subgoal.name) + populate_db.suffix + " " + subgoal.name + populate_db.suffix; 
      }
      
      return str;
      
  }
  
  public static String get_condition(Query q)
  {
      String str = new String();
      
      for(int i = 0; i<q.conditions.size(); i++)
      {
          if(i >= 1)
              str += " and ";
          
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
          
          str += get_single_condition_str(q.conditions.get(i));
      }
      
      return str;
  }
  
  public static String get_single_condition_str(Conditions condition)
  {
      String str = new String();
      
      String arg1 = condition.arg1.name.replace("|", ".");
      
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
        
        String arg2 = condition.arg2.name.replace("|", ".");
             
          str += arg2;
      }
      
      return str;
  }
  
}
