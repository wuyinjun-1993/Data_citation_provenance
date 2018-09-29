package edu.upenn.cis.citation.query_view_generators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.Operation.op_equal;
import edu.upenn.cis.citation.Operation.op_greater;
import edu.upenn.cis.citation.Operation.op_less_equal;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.views.Query_converter;

public class view_generator {
  
  static HashMap<String, Vector<String>> query_table_names = new HashMap<String, Vector<String>>();
  
  static HashMap<String, Vector<Argument>> query_head_names = new HashMap<String, Vector<Argument>>();
  
  static HashMap<String, Vector<Conditions>> query_conditions = new HashMap<String, Vector<Conditions>>();

  
  public static String [] citatable_tables = {"gpcr","object", "ligand", "family", "introduction"};
  static double head_var_rate = 0.8;
  
  static double global_predicate_rate = 1;
  static double local_predicates_rate = 0.7;
  static Vector<String> available_data_type_vec = new Vector<String>();
  static String [] available_data_type = {"integer", "text", "boolean", "character varying", "double precision", "real", "character varying", "bigint"};

  public static String view_file_name = "views";
  
  public static String citation_query_file_name = "citation_queries";
  
  public static String view_citation_query_mapping_file_name = "view_citation_query_mappings";
  
  public static int view_instance_size = 100000;
  
  public static void main(String[] args)
  {
    
  }
  
  static Vector<Integer> find_conflict_condition(Query view, String relation, String attr_name, Vector list, boolean is_integer_type)
  {
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      for(int i = 0; i<view.conditions.size(); i++)
      {
          Conditions condition = view.conditions.get(i);
          
          if(condition.subgoal1.equals(relation) && condition.arg1.get(0).name.equals(attr_name) && condition.arg2.get(0).isConst())
          {       
              
              conditions.add(condition);
//            {
//                            
//                String value_str = condition.arg2.name;
//                
//                int value = Integer.valueOf(value_str.substring(1, value_str.length() - 1));
//                
//                if(condition.op.get_op_name().equals(">"))
//                {
//                    return list.size();
//                }
//                else
//                {
//                    if(condition.op.get_op_name().equals("<="))
//                    {
//                        return 0;
//                    }
//                }
//            }
          }
      }
      
      
      
      if(conditions.size() == 0)
          return new Vector<Integer>();
      else
      {
          if(conditions.size() == 1)
          {
              if(conditions.get(0).op.get_op_name().equals(">"))
              {
                  String value_str = conditions.get(0).arg2.get(0).name;
                  
                  int id = -1;
                  
                  if(is_integer_type)
                  {
                    int value = Integer.valueOf(value_str.substring(1, value_str.length() - 1));
                    
                    id = list.indexOf(value);
                  }
                  else
                  {
                    String value = value_str.substring(1, value_str.length() - 1);
                    
                    id = list.indexOf(value);
                  }
                                      
                  Vector<Integer> ids = new Vector<Integer>();
                  
                  if(id + 1 >= list.size())
                      return null;
                  
                  ids.add(id + 1);
                  
                  ids.add(list.size());
                  
                  return ids;
              }
              else
              {
                  if(conditions.get(0).op.get_op_name().equals("<="))
                  {
                      String value_str = conditions.get(0).arg2.get(0).name;
                      
                      int id = -1;
                      
                      if(is_integer_type)
                      {
                        int value = Integer.valueOf(value_str.substring(1, value_str.length() - 1));
                        
                        id = list.indexOf(value);
                      }
                      else
                      {
                        String value = value_str.substring(1, value_str.length() - 1);
                        
                        id = list.indexOf(value);
                      }
                                              
                      Vector<Integer> ids = new Vector<Integer>();
                      
                      if(id <= 1)
                          return null;
                      
                      ids.add(0);
                      
                      ids.add(id);
                      
                      return ids;
                  }
              }
          }
          else
          {
              Vector<Integer> ids = new Vector<Integer>();
              
              if(conditions.get(0).op.get_op_name().equals(">"))
              {
                  String value_str1 = conditions.get(0).arg2.get(0).name;
                  
                  String value_str2 = conditions.get(1).arg2.get(0).name;

                  int id1 = -1;
                  
                  int id2 = -1;
                  
                  if(is_integer_type)
                  {
                    int value1 = Integer.valueOf(value_str1.substring(1, value_str1.length() - 1));
                    
                    int value2 = Integer.valueOf(value_str2.substring(1, value_str2.length() - 1));
                    
                    id1 = list.indexOf(value1);
                    
                    id2 = list.indexOf(value2);
                  }
                  else
                  {
                    String value1 = value_str1.substring(1, value_str1.length() - 1);
                    
                    String value2 = value_str2.substring(1, value_str2.length() - 1);
                    
                    id1 = list.indexOf(value1);
                    
                    id2 = list.indexOf(value2);
                  }
                                                              
                  if(id1 + 1 >= id2)
                  {
                      return null;
                  }
                  
                      ids.add(id1 + 1);
                      
                      ids.add(id2);
                  
                  return ids;
              }
              else
              {
                  String value_str1 = conditions.get(1).arg2.get(0).name;
                  
                  String value_str2 = conditions.get(0).arg2.get(0).name;
                  
                  int id1 = -1;
                  
                  int id2 = -1;
                  
                  if(is_integer_type)
                  {
                    int value1 = Integer.valueOf(value_str1.substring(1, value_str1.length() - 1));
                    
                    int value2 = Integer.valueOf(value_str2.substring(1, value_str2.length() - 1));
                    
                    id1 = list.indexOf(value1);
                    
                    id2 = list.indexOf(value2);
                  }
                  else
                  {
                    String value1 = value_str1.substring(1, value_str1.length() - 1);
                    
                    String value2 = value_str2.substring(1, value_str2.length() - 1);
                    
                    id1 = list.indexOf(value1);
                    
                    id2 = list.indexOf(value2);
                  }
                  
//                  int value1 = Integer.valueOf(value_str1.substring(1, value_str1.length() - 1));
//                  
//                  int value2 = Integer.valueOf(value_str2.substring(1, value_str2.length() - 1));
//                  
//                  int id1 = list.indexOf(value1);
//                  
//                  int id2 = list.indexOf(value2);
                                                              
                  if(id1 + 1 >= id2)
                  {
                      return null;
                  }
                  
                      ids.add(id1 + 1);
                      
                      ids.add(id2);
                  
                  return ids;
              }
          }
      }
      
      return new Vector<Integer>();
      
  }
  
  static boolean check_duplicated_conditions(Vector<Query> views, String relation, String attr_name, String constant)
  {
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
          Query view = (Query) iter.next();
          
          for(int i = 0; i<view.conditions.size(); i++)
          {
              Conditions condition = view.conditions.get(i);
              
              if(condition.arg2.get(0).isConst())
              {
                  if(condition.subgoal1.equals(relation) && condition.arg1.get(0).name.equals(attr_name) && condition.arg2.get(0).name.equals(constant))
                      return true;
              }
          }
          
      }
      
      return false;
  }
  
  static void remove_duplicate_predicates(Query view, Vector list, String relation, boolean is_integer_type, String attr_name)
  {
      Vector<Integer> ids1 = new Vector<Integer>();
      
      Vector<Integer> ids2 = new Vector<Integer>();
      
      if(is_integer_type)
      {
        int smallest_id = -1;
        
        double smallest_value = -1;
        
        int largest_id = list.size();
        
        double largest_value = (double) list.lastElement();
        
        for(int i = 0; i<view.conditions.size(); i++)
        {
            Conditions condition = view.conditions.get(i);
            
            if(condition.subgoal1.equals(relation) && condition.arg1.get(0).name.equals(attr_name) && condition.arg2.get(0).isConst())
            {
                String value_str = condition.arg2.get(0).name;
                
                double value = Double.valueOf(value_str.substring(1, value_str.length() - 1));
                
                if(condition.op.get_op_name().equals(">"))
                {
                    if(value > smallest_value)
                    {
                        smallest_id = i;
                        
                        smallest_value = value;
                    }
                    
                    ids1.add(i);
                }
                else
                {
                    if(value < largest_value)
                    {
                        largest_id = i;
                        
                        largest_value = value;
                    }
                    
                    ids2.add(i);
                }
            }
            
            
            
        }
        
        if(ids1.size() >= 2)
        {
            ids1.removeElement(smallest_id);
            
            view.conditions.removeElementAt(ids1.get(0));
            
        }
        
        if(ids2.size() >= 2)
        {
            ids2.removeElement(largest_id);
            
            view.conditions.removeElementAt(ids2.get(0));
        }
      }
      else
      {
        int smallest_id = -1;
        
        String smallest_value = new String();
        
        int largest_id = list.size();
        
        String largest_value = (String) list.lastElement();
        
        for(int i = 0; i<view.conditions.size(); i++)
        {
            Conditions condition = view.conditions.get(i);
            
            if(condition.subgoal1.get(0).equals(relation) && condition.arg1.get(0).name.equals(attr_name) && condition.arg2.get(0).isConst())
            {
                String value_str = condition.arg2.get(0).name;
                
                String value = value_str.substring(1, value_str.length() - 1);
                
                if(condition.op.get_op_name().equals(">"))
                {
                    if(value.compareToIgnoreCase(smallest_value) > 0)
                    {
                        smallest_id = i;
                        
                        smallest_value = value;
                    }
                    
                    ids1.add(i);
                }
                else
                {
                    if(value.compareToIgnoreCase(largest_value) < 0)
                    {
                        largest_id = i;
                        
                        largest_value = value;
                    }
                    
                    ids2.add(i);
                }
            }
            
            
            
        }
        
        if(ids1.size() >= 2)
        {
            ids1.removeElement(smallest_id);
            
            view.conditions.removeElementAt(ids1.get(0));
            
        }
        
        if(ids2.size() >= 2)
        {
            ids2.removeElement(largest_id);
            
            view.conditions.removeElementAt(ids2.get(0));
        }
      }
      
      
  }
  
  static HashMap<String, Vector> gen_relation_pkey_range(HashMap<String, Boolean> is_integer_type, Query q, HashMap<String, String> subgoal_arg_mappings, Connection c, PreparedStatement pst) throws SQLException
  {
    Set<String> set = query_generator.relation_primary_key_mapping.keySet();
   
    HashMap<String, Vector> maps = new HashMap<String, Vector>();
    
    HashSet<String> subgoal_names = new HashSet<String>();
    
   for(int m = 0; m<q.body.size(); m++)
   {
     Subgoal subgoal = (Subgoal)q.body.get(m);
     
     String subgoal_name = q.subgoal_name_mapping.get(subgoal.name);
     
     if(subgoal_names.contains(subgoal_name))
     {
       continue;
     }
     
       String relation = subgoal_name;
       
       String string = new String();
       
       String type = new String();
       
       for(Conditions condition: q.conditions)
       {
         if(q.subgoal_name_mapping.get(condition.subgoal1.get(0)).equals(relation))
         {
           Argument arg = condition.arg1.get(0); 
           
           string = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
           
           type = arg.data_type;
           
           break;
         }
       }
       
       subgoal_arg_mappings.put(relation, string);
       
//       Vector<String> pkeys = query_generator.relation_primary_key_mapping.get(relation);
//       
//       
//       
//       int i = 0 ;
//       
//       for(i = 0 ; i<pkeys.size(); i++)
//       {
//         
//         String type = query_generator.relation_primary_key_type_mapping.get(relation).get(i);
//         
//         if(type.equals("integer"))
//         {
//           break;
//         }
//         
////         if(i >= 1)
////           string += ",";
////         
////         string += pkeys.get(i);
//       }
       
       
       if(!type.equals("text"))
       {
         is_integer_type.put(relation, true);
       }
       else
       {
         is_integer_type.put(relation, false);
       }
       
       
       String cond = null;
       
       for(Conditions condition: q.conditions)
       {
         if(q.subgoal_name_mapping.get(condition.subgoal1.get(0)).equals(relation) && condition.arg1.get(0).name.equals(relation + init.separator + string))
         {
           cond = string + condition.op.toString() + condition.arg2.get(0);
           
           break;
         }
       }
       
       
       
       String query = "select distinct " + string + " from " + relation;
       
       if(cond != null)
       {
         query += " where " + cond;
       }
       
       
       query += " order by " + string;
       
       pst = c.prepareStatement(query);
       
//       System.out.println(query);
       
       ResultSet rs = pst.executeQuery();
       
       if(!type.equals("text"))
       {
         Vector<Integer> ids = new Vector<Integer>();
         
         while(rs.next())
         {
             ids.add(rs.getInt(1));
         }
         
         maps.put(relation, ids);
       }
       else
       {
         Vector<String> ids = new Vector<String>();
         
         while(rs.next())
         {
             ids.add(rs.getString(1));
         }
         
         maps.put(relation, ids);
       }
   }
   
   return maps;
  }
  
  static String get_data_type(String relation, String attr, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "select data_type from information_schema.columns where table_name = '" + relation + "' and column_name = '" + attr + "'";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    if(rs.next())
    {
      return rs.getString(1);
    }
    
    return null;
  }
  
  static boolean split_view(Vector<Query> views, Query view, Vector<Query> new_views, Connection c2, PreparedStatement pst, HashMap<String, Vector> all_ranges, HashMap<String, Boolean> is_integer_type, HashMap<String, String> subgoal_arg_mappings) throws SQLException, ClassNotFoundException
  {
      
      String relation = null;
      
      String origin_name = null;
      
      Random r = new Random();
      
      if(view.conditions.isEmpty())
      {
          
          int index = r.nextInt(view.body.size());
          
          Subgoal subgoal = (Subgoal) view.body.get(index);
          
          origin_name = subgoal.name;
          
          relation = view.subgoal_name_mapping.get(subgoal.name);
          
          
      }
      else
      {
          origin_name = view.conditions.get(0).subgoal1.get(0);
          
          relation = view.subgoal_name_mapping.get(origin_name);
      }
      
//      String [] primary_key_type = get_primary_key(relation, c2, pst);
      
      String primary_key = subgoal_arg_mappings.get(relation);
      
      Vector ranges = all_ranges.get(relation);
      
//      if(ranges == null)
//      {
//          query_generator.build_relation_primary_key_mapping(c2, pst);
//          
//          ranges = gen_relation_pkey_range(null, c2, pst).get(relation);//query_generator.relation_primary_key_ranges.get(relation);
//      }
      
      Vector<Integer> conflict_ids = find_conflict_condition(view, origin_name, origin_name + init.separator + primary_key, ranges, is_integer_type.get(relation));
      
      if(conflict_ids == null)
          return false;
      
//    while(conflict_ids == null)
//    {
//        index = r.nextInt(view.body.size());
//        
//        subgoal = (Subgoal) view.body.get(index);
//        
//        relation = view.subgoal_name_mapping.get(subgoal.name);
//        
//        primary_key_type = get_primary_key(relation, c, pst);
//        
//        ranges = query_generator.relation_primary_key_ranges.get(relation);
//        
//        conflict_ids = find_conflict_condition(view, subgoal.name, primary_key_type[0], ranges);
//    }
      
      
      int const_id = 0;
      
      if(conflict_ids.isEmpty())
          const_id = r.nextInt(ranges.size() - 1);
      else
      {
          int range = conflict_ids.get(1) - conflict_ids.get(0);
          
          const_id = conflict_ids.get(0) + r.nextInt(range);
      }
      
      
      
      while(check_duplicated_conditions(views, relation, relation + init.separator + primary_key, "'" + ranges.get(const_id) + "'"))
      {
          int range = conflict_ids.get(1) - conflict_ids.get(0);
          
          const_id = conflict_ids.get(0) + r.nextInt(range);
      }
      
      Conditions condition1 = new Conditions(new Argument(origin_name + init.separator + primary_key, origin_name), origin_name, new op_greater(), new Argument("'" + ranges.get(const_id) + "'"), new String());
      
      Conditions condition2 = new Conditions(new Argument(origin_name + init.separator + primary_key, origin_name), origin_name, new op_less_equal(), new Argument("'" + ranges.get(const_id) + "'"), new String());
      
      String data_type = get_data_type(origin_name, primary_key, c2, pst);
      
      condition1.arg1.get(0).set_data_type(data_type);
      
      condition2.arg1.get(0).set_data_type(data_type);
      
      
      
      Query view1 = (Query) view.clone();
      
      
      view1.conditions.add(condition1);
      
      remove_duplicate_predicates(view1, ranges, relation, is_integer_type.get(relation), relation + init.separator + primary_key);

      
      view1.name = view.name + "_1";
              
      Query view2 = (Query) view.clone();
      
      
      view2.name = view.name + "_2";
                      
      view2.conditions.add(condition2);
      
      remove_duplicate_predicates(view2, ranges, relation, is_integer_type.get(relation), relation + init.separator + primary_key);

              
//    int v1_id = view_operation.add(view1, view1.name, c1, pst);
      
      view1.name = view.name + "_1";

      
//    view_operation.add(view1, view1.name, c2, pst, false);
      
      view1.name = view.name + "_1";
      
//    String citation_view_name1 = view1.name.replaceFirst("v", "c");
//
//    String query_name1 = view1.name.replaceFirst("v", "q");
      
//    store_single_citation_view(view1, v1_id, citation_view_name1, query_name1, c1, pst);
      
//    store_single_citation_view(view1, v1_id, citation_view_name1, query_name1, c2, pst);
      
//    int v2_id = view_operation.add(view2, view2.name, c1, pst);
      
      view2.name = view.name + "_2";
      
//    view_operation.add(view2, view2.name, c2, pst, false);
      
      view2.name = view.name + "_2";

//    String citation_view_name2 = view2.name.replaceFirst("v", "c");
//
//    String query_name2 = view2.name.replaceFirst("v", "q");
      
//    store_single_citation_view(view2, v2_id, citation_view_name2, query_name2, c1, pst);
      
//    store_single_citation_view(view2, v2_id, citation_view_name2, query_name2, c2, pst);
      
      views.add(view1);
      
      views.add(view2);
      
      new_views.add(view1);
      
      new_views.add(view2);
      
      System.out.println(view1.name + "::" + check_correct(view1.conditions, c2, pst));
      
      System.out.println(view2.name + "::" + check_correct(view2.conditions, c2, pst));
      
      return true;
  }
  
  
  static boolean check_correct(Vector<Conditions> conditions, Connection c, PreparedStatement pst) throws SQLException
  {
    if(conditions.size() <= 1)
      return true;
    
    if(conditions.size() > 2)
      return false;
    
    String op1 = conditions.get(0).op.toString();
    
    String op2 = conditions.get(1).op.toString();
    
    if(op1.equals(op2))
      return false;
    
    if(op1.equals("<="))
    {
      String query = "select (" + conditions.get(0).arg2.get(0).name + " > " + conditions.get(1).arg2.get(0).name + ")";
      
      pst = c.prepareStatement(query);
      
      ResultSet rs = pst.executeQuery();
      
      if(rs.next())
      {
        return rs.getBoolean(1);
      }
      
      return false;
    }
    else
    {
      String query = "select (" + conditions.get(1).arg2.get(0).name + " > " + conditions.get(0).arg2.get(0).name + ")";
      
      pst = c.prepareStatement(query);
      
      ResultSet rs = pst.executeQuery();
      
      if(rs.next())
      {
        return rs.getBoolean(1);
      }
      
      return false;
    }
  }
  
  static Vector get_interval(Query v, HashMap<String, Object[]> selected_id)
  {
      Subgoal subgoal = (Subgoal) v.body.get(0);
      
      String subgoal_name = subgoal.name;
      
      Object [] curr_ids = selected_id.get(subgoal_name);
      
      if(v.conditions.size() == 1)
      {
          String arg = v.conditions.get(0).arg2.get(0).name;
          
          String data_type = v.conditions.get(0).arg1.get(0).data_type;
          
          arg = arg.replace("'", "");

          if(data_type.equals("text"))
          {
            String value = arg;
            
            if(v.conditions.get(0).op.toString().equals(">"))
            {
                Vector<String> values = get_interval_upper((String[])curr_ids, value);
                
                return values;
            }
            else
            {
                Vector<String> values = get_interval_lower((String[])curr_ids, value);
                
                return values;
            }
          }
          else
          {
            double value = Double.valueOf(arg);
            
            if(v.conditions.get(0).op.toString().equals(">"))
            {
                Vector<Integer> values = get_interval_upper((Integer[])curr_ids, value);
                
                return values;
            }
            else
            {
                Vector<Integer> values = get_interval_lower((Integer[])curr_ids, value);
                
                return values;
            }
          }
          
      }
      else
      {
          
        String data_type = v.conditions.get(0).arg1.get(0).data_type;
        
        String arg1 = v.conditions.get(0).arg2.get(0).name.replace("'", "");

        String arg2 = v.conditions.get(1).arg2.get(0).name.replace("'", "");
        
        if(data_type.equals("text"))
        {
          String value1 = arg1;
          
          String value2 = arg2;
          
          Vector<String> values1 = null;
          
          Vector<String> values2 = null;
          
          
          // > value1 && <= value2
          if(v.conditions.get(0).op.toString().equals(">"))
          {
              values1 = get_interval_upper((String[])curr_ids, value1);
              
              values2 = get_interval_lower((String[])curr_ids, value2);
          }
          else
          {
              values1 = get_interval_lower((String[])curr_ids, value1);
              
              values2 = get_interval_upper((String[])curr_ids, value2);
          }
          
          values1.retainAll(values2);
          
          return values1;
        }
        else
        {
          double value1 = Double.valueOf(arg1);
          
          double value2 = Double.valueOf(arg2);
          
          Vector<Integer> values1 = null;
          
          Vector<Integer> values2 = null;
          
          
          // > value1 && <= value2
          if(v.conditions.get(0).op.toString().equals(">"))
          {
              values1 = get_interval_upper((Integer[])curr_ids, value1);
              
              values2 = get_interval_lower((Integer[])curr_ids, value2);
          }
          else
          {
              values1 = get_interval_lower((Integer[])curr_ids, value1);
              
              values2 = get_interval_upper((Integer[])curr_ids, value2);
          }
          
          values1.retainAll(values2);
          
          return values1;
        }
        
      }
  }
  
  static Vector<Integer> get_interval_lower(Integer [] curr_ids, double value)
  {
      Vector<Integer> values = new Vector<Integer>();
      
      
      for(int i = 0; i<curr_ids.length; i++)
      {
          int id = curr_ids[i];
          
          if(id < value)
          {
              values.add(id);
          }
      }
      
      return values;
  }
  
  
  static Vector<Integer> get_interval_upper(Integer [] curr_ids, double value)
  {
      Vector<Integer> values = new Vector<Integer>();
      
      
      for(int i = 0; i<curr_ids.length; i++)
      {
          int id = curr_ids[i];
          
          if(id > value)
          {
              values.add(id);
          }
      }
      
      return values;
  }
  
  static Vector<String> get_interval_upper(String [] curr_ids, String value)
  {
      Vector<String> values = new Vector<String>();
      
      
      for(int i = 0; i<curr_ids.length; i++)
      {
          String id = curr_ids[i];
          
          if(id.compareToIgnoreCase(value) > 0)
          {
              values.add(id);
          }
      }
      
      return values;
  }
  
  static Vector<String> get_interval_lower(String [] curr_ids, String value)
  {
      Vector<String> values = new Vector<String>();
      
      
      for(int i = 0; i<curr_ids.length; i++)
      {
          String id = curr_ids[i];
          
          if(id.compareToIgnoreCase(value) < 0)
          {
              values.add(id);
          }
      }
      
      return values;
  }
  
  public static void inputquery_conditions(HashMap<String, Object[]> selected_ids, HashMap<String, Integer> indexes)
  {
      
      String file1 = "query_selected_values.txt";
      
      String file2 = "query_selected_values_index_gap.txt";
      
      try (BufferedReader br = new BufferedReader(new FileReader(file1))) {
          String line;            
          
          int num = 0;
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String relation = line.split("::")[0];
              
              String ids = line.split("::")[1];
              
              String [] all_ids = ids.split(",");
              
              
              try{
                Integer [] all_id_integers = new Integer[all_ids.length];
                
                for(int i = 0; i<all_id_integers.length; i++)
                {
                    all_id_integers[i] = Integer.valueOf(all_ids[i]);
                }
                
                selected_ids.put(relation, all_id_integers);
              }
              catch(Exception e)
              {
                String [] all_id_integers = new String[all_ids.length];
                
                for(int i = 0; i<all_id_integers.length; i++)
                {
                    all_id_integers[i] = all_ids[i];
                }
              }
              
          }
          
          
          
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      try (BufferedReader br = new BufferedReader(new FileReader(file2))) {
          String line;            
          
          int num = 0;
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String relation = line.split("::")[0];
              
              String ids = line.split("::")[1];
              
              int id_integer = Integer.valueOf(ids);
              
              indexes.put(relation, id_integer);
          }
          
          
          
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
  }
  
  static boolean update_pkey_ranges(HashMap<String, Vector> pkey_ranges, Query view, Connection c, PreparedStatement pst) throws SQLException
  {
    if(view.conditions.size() < 1)
      return true;
    
    Subgoal subgoal = (Subgoal) view.body.get(0);
    
    String type = view.conditions.get(0).arg1.get(0).data_type;
    
    String relation = view.subgoal_name_mapping.get(view.conditions.get(0).subgoal1.get(0));

    Vector curr_range = pkey_ranges.get(relation);
    
    String string = view.conditions.get(0).arg1.get(0).name.replace(init.separator, ".");
    
    String query = "select distinct " + string + " from " + relation;
    
    String cond = new String();
    
    for(int i = 0; i < view.conditions.size(); i++)
    {
      if(i >= 1)
        cond += " and ";
      
      
      Conditions condition = view.conditions.get(i);
      
        cond += string + condition.op.toString() + condition.arg2.get(0);
        
        
    }
    
    
    
    if(cond != null)
    {
      query += " where " + cond;
    }
    
    
    query += " order by " + string;
    
    pst = c.prepareStatement(query);
    
    System.out.println(query);
    
    ResultSet rs = pst.executeQuery();
    
    if(!type.equals("text"))
    {
      Vector<Integer> ids = new Vector<Integer>();
      
      while(rs.next())
      {
          ids.add(rs.getInt(1));
      }
      
      
      ids.retainAll(curr_range);
      
      if(ids.size() <= 1)
        return false;
      
      pkey_ranges.put(relation, ids);
    }
    else
    {
      Vector<String> ids = new Vector<String>();
      
      while(rs.next())
      {
          ids.add(rs.getString(1));
      }
      
      ids.retainAll(curr_range);
      
      if(ids.size() <= 1)
        return false;
      
      pkey_ranges.put(relation, ids);
    }
    
    return true;
  }
  
  public static Vector<Query> gen_one_additional_predicates(Vector<Query> views, Query query, String view_file_name, String citation_query_file_name, String view_citation_query_mapping_file_name, Connection c1, PreparedStatement pst) throws SQLException, ClassNotFoundException
  {
//    Vector<Integer> sizes = generator_random_numbers(1, sizeofquery);               
      Random r = new Random();
      
      HashMap<String, Object[]> selected_id = new HashMap<String, Object[]>();
      
      HashMap<String, Integer> indexes = new HashMap<String, Integer>();
      
      inputquery_conditions(selected_id, indexes);
      
      int id = 0;
      
      Query view = null;
      
      Vector<Query> useful_views = new Vector<Query>();
      
//      for(Iterator iter = views.iterator(); iter.hasNext();)
//      {
//          Query v = (Query) iter.next();
//          
//          if(v.body.size() == 1)
//          {
////            Subgoal subgoal = (Subgoal) v.body.get(0);
////            
////            String subgoal_name = subgoal.name;
////            
////            int [] curr_ids = selected_id.get(subgoal_name);
////            
////            int curr_index = indexes.get(subgoal_name);
////            
////            if(curr_index < curr_ids.length)
//              
//              
//              if(v.conditions.size() >= 1)
//              {
////                  Vector good_ids = get_interval(v, selected_id);
////                  
////                  if(good_ids.size() == 1)
////                      continue;
//              }
//              
//                  useful_views.add(v);
//          }
//      }
      
      useful_views.addAll(views);
      
      HashMap<String, Boolean> is_integer_type = new HashMap<String, Boolean>();
      
      query_generator.build_relation_primary_key_mapping(c1, pst);
      
      HashMap<String, String> subgoal_arg_mappings = new HashMap<String, String>();
      
      HashMap<String, Vector> ranges = gen_relation_pkey_range(is_integer_type, query, subgoal_arg_mappings, c1, pst);//query_generator.relation_primary_key_ranges.get(relation);
      
      Vector<Query> new_views = new Vector<Query>();
      
      while(!useful_views.isEmpty())
      {
          int index_for_change = r.nextInt(useful_views.size());
          
//        for(Iterator iter = useful_views.iterator(); iter.hasNext();)
//        {
//            if(id == index_for_change)
//            {
//                view = (Query) iter.next();
//                
//                
//                
//                break;
//            }
//            
//            id++;
//        }
          
          view = useful_views.get(index_for_change);
          
          for(int i = 0; i<useful_views.size(); i++)
          {
            if(useful_views.get(i).name.equals("v9_2"))
            {
              view = useful_views.get(i);
            }
          }
//          
//        if(view.conditions.size() >= 1)
//        {
//            Vector<Integer> good_ids = get_interval(view, selected_id);
//            
////              if(good_ids.size() == 1)
////                  continue;
////              else
//            {
//                if(split_view(good_ids, views, view, new_views, c1, c2, pst))
//                {
//                    views.remove(view);
//                    
//                    Load_views_and_citation_queries.remove_one_view_with_citation_queries(view.name, c2, pst);
//                    
//                    populate_db.delete(view.name, view.body, view.subgoal_name_mapping, c1, pst);
//                    
////                      int old_id = view_operation.delete_view_by_name(view.name, c1, pst);
////                      
////                      view_operation.delete_view_by_name(view.name, c2, pst, false);
////                      
////                      citation_view_operation.delete_citation_views_by_id(old_id, c1, pst);
////                      
////                      citation_view_operation.delete_citation_views_by_id(old_id, c2, pst);
////                      
////                      Query_operation.delete_query_by_id(old_id, c1, pst);
////                      
////                      Query_operation.delete_query_by_id(old_id, c2, pst);
//                    
//                    break;
//                }
//            }
//        }
          
          if(!update_pkey_ranges(ranges, view, c1, pst))
          {
            useful_views.remove(view);
            
            continue;
          }
          
          if(split_view(views, view, new_views, c1, pst, ranges, is_integer_type, subgoal_arg_mappings))
          {
              views.remove(view);
              
//              Load_views_and_citation_queries.remove_one_view_with_citation_queries(view.name, view_file_name, citation_query_file_name, view_citation_query_mapping_file_name, c1, pst);
              
//              populate_db.delete(view.name, view.body, view.subgoal_name_mapping, c1, pst);
              
//            int old_id = view_operation.delete_view_by_name(view.name, c1, pst);
//            
//            view_operation.delete_view_by_name(view.name, c2, pst, false);
//            
//            citation_view_operation.delete_citation_views_by_id(old_id, c1, pst);
//            
//            citation_view_operation.delete_citation_views_by_id(old_id, c2, pst);
//            
//            Query_operation.delete_query_by_id(old_id, c1, pst);
//            
//            Query_operation.delete_query_by_id(old_id, c2, pst);
              
              break;
          }
      }
      
      return new_views;
              
  }
  
  
  
  public static void initial()
  {
      Vector<String> table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2object");
      
      table_names.add("object");
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"contributor_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"object_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("object" + init.separator +"object_id", "object"), "object", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      Vector<Argument> head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("object", table_names);
      
      query_head_names.put("object", head_variables);
      
      query_conditions.put("object", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2object");
      
      table_names.add("gpcr");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"contributor_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"object_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("gpcr" + init.separator +"object_id", "gpcr"), "gpcr", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("gpcr", table_names);
      
      query_head_names.put("gpcr", head_variables);
      
      query_conditions.put("gpcr", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2family");
      
      table_names.add("family");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2family" + init.separator +"contributor_id", "contributor2family"), "contributor2family", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2family" + init.separator +"family_id", "contributor2family"), "contributor2family", new op_equal(), new Argument("family" + init.separator +"family_id", "family"), "family", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("family", table_names);
      
      query_head_names.put("family", head_variables);
      
      query_conditions.put("family", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2intro");
      
      table_names.add("introduction");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2intro" + init.separator +"contributor_id", "contributor2intro"), "contributor2intro", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2intro" + init.separator +"family_id", "contributor2intro"), "contributor2intro", new op_equal(), new Argument("introduction" + init.separator +"family_id", "introduction"), "introduction", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("introduction", table_names);
      
      query_head_names.put("introduction", head_variables);
      
      query_conditions.put("introduction", conditions);
      
      table_names = new Vector<String>();
              
      table_names.add("contributor2ligand");
      
      table_names.add("ligand");
      
      conditions = new Vector<Conditions>();
              
      conditions.add(new Conditions(new Argument("contributor2ligand" + init.separator +"ligand_id", "contributor2ligand"), "contributor2ligand", new op_equal(), new Argument("ligand" + init.separator +"ligand_id", "ligand"), "ligand", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor2ligand" + init.separator + "first_names", "contributor2ligand"));
      
      head_variables.add(new Argument("contributor2ligand" + init.separator + "surname", "contributor2ligand"));
      
      query_table_names.put("ligand", table_names);
      
      query_head_names.put("ligand", head_variables);
      
      query_conditions.put("ligand", conditions);
      
//    table_names = new Vector<String>();
//    
//    table_names.add("contributor");
//    
//    table_names.add("contributor2ligand");
//    
//    table_names.add("ligand");
//    
//    conditions = new Vector<String>();
//        
//    conditions.add("ligand_ligand_id = contributor2ligand_ligand_id");
//    
//    head_variables = new Vector<String>();
//    
//    head_variables.add("contributor2ligand_first_names");
//    
//    head_variables.add("contributor2ligand_surname");
//    
//    query_table_names.put("ligand", table_names);
//    
//    query_head_names.put("ligand", head_variables);
//    
//    query_conditions.put("ligand", conditions);
      
      
      
  }
  
  
//  public static void store_views_with_citation_queries(Vector<Query> views, String view_file)
//  {
//    Vector<String> view_strings = Load_views_and_citation_queries.views2text_strings(views);
//    Load_views_and_citation_queries.write2files(view_file, view_strings);
//    Vector<Query> citation_queries = new Vector<Query>();
//    Vector<String> view_citation_query_mappings = new Vector<String>();
//    for(int i = 0; i<views.size(); i++)
//    {
//      Query query = store_citation_queries(views.get(i), i, "q" + i);
//      citation_queries.add(query);
//      view_citation_query_mappings.add(views.get(i).name + "|" + query.name + "|" + "Contributor");
//    }
//    Vector<String> citation_query_strings = Load_views_and_citation_queries.views2text_strings(citation_queries);
//    Load_views_and_citation_queries.write2files(citation_query_file_name, citation_query_strings);
//    Load_views_and_citation_queries.write2files(view_citation_query_mapping_file_name, view_citation_query_mappings);
////    
//    
//  }
  
  public static void store_views_with_citation_queries(Vector<Query> views, String view_file, String citation_query_file, String view_citation_query_mapping_file_name)
  {
    Vector<String> view_strings = Load_views_and_citation_queries.views2text_strings(views);
    Load_views_and_citation_queries.write2files(view_file, view_strings);
//    Vector<Query> citation_queries = new Vector<Query>();
//    Vector<String> view_citation_query_mappings = new Vector<String>();
//    for(int i = 0; i<views.size(); i++)
//    {
//      String citation_query_name = views.get(i).name.replaceFirst("v", "q");
//      
//      Query query = store_citation_queries(views.get(i), citation_query_name);
//      citation_queries.add(query);
//      view_citation_query_mappings.add(views.get(i).name + "|" + query.name + "|" + "Contributor");
//    }
//    Vector<String> citation_query_strings = Load_views_and_citation_queries.views2text_strings(citation_queries);
//    Load_views_and_citation_queries.write2files(citation_query_file, citation_query_strings);
//    Load_views_and_citation_queries.write2files(view_citation_query_mapping_file_name, view_citation_query_mappings);
//  
    
  }
  
  public static void append_views_with_citation_queries(Vector<Query> views, int offset, String view_file_name, String citation_query_file, String view_citation_query_mapping_file_name)
  {
    Vector<String> view_strings = Load_views_and_citation_queries.views2text_strings(views);
    Load_views_and_citation_queries.append2files(view_file_name, view_strings);
//    Vector<Query> citation_queries = new Vector<Query>();
//    Vector<String> view_citation_query_mappings = new Vector<String>();
//    for(int i = 0; i<views.size(); i++)
//    {
//      String citation_query_name = views.get(i).name.replaceFirst("v", "q");
//      
//      Query query = store_citation_queries(views.get(i), citation_query_name);
//      citation_queries.add(query);
//      view_citation_query_mappings.add(views.get(i).name + "|" + query.name + "|" + "Contributor");
//    }
//    Vector<String> citation_query_strings = Load_views_and_citation_queries.views2text_strings(citation_queries);
//    Load_views_and_citation_queries.append2files(citation_query_file, citation_query_strings);
//    Load_views_and_citation_queries.append2files(view_citation_query_mapping_file_name, view_citation_query_mappings);
    
    
  }
  
  static Query store_citation_queries(Query view, String qname)
  {
      Vector<String> citable_table_names = new Vector<String>();
      
      citable_table_names.addAll(Arrays.asList(citatable_tables));
      
      String subgoal = null;
      
//    while(true)
      {
          Random r = new Random();
          
          int index = r.nextInt(view.body.size());
          
          
          
          subgoal = view.subgoal_name_mapping.get(((Subgoal)view.body.get(index)).name);
          
//        if(citable_table_names.contains(subgoal))
//            break;
      }
      
      
      Vector<Conditions> conditions = query_conditions.get(subgoal);
      
      Vector<Argument> head_vars = query_head_names.get(subgoal);
      
      Vector<String> subgoals = query_table_names.get(subgoal);
      
      Subgoal head = new Subgoal(qname, head_vars);
      
      Vector<Subgoal> body = new Vector<Subgoal>();
      
      HashMap<String, String> subgoal_mapping = new HashMap<String, String>();
      
      for(int i = 0; i < subgoals.size(); i++)
      {
          Vector<Argument> args = new Vector<Argument>();
          
          body.add(new Subgoal(subgoals.get(i), args));
          
          subgoal_mapping.put(subgoals.get(i), subgoals.get(i));
      }
      
      Vector<Lambda_term> l_args = new Vector<Lambda_term>();
      
      for(int i = 0; i<view.lambda_term.size(); i++)
      {
          Lambda_term l = view.lambda_term.get(i);
          
          if(l.table_name.equals(subgoal))
          {
              l_args.add(l);
              
              break;
          }
      }
              
      Query q = new Query(qname, head, body, l_args, conditions, subgoal_mapping);
      
      return q;
      
  }
  
  static Vector<Integer> generator_random_numbers(int num, int max_num)
  {
      int i = 0;
      
      Vector<Integer> random_number = new Vector<Integer>();
      
      Random r = new Random();
      
      while(i < num)
      {
          double val = r.nextGaussian() * 5;
          
          if(val <= 0 || val > max_num)
              continue;
          
          int value = (int) Math.ceil(val);
          
          if(value == 0)
              continue;
          
          random_number.add(value);
          
          i++;
      }
      return random_number;
  }
  
  static HashMap<String, String> set_up_relation_primary_keys()
  {
    HashMap<String, String> relation_primary_key_mappings = new HashMap<String, String>();
    
    relation_primary_key_mappings.put("gpcr", "object_id");
    
    relation_primary_key_mappings.put("object", "object_id");
    
    relation_primary_key_mappings.put("family", "family_id");
    
    relation_primary_key_mappings.put("introduction", "family_id");
    
    relation_primary_key_mappings.put("ligand", "ligand_id");
    
    return relation_primary_key_mappings;
    
  }
  
  public static Vector<Query> gen_views_without_predicates(HashSet<String> subgoal_names, Query query, int num_views, int sizeofquety, int offset, Connection c, PreparedStatement pst) throws SQLException
  {
    initial();
    
      Vector<Integer> sizes = generator_random_numbers(num_views, sizeofquety);
      
      Vector<String> all_citable_tables = new Vector<String>();
      
      all_citable_tables.addAll(Arrays.asList(citatable_tables));
      
//    all_citable_tables.removeAll(subgoal_names);
      
      HashMap<String, String> relation_primary_key_mappings = set_up_relation_primary_keys();
      
      Vector<Query> queries = new Vector<Query>();
      
      int num = 0;
      
      if(!sizes.contains(1))
          sizes.set(0, 1);
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      while(queries.size() < sizes.size())
      {
          
          int size = sizes.get(num);
             
          Query view = generate_view_without_predicates_partial_mappings(conditions, subgoal_names, query, num + offset, sizeofquety, all_citable_tables, relation_primary_key_mappings, size, c, pst);
                      
//      if(!queries.contains(query))
          {
              queries.add(view);
              
              System.out.println(view.lambda_term + "," + view.toString());
              
              num ++;
          }
          
          
      }
      
      return queries;
  }
  
  public static Vector<Query> gen_views(boolean has_agg, HashSet<String> subgoal_names, Query query, int num_views, int sizeofquety, int offset, Connection c, PreparedStatement pst) throws SQLException
  {
    initial();
    
      Vector<Integer> sizes = generator_random_numbers(num_views, sizeofquety);
      
      Vector<String> all_citable_tables = new Vector<String>();
      
      all_citable_tables.addAll(Arrays.asList(citatable_tables));
      
//    all_citable_tables.removeAll(subgoal_names);
      
      HashMap<String, String> relation_primary_key_mappings = set_up_relation_primary_keys();
      
      Vector<Query> queries = new Vector<Query>();
      
      int num = 0;
      
      if(!sizes.contains(1))
          sizes.set(0, 1);
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      while(queries.size() < sizes.size())
      {
          
          int size = sizes.get(num);
             
          Query view = generate_view_without_predicates_partial_mappings(conditions, subgoal_names, query, num + offset, sizeofquety, all_citable_tables, relation_primary_key_mappings, has_agg, c, pst);
                      
//      if(!queries.contains(query))
          {
              queries.add(view);
              
              System.out.println(view.lambda_term + "," + view.toString());
              
              num ++;
          }
          
          
      }
      
      return queries;
  }
  
  public static Vector<Query> gen_views_random(boolean has_agg, HashSet<String> subgoal_names, Query query, int num_views, int sizeofquety, int offset, Connection c, PreparedStatement pst) throws SQLException
  {
    initial();
    
      Vector<Integer> sizes = generator_random_numbers(num_views, sizeofquety);
      
      Vector<String> all_citable_tables = new Vector<String>();
      
      all_citable_tables.addAll(Arrays.asList(citatable_tables));
      
//    all_citable_tables.removeAll(subgoal_names);
      
      HashMap<String, String> relation_primary_key_mappings = set_up_relation_primary_keys();
      
      Vector<Query> queries = new Vector<Query>();
      
      int num = 0;
      
      if(!sizes.contains(1))
          sizes.set(0, 1);
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      while(queries.size() < sizes.size())
      {
          
          int size = sizes.get(num);
             
          Query view = generate_view_without_predicates_partial_mappings_random(conditions, subgoal_names, query, num + offset, sizeofquety, all_citable_tables, relation_primary_key_mappings, has_agg, c, pst);
                      
//      if(!queries.contains(query))
          {
              queries.add(view);
              
              System.out.println(view.lambda_term + "," + view.toString());
              
              num ++;
          }
          
          
      }
      
      return queries;
  }
  
  public static Vector<Query> update_instance_size(Vector<Query> views, Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> updated_views = new Vector<Query>();
    
    Vector<Conditions> conditions = new Vector<Conditions>();
    
    for(int i = 0; i<views.size(); i++)
    {
      Query view = views.get(i);
      
      Vector<Conditions> q_conditions = new Vector<Conditions>();
      
      if(conditions.isEmpty())
      {
        q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, view.body, view.subgoal_name_mapping, c, pst));
      }
      else
      {
        q_conditions.addAll(conditions);
      }
      
      conditions.clear();
      
      conditions.addAll(q_conditions);
      
      Query updated_view = new Query(view.name, view.head, view.body, view.lambda_term, conditions, view.subgoal_name_mapping);
      
      System.out.println(updated_view);
      
      updated_views.add(updated_view);
    }
    
    return updated_views;
  }
  
  static HashMap<String, String> get_attr_types(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
      Vector<String> attr_names = new Vector<String>();
      
      String query_attr_name = "SELECT column_name, data_type "
              + "FROM information_schema.columns "
              + "WHERE table_name = '"+ relation_name + "' "
              + "ORDER BY ordinal_position";
      pst = c.prepareStatement(query_attr_name);
      
      ResultSet rs = pst.executeQuery();
      
      HashMap<String, String> attr_type_mapping = new HashMap<String, String>();
      
      while(rs.next())
      {
          String attr_name = rs.getString(1);
          
          if(attr_name.endsWith(init.separator + "prov"))
            continue;
          
          String type = rs.getString(2);
          
          if(available_data_type_vec.size() == 0)
          {
              available_data_type_vec.addAll(Arrays.asList(available_data_type));
          }
          
          if(available_data_type_vec.contains(type))
          {
              attr_type_mapping.put(attr_name, type);
          }
      }
      
      return attr_type_mapping;
  }
  
  static String [] get_primary_key(String table_name, Connection c, PreparedStatement pst) throws SQLException
  {
      String query = "SELECT"
              + " c.column_name, c.data_type"
              + " FROM information_schema.table_constraints tc"
              + " JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)"
              + " JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name"
              + " where constraint_type = 'PRIMARY KEY' and tc.table_name = '"+ table_name  +"'";
      
      pst =c.prepareStatement(query);
      
      String [] results = new String [2];
      
      ResultSet rs = pst.executeQuery();
      
      if(rs.next())
      {
          results[0] = rs.getString(1);
          
          results[1] = rs.getString(2);
      }
      
      return results;
      
  }
  
  static Vector<Argument> gen_head_vars(String relation_name, Vector<String> attr_list, int size, Connection c, PreparedStatement pst)
  {
      
      HashSet<Integer> id_set = new HashSet<Integer>();
      
      Vector<Argument> head_vars = new Vector<Argument>();
      
      Random r = new Random();
      
      for(int i = 0; i<size; i++)
      {
          int index = 0;
          if(i == 0)
          {
              index = 0;
              
              id_set.add(index);          
          }
          else
          {
              index = r.nextInt(attr_list.size());
              
              if(index != 0)
                  id_set.add(index);          
          }
          
      }
      
      for(Iterator iter = id_set.iterator(); iter.hasNext();)
      {
          Integer index = (Integer) iter.next();
          
          Argument l = new Argument(relation_name + init.separator + attr_list.get(index), relation_name);
          
          head_vars.add(l);
          
      }
      
      return head_vars;
  }
  
  static Vector<Argument> gen_head_vars(String relation, String relation_name, Vector<String> attr_list, int size, Connection c, PreparedStatement pst)
  {
      
      HashSet<Integer> id_set = new HashSet<Integer>();
      
      Vector<Argument> head_vars = new Vector<Argument>();
      
      Random r = new Random();
      
      for(int i = 0; i<size; i++)
      {
          int index = r.nextInt(attr_list.size());
          
//        if(query_generator.parameterizable_attri.get(relation).contains(attr_list.get(index)))
          if(!id_set.contains(index))
          {
              id_set.add(index);
          }
          else
          {
              i--;
          }
//        
      }
      
      for(Iterator iter = id_set.iterator(); iter.hasNext();)
      {
          Integer index = (Integer) iter.next();
          
          Argument l = new Argument(relation_name+ init.separator + attr_list.get(index), relation_name);
          
          head_vars.add(l);
          
      }
      
      return head_vars;
  }
  
  static Vector<Argument> get_all_attributes(Query query)
  {
    Vector<Argument> args = new Vector<Argument>();
    for(int i = 0; i<query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      args.addAll(subgoal.args);
    }
    return args;
  }
  
  static Query generate_view_without_predicates_partial_mappings(Vector<Conditions> conditions, HashSet<String> subgoal_names, Query query, int id, int size, Vector<String> all_citable_tables, HashMap<String, String> relation_primary_key_mappings, boolean has_agg, Connection c, PreparedStatement pst) throws SQLException
  {
    
    Vector<Argument> all_args = get_all_attributes(query);
    all_args.removeAll(query.head.args);
    
    Vector<Subgoal> body = new Vector<Subgoal>();
    
    
    
    Vector<Argument> head_grouping_attrs = new Vector<Argument>();
    
    head_grouping_attrs.addAll(query.head.args);
    
    Vector<Vector<Argument>> head_agg_attrs = new Vector<Vector<Argument>>();

    Vector<String> head_agg_functions = new Vector<String>();
    
    Random r = new Random();

    HashMap<String, String> maps = new HashMap<String, String>();
    
    if(has_agg)
    {
      body.addAll(query.body);
      
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Argument head_grouping_arg = (Argument) query.head.agg_args.get(i).get(0);
        String rel_name = head_grouping_arg.relation_name;//.name.substring(0, head_grouping_arg.name.indexOf(init.separator));
        String origin_rel_name = query.subgoal_name_mapping.get(rel_name);
        String arg_name = head_grouping_arg.attribute_name;//.name.substring(head_grouping_arg.name.indexOf(init.separator) + 1, head_grouping_arg.name.length());
        
        if(query_generator.parameterizable_attri.get(origin_rel_name).contains(arg_name))
        {
          Boolean b = r.nextBoolean();
          
          if(b)
          {
            Vector<Argument> curr_head_arr = new Vector<Argument>();
            
            curr_head_arr.add((Argument) query.head.agg_args.get(i).get(0));
            
            head_agg_attrs.add(curr_head_arr);
            head_agg_functions.add((String) query.head.agg_function.get(i));
          }
        }
      }
      
      if(head_agg_attrs.isEmpty())
      {
        Vector<Argument> curr_head_arr = new Vector<Argument>();
        
        curr_head_arr.add((Argument) query.head.agg_args.get(0).get(0));
        
        head_agg_attrs.add(curr_head_arr);
        head_agg_functions.add((String) query.head.agg_function.get(0));
      }
    }
    else
    {
      
      HashMap<String, Subgoal> subgoal_name_mappings = new HashMap<String, Subgoal>();
      
      for(int i = 0; i<query.body.size(); i++)
      {
        Subgoal subgoal = (Subgoal) query.body.get(i);
        
        subgoal_name_mappings.put(subgoal.name, subgoal);
      }
      
      HashSet<String> names = new HashSet<String>();
      
      for(int i = 0; i < query.head.args.size(); i++)
      {
        Argument arg = (Argument) query.head.args.get(i);
        
        names.add(arg.relation_name);
      }
      
      for(String name: names)
      {
        body.add(subgoal_name_mappings.get(name));
      }
      
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Boolean b = r.nextBoolean();
        
        if(b)
        {
          Vector<Argument> args = query.head.agg_args.get(i);
          
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
      }
    }
    

    maps.putAll(query.subgoal_name_mapping);
    
    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = all_args.get(i);
      
      String arg_relation = arg.relation_name;
      String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(query_generator.parameterizable_attri.get(arg_relation).contains(arg_name) && !query_generator.relation_primary_key_mapping.get(arg_relation).contains(arg_name))
      {
        double b = r.nextDouble();
        
        if(b < head_var_rate && !head_grouping_attrs.contains(all_args.get(i)))
        {
          head_grouping_attrs.add(all_args.get(i));
          break;
        }
      }

    }
    
    Subgoal head = new Subgoal("v" + id, head_grouping_attrs, head_agg_attrs, head_agg_functions, true);
    
    Vector<Conditions> q_conditions = new Vector<Conditions>();
    
    if(has_agg)
    {
      if(conditions.isEmpty())
      {
        q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, body, maps, c, pst));
      }
      else
      {
        q_conditions.addAll(conditions);
      }
      
      conditions.clear();
      
      conditions.addAll(q_conditions);
    }
    
    return new Query("v" + id, head, body, new Vector<Lambda_term>(), q_conditions , maps);

  }
  
  static Query generate_view_without_predicates_partial_mappings(Vector<Conditions> conditions, HashSet<String> subgoal_names, Query query, int id, int size, Vector<String> all_citable_tables, HashMap<String, String> relation_primary_key_mappings, int view_size, Connection c, PreparedStatement pst) throws SQLException
  {
    
    Vector<Argument> all_args = get_all_attributes(query);
    all_args.removeAll(query.head.args);
    
    Vector<Subgoal> body = new Vector<Subgoal>();
    
    HashMap<String, String> view_subgoal_name_mappings = new HashMap<String, String>();
    
    Vector<Argument> head_grouping_attrs = new Vector<Argument>();
    
    head_grouping_attrs.addAll(query.head.args);
    
    Vector<Vector<Argument>> head_agg_attrs = new Vector<Vector<Argument>>();

    Vector<String> head_agg_functions = new Vector<String>();
    
    Random r = new Random();

      HashMap<String, Subgoal> subgoal_name_mappings = new HashMap<String, Subgoal>();
      
      for(int i = 0; i<query.body.size(); i++)
      {
        Subgoal subgoal = (Subgoal) query.body.get(i);
        
        subgoal_name_mappings.put(subgoal.name, subgoal);
      }
      
      HashSet<String> names = new HashSet<String>();
      
      for(int i = 0; i < query.head.args.size(); i++)
      {
        Argument arg = (Argument) query.head.args.get(i);
        
        names.add(arg.relation_name);
      }
      
      for(String name: names)
      {
        body.add(subgoal_name_mappings.get(name));
        
        view_subgoal_name_mappings.put(name, query.subgoal_name_mapping.get(name));
      }
      
      
      int max_size = view_size - body.size();
      
      while(max_size > 0)
      {
        
        int sid = r.nextInt(query.body.size());
        
        Subgoal subgoal = (Subgoal) query.body.get(sid);
        
        if(names.contains(subgoal.name))
        {
          continue;
        }
        else
        {
          names.add(subgoal.name);
          
          body.add(subgoal_name_mappings.get(subgoal.name));
          
          view_subgoal_name_mappings.put(subgoal.name, query.subgoal_name_mapping.get(subgoal.name));
          
          max_size--;
        }
      }
      
      
      
      
      
      
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Vector<Argument> args = query.head.agg_args.get(i);
        
        int m = 0;
        
        for(m = 0; m<args.size(); m++)
        {
          Argument arg = args.get(m);
          
          String subgoal_name = arg.relation_name;
          
          if(!names.contains(subgoal_name))
          {
            break;
          }
        }
        
        if(m < args.size())
        {
          continue;
        }
        
        if(head_grouping_attrs.isEmpty())
        {
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
        
        Boolean b = r.nextBoolean();
        
        if(b)
        {
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
      }
    

    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = all_args.get(i);
      
      String arg_relation = arg.relation_name;
      String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(query_generator.parameterizable_attri.get(arg_relation).contains(arg_name) && !query_generator.relation_primary_key_mapping.get(arg_relation).contains(arg_name))
      {
        double b = r.nextDouble();
        
        if(b < head_var_rate && names.contains(arg_relation) && !head_grouping_attrs.contains(all_args.get(i)))
        {
          head_grouping_attrs.add(all_args.get(i));
          break;
        }
      }

    }
    
    Subgoal head = new Subgoal("v" + id, head_grouping_attrs, head_agg_attrs, head_agg_functions, true);
    
    Vector<Conditions> q_conditions = new Vector<Conditions>();

    return new Query("v" + id, head, body, new Vector<Lambda_term>(), q_conditions , view_subgoal_name_mappings);

  }
  
  
  static Query generate_view_without_predicates_partial_mappings_random(Vector<Conditions> conditions, HashSet<String> subgoal_names, Query query, int id, int size, Vector<String> all_citable_tables, HashMap<String, String> relation_primary_key_mappings, boolean has_agg, Connection c, PreparedStatement pst) throws SQLException
  {
    
    Vector<Argument> all_args = get_all_attributes(query);
    all_args.removeAll(query.head.args);
    
    Vector<Subgoal> body = new Vector<Subgoal>();
    
    body.addAll(query.body);
    
    Vector<Argument> head_grouping_attrs = new Vector<Argument>();
    
    head_grouping_attrs.addAll(query.head.args);
    
    Vector<Vector<Argument>> head_agg_attrs = new Vector<Vector<Argument>>();

    Vector<String> head_agg_functions = new Vector<String>();
    
    Random r = new Random();

    HashMap<String, String> maps = new HashMap<String, String>();
    
    if(has_agg)
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Argument head_grouping_arg = (Argument) query.head.agg_args.get(i).get(0);
        String rel_name = head_grouping_arg.relation_name;//.name.substring(0, head_grouping_arg.name.indexOf(init.separator));
        String origin_rel_name = query.subgoal_name_mapping.get(rel_name);
        String arg_name = head_grouping_arg.attribute_name;//.name.substring(head_grouping_arg.name.indexOf(init.separator) + 1, head_grouping_arg.name.length());
        
        if(query_generator.parameterizable_attri.get(origin_rel_name).contains(arg_name))
        {
          Boolean b = r.nextBoolean();
          
          if(b)
          {
            Vector<Argument> curr_head_arr = new Vector<Argument>();
            
            curr_head_arr.add((Argument) query.head.agg_args.get(i).get(0));
            
            head_agg_attrs.add(curr_head_arr);
            head_agg_functions.add((String) query.head.agg_function.get(i));
          }
        }
      }
      
      if(head_agg_attrs.isEmpty())
      {
        Vector<Argument> curr_head_arr = new Vector<Argument>();
        
        curr_head_arr.add((Argument) query.head.agg_args.get(0).get(0));
        
        head_agg_attrs.add(curr_head_arr);
        head_agg_functions.add((String) query.head.agg_function.get(0));
      }
    }
    else
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Boolean b = r.nextBoolean();
        
        if(b)
        {
          Vector<Argument> args = query.head.agg_args.get(i);
          
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
      }
    }
    

    maps.putAll(query.subgoal_name_mapping);
    
    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = all_args.get(i);
      
      String arg_relation = arg.relation_name;
      String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(query_generator.parameterizable_attri.get(arg_relation).contains(arg_name) && !query_generator.relation_primary_key_mapping.get(arg_relation).contains(arg_name))
      {
        Boolean b = r.nextBoolean();
        
        if(b && !head_grouping_attrs.contains(all_args.get(i)))
        {
          head_grouping_attrs.add(all_args.get(i));
          break;
        }
      }

    }
    
    Subgoal head = new Subgoal("v" + id, head_grouping_attrs, head_agg_attrs, head_agg_functions, true);
    
    Vector<Conditions> q_conditions = new Vector<Conditions>();
    
    q_conditions.addAll(query_generator.gen_conditions_random(false, view_instance_size, body, maps, c, pst));
    
    if(has_agg)
    {
//      if(conditions.isEmpty())
//      {
//          q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, body, maps, c, pst));
//      }
//      else
//      {
//        q_conditions.addAll(conditions);
//      }
//      
//      conditions.clear();
//      
//      conditions.addAll(q_conditions);
      
      boolean b = r.nextBoolean();
      
      if(b)      
        q_conditions.addAll(query_generator.gen_conditions_random(body, maps, c, pst));
    }
    
    return new Query("v" + id, head, body, new Vector<Lambda_term>(), q_conditions , maps);

  }
  

}
