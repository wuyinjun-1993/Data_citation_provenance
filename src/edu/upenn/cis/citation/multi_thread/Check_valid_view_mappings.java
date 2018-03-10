package edu.upenn.cis.citation.multi_thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.bit_operation.Bit_operation;
import edu.upenn.cis.citation.citation_view.Head_strs;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.views.Single_view;

public class Check_valid_view_mappings implements Runnable {
  private Thread t;
  private String threadName;
  
  public Single_view view;
  
  public HashSet<Tuple> view_mappings;
  
  public ArrayList<Vector<Head_strs>> values_from_why_tokens;
  
//  public HashMap<Tuple, long[]> tuple_rows_bit_index = new HashMap<Tuple, long[]>();
  
  public HashMap<Tuple, HashSet<Integer>> tuple_rows = new HashMap<Tuple, HashSet<Integer>>();

  
  public Connection c;
  
  public PreparedStatement pst;
  
  public HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings;
  
//  HashMap<String, String> subgoal_name_mappings;
  
  public Check_valid_view_mappings( String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings, Connection c, PreparedStatement pst) {
     threadName = name;

     this.view = view;
     
     this.view_mappings = view_mappings;
     
     this.values_from_why_tokens = curr_tuples;
     
     this.rel_attr_value_mappings = rel_attr_value_mappings;
     
     this.c = c;
     
     this.pst = pst;
     
//     this.subgoal_name_mappings = subgoal_name_mappings;
//     System.out.println("Creating " +  threadName );
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
    
    for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
    {
//      view.reset_values();
      
      Tuple tuple = (Tuple) iter2.next();
      
//      System.out.println(tuple.name + "|" + tuple.mapSubgoals_str);
      
      //each table -> related table -> arg_list
      
      HashMap<String, ArrayList<Conditions>> undermined_table_conditions_mappings = new HashMap<String, ArrayList<Conditions>>();
      
      HashMap<String, ArrayList<ArrayList<String>>> undetermined_table_arg_value_mappings = new HashMap<String, ArrayList<ArrayList<String>>>();
      
      long [] bit_sequence = Bit_operation.init(values_from_why_tokens.size());
      
      HashSet<Integer> rids = new HashSet<Integer>();
      
//      tuple_rows_bit_index.put(tuple, bit_sequence);
      
      tuple_rows.put(tuple, rids);
      
      boolean first = true;
      
      ArrayList<String[][]> partial_mapping_values = new ArrayList<String[][]>();
      
      for(int i = 0; i<tuple.cluster_patial_mapping_condition_ids.size(); i++)
      {
        HashSet<String> partial_mapping_subgoals = get_unique_partial_mapping_subgoals(view, tuple, i);
        
        String[][] curr_partial_mapping_values = new String[values_from_why_tokens.size()][partial_mapping_subgoals.size()];
        
        partial_mapping_values.add(curr_partial_mapping_values);
      }
      
      for(int i = 0; i<values_from_why_tokens.size(); i++)
      {
        view.evaluate_args(values_from_why_tokens.get(i), tuple);
        
        if(view.check_validity(tuple, partial_mapping_values, i))//rel_attr_value_mappings, undermined_table_conditions_mappings, undetermined_table_arg_value_mappings, first, c, pst))
        {
          
//              if(undermined_table_conditions_mappings.size() == 0)              
          {
//            Bit_operation.set_bit(tuple_rows_bit_index.get(tuple), i);
            
            tuple_rows.get(tuple).add(i);
          }
          
        }
        
//        first = false;
      }
      
      try {
        get_valid_row_ids(tuple, partial_mapping_values, tuple_rows.get(tuple), view.conditions, view.subgoals, view.subgoal_name_mappings, c, pst);
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
      
      Argument arg2 = condition.arg2;
      
      Argument arg1 = condition.arg1;
      
      if(condition.get_mapping2)
      {
        if(!partial_join_mapped_attribute_names.contains(arg2.name))
        {
          partial_join_mapped_attribute_names.add(arg2.name);
          
        }
      }
      
      if(condition.get_mapping1)
      {
        if(!partial_join_mapped_attribute_names.contains(arg1.name))
        {
          partial_join_mapped_attribute_names.add(arg1.name);

        }
        
      }
      
      
    }
    
    return partial_join_mapped_attribute_names;
  }
  
  static void get_valid_row_ids(Tuple tuple, ArrayList<String[][]> partial_mapping_values, HashSet<Integer> row_ids, Vector<Conditions> conditions, Vector<Subgoal> subgoals, HashMap<String, String> subgoal_name_mappings, Connection c, PreparedStatement pst) throws SQLException
  {
    
//    System.out.println(tuple);

    String sql_base = "select t.row_id from (VALUES ";
    

    
    for(int i = 0; i<tuple.cluster_subgoal_ids.size(); i++)
    {
      if(row_ids.isEmpty())
        return;
      
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
      
      for(Integer id: row_ids)
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
      
      sql += ") as t(row_id ";
      
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
          
          join_condition += "t." + arg1.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg2.name.replaceFirst("\\" + init.separator, ".");

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
          
          join_condition += "t." + arg2.name.replaceAll("\\" + init.separator, "_") + condition.op.toString() + arg1.name.replaceFirst("\\" + init.separator, ".");

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
      
      row_ids.clear();
      
      while(rs.next())
      {
        row_ids.add(rs.getInt(1));
      }
      
//      if(tuple.toString().equals("v4|family0=family,introduction2=introduction"))
//      {
//        System.out.println(row_ids);
//      }
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
  
  public void start () {
//     System.out.println("Starting " +  threadName );
     if (t == null) {
        t = new Thread (this, threadName);
        t.start ();
     }
  }
  
  public void join() throws InterruptedException
  {
    t.join();
  }
}

