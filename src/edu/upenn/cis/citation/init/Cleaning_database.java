package edu.upenn.cis.citation.init;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;
import java.util.Map.Entry;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.query_view_generators.query_generator;
import edu.upenn.cis.citation.views.Query_converter;

public class Cleaning_database {
  
  public static void main(String[] args) throws SQLException, ClassNotFoundException
  {
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
    
  HashMap<String, Vector<String>> relation_primary_key_mappigns = new HashMap<String, Vector<String>>();
    for(int i = 0; i<query_generator.citatable_tables.length; i++)
    {
      get_primary_key(query_generator.citatable_tables[i], relation_primary_key_mappigns, c, pst);
    }
  
  
    for(int i = 0; i<query_generator.citatable_tables.length; i++)
    {
      String relation = query_generator.citatable_tables[i];
      Vector<String> arguments = new Vector<String>();
      Vector<String> data_types = new Vector<String>();
      get_arg_with_type(relation, relation_primary_key_mappigns, arguments, data_types, c, pst);
//      String sql = gen_sql(relation, relation_primary_key_mappigns, arguments, data_types);
//      System.out.println(sql);
//      pst = c.prepareStatement(sql);
//      pst.execute();
    }
    c.close();
  }
  
  static void get_primary_key(String table_name, HashMap<String, Vector<String>> relation_primary_key_mappings, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "SELECT c.column_name, c.data_type FROM information_schema.table_constraints tc "
        + "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) "
        + "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = "
        + "c.table_name AND ccu.column_name = c.column_name where constraint_type = 'PRIMARY KEY' and tc.table_name = '" + table_name +"'";
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    Vector<String> primary_keys = new Vector<String>();
    
    while(rs.next())
    {
      String primary_key = rs.getString(1);
      primary_keys.add(primary_key);
    }
    relation_primary_key_mappings.put(table_name, primary_keys);

  }
  
  static boolean containsNull(String table_name, String arg_name, Connection c, PreparedStatement pst) throws SQLException
  {
    String string = "select count(*) from " + table_name + " where " + arg_name + " is null";
    
    pst = c.prepareStatement(string);
    
    ResultSet rs = pst.executeQuery();
    
    if(rs.next())
    {
      int count = rs.getInt(1);
      
      if(count == 0)
        return false;
      else
        return true;
    }
    return false;
  }
  
  static void get_arg_with_type(String table_name, HashMap<String, Vector<String>> relation_primary_key_mappings, Vector<String> args, Vector<String> data_types, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = "select column_name, data_type from information_schema.columns where table_name = '" + table_name + "'";
    
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    Vector<String> primary_keys = relation_primary_key_mappings.get(table_name);
    
    while(rs.next())
    {
      String arg_name = rs.getString(1);
      
      if(!containsNull(table_name, arg_name, c, pst))
      {
        continue;
      }
      
      if(primary_keys.contains(arg_name))
        continue;
      
      
      String data_type = rs.getString(2);
      String update_sql = new String();
      System.out.println(arg_name + "::" + data_type);
      
      if(!Query_converter.numeric_data_type_set.contains(data_type))
      {
        update_sql = "alter table " + table_name + " alter " + arg_name + " type text";
        System.out.println(update_sql);
        try {
          pst = c.prepareStatement(update_sql);
          pst.execute();
          update_sql = "update " + table_name + " set " + arg_name + " = 'null' where " + arg_name + " is null";
          System.out.println(update_sql);
          pst = c.prepareStatement(update_sql);
          pst.execute();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          update_sql = "update " + table_name + " set " + arg_name + " = 'null' where " + arg_name + " is null";
          System.out.println(update_sql);
          pst = c.prepareStatement(update_sql);
          pst.execute();
        }
        
      }
      else
      {
        update_sql = "alter table " + table_name + " alter " + arg_name + " type double precision";
        System.out.println(update_sql);
        pst = c.prepareStatement(update_sql);
        pst.execute();
        
        update_sql = "update " + table_name + " set " + arg_name + " = 'nan' where " + arg_name + " is null";
        System.out.println(update_sql);
        pst = c.prepareStatement(update_sql);
        pst.execute();
        
      }
      
//      args.add(arg_name);
//      data_types.add(data_type);
//      
//      pst = c.prepareStatement(update_sql);
//      System.out.println(update_sql);
//      try {
//        pst.execute();
//      } catch (SQLException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
    }
  }
  
  static String gen_sql(String table_name, HashMap<String, Vector<String>> relation_primary_key_mappigns, Vector<String> arg_names, Vector<String> data_types)
  {
    String string = new String();
//    for(Entry<String, Vector<String>> entry: unique_relations.entrySet())
    {
//      String relation_name = entry.getKey();
//      Vector<String> arg_names = entry.getValue();
//      Vector<String> type_list = types.get(relation_name);
//      if(count > 0)
//        string += ",";
      Vector<String> primary_keys = relation_primary_key_mappigns.get(table_name);
      string += "update " + table_name + " set(";
      Vector<String> updated_strings = new Vector<String>();
      for(int i = 0; i<arg_names.size(); i++)
      {
        if(i >= 1)
          string += ",";
        String arg_name_string = arg_names.get(i);
        if(primary_keys.contains(arg_name_string))
          continue;
        
        
        String data_type = data_types.get(i);
        string += arg_name_string;
        if(!Query_converter.numeric_data_type_set.contains(data_type))
          updated_strings.add("case when "+ arg_name_string +" is null then 'null' else cast (" + arg_name_string +" as text) end");
        else
          updated_strings.add("case when "+ arg_name_string +" is null then 'nan' else cast (" + arg_name_string +" as double precision) end");
      }
      string += ")=(";
      for(int i = 0; i < updated_strings.size(); i++)
      {
        if(i >= 1)
          string += ",";
        string += updated_strings.get(i);
      }
      string += ")";
      
//      string += " from " + table_name + ")";
      
    }
    return string;
  }

}
