package edu.upenn.cis.citation.init;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Vector;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class init {
  
  public static String db_url = "jdbc:postgresql://localhost:5432/provenance";
  
  public static String db_prov_url = "jdbc:gprom:postgresql://localhost:5432/provenance";
  
  public static String db_url2 = "jdbc:postgresql://localhost:5432/test2";

  
  static String [] view_tables = {"view2conditions", "view2lambda_term", "view2subgoals", "view_table", 
      "query2conditions", "query2head_variables", "query2lambda_term", "query2subgoal", 
      "citation2query", "citation2view", "citation_table", 
      "user_query2conditions", "user_query2subgoals", "user_query_conditions", "user_query_table"};
  
  public static String separator = "|";
  
  public static String provenance_column_suffix = separator + "prov";
  
  public static String usr_name = "wuyinjun";
  
  public static String passwd = "12345678";
  
  public static void main(String [] args) throws ClassNotFoundException, SQLException
  {
        
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(db_url, usr_name , passwd);
    
  reset(c, pst);
  
//    Vector<String> relation_names = get_all_base_relations(c, pst);
//    
//    for(int i = 0; i<relation_names.size(); i++)
//    {
//      insert_provenance_columns(relation_names.get(i), c, pst);
//      
//      insert_provenance_tokens(relation_names.get(i), c, pst);
//    }
  
  c.close();
  }
  
  
  static void reset(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<String> relations = get_all_base_relations(c, pst);
    
    for(int i = 0; i<relations.size(); i++)
    {
      String relation = relations.get(i);
      
      reset_relation(relation, c, pst);
    }
  }
  
  static void insert_provenance_tokens(String relation, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "select * from " + relation;
    
    Vector<String> column_names = get_attributes_single_relation_without_provenance_column(relation, c, pst);
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    String update_query_head = "update " + relation + " set ";
    
    
    
    int num = 0;
    
    while(rs.next())
    {
      String where_clause = " where ";
      
      for(int i = 0; i<column_names.size(); i++)
      {
        String value = rs.getString(column_names.get(i));
        
        if(i >= 1)
          where_clause += " and ";
        
        if(value!=null && value.contains("'"))
        {
          value = value.replaceAll("'", "''");
        }
        if(value == null)
        {
          where_clause += column_names.get(i) + " is null";
        }
        else
        {
          where_clause += column_names.get(i) + " = '" + value + "'";
        }
        
      }
      
//      for(int i = 0; i<column_names.size(); i++)
//      {        
//        String col_name_encoding = MD5.get_MD5_encoding(relation, column_names.get(i));
//        
//        String update_query = update_query_head + "\"c" + separator + col_name_encoding + provenance_column_suffix + "\" = '" + relation + separator + column_names.get(i) + separator + num + "'" + where_clause;
//        
//        System.out.println(update_query);
//        
//        pst = c.prepareStatement(update_query);
//        
//        pst.execute();
//      }
            
      String col_name_encoding = MD5.get_MD5_encoding(relation);
      
      String update_query = update_query_head + "\"c" + separator + col_name_encoding + provenance_column_suffix + "\" = '" + relation + separator + num + "'" + where_clause;
      
      pst = c.prepareStatement(update_query);
      
      pst.execute();
      
      num ++;
    }
  }
  
  public static Vector<String> get_all_base_relations(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<String> table_list = new Vector<String>();
    
    HashSet<String> view_table_list = new HashSet<String>(Arrays.asList(view_tables));
    
    String query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    while(rs.next())
    {
      
        String relation_name = rs.getString(1);
        
        if(view_table_list.contains(relation_name))
        {
          continue;
        }
      
        table_list.add(rs.getString(1));
    }
    
    query = "SELECT table_name FROM information_schema.views WHERE table_schema='public'";
    
    pst = c.prepareStatement(query);
    
    rs = pst.executeQuery();
    
    while(rs.next())
    {
      
        String relation_name = rs.getString(1);
        
        if(view_table_list.contains(relation_name))
        {
          continue;
        }
      
        table_list.remove(relation_name);
    }
    
    return table_list;
  }
  
  public static Vector<String> get_attributes_single_relation(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + relation_name +"' ORDER BY ordinal_position";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    Vector<String> attributes = new Vector<String>();
    
    while(rs.next())
    {
      String attribute = rs.getString(1);
      
      if(!attribute.endsWith(provenance_column_suffix))
      {
        attributes.add(attribute);
      }
      
    }
    
    return attributes;
    
  }
  
  public static Vector<String> get_attributes_single_relation_without_provenance_column(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + relation_name +"' ORDER BY ordinal_position";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    Vector<String> attributes = new Vector<String>();
    
    while(rs.next())
    {
      
      String attr_name = rs.getString(1);
      
      if(!attr_name.endsWith(provenance_column_suffix))
      {
        attributes.add(rs.getString(1));
      }
    }
    
    return attributes;
    
  }
  
  
  static void reset_relation(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
    String query = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + relation_name +"' ORDER BY ordinal_position";
    
    pst = c.prepareStatement(query);
    
    ResultSet rs = pst.executeQuery();
    
    while(rs.next())
    {
      String attr_name = rs.getString(1);
      
      if(attr_name.endsWith(provenance_column_suffix))
      {
        String reset_query = "alter table " + relation_name + " drop column \"" + attr_name + "\"";
        
        pst = c.prepareStatement(reset_query);
        
        pst.execute();
      }
      
    }
  }
  
  public static void insert_provenance_columns(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
    reset_relation(relation_name, c, pst);
    
    Vector<String> attr_names = get_attributes_single_relation(relation_name, c, pst);
    
//    for(int i = 0; i<attr_names.size(); i++)
//    {
//                  
//      String col_name_encoding = MD5.get_MD5_encoding(relation_name, attr_names.get(i));
//      
//      String query = "alter table " + relation_name + " add column \"c" + separator + col_name_encoding + provenance_column_suffix + "\" text";
//                  
//      pst = c.prepareStatement(query);
//      
//      pst.execute();
//    }
    
    String relation_name_encoding = MD5.get_MD5_encoding(relation_name);
    
    String query = "alter table " + relation_name + " add column \"c" + separator + relation_name_encoding + provenance_column_suffix + "\" text";
    
    pst = c.prepareStatement(query);
    
    pst.execute();
  }
  

}
