package edu.upenn.cis.citation.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;
import org.apache.logging.log4j.Level;
import org.gprom.jdbc.driver.GProMConnection;
import org.gprom.jdbc.utility.PropertyConfigurator;
import com.sun.jna.Native;

public class minimal_test {
  
  public static GProMConnection con = null;
  
  public static String db_prov_url = "jdbc:gprom:postgresql://localhost:5432/";
  
  public static String db_url = "jdbc:postgresql://localhost:5432/";
  
  public static Vector<String[]> provenance_content = new Vector<String[]> ();
  
  public static HashMap<Head_strs, ArrayList<Integer>> tuple_why_prov_mappings = new HashMap<Head_strs, ArrayList<Integer>>();
  
  public static ArrayList<Vector<Head_strs>> all_why_tokens = new ArrayList<Vector<Head_strs>>();

  
  public static String sql = "PROVENANCE OF(select \"object.annotation_status\",\"ligand.drugs_url\",\"object.systematic_name\",\"ligand.approved_source\",\"object.last_modified\",\"object.only_grac\",\"ligand.in_gtip\","
      + "\"object.old_object_id\",\"ligand.approved\",\"ligand.radioactive\",\"gpcr.object_id\",\"object.abbreviation\",\"ligand.pubchem_sid\",\"object.only_iuphar\",\"ligand.abbreviation_vector\","
      + "\"object.quaternary_structure_comments\",\"ligand.type\",\"gpcr.ligand\",\"object.in_gtip\",\"ligand.old_ligand_id\",\"ligand.verified\",\"ligand.ligand_id\",\"object.in_cgtp\",\"ligand.labelled\","
      + "\"ligand.abbreviation\",\"object.object_id\",\"ligand.withdrawn_drug\",\"object.no_contributor_list\" from \"object\" \"object\",\"ligand\" \"ligand\",\"gpcr\" \"gpcr\" "
      + "where (\"gpcr.object_id\"=64 or \"gpcr.object_id\"=224 or \"gpcr.object_id\"=290 or \"gpcr.object_id\"=295 or \"gpcr.object_id\"=168 or \"gpcr.object_id\"=364 or \"gpcr.object_id\"=108 or \"gpcr.object_id\"=13 "
      + "or \"gpcr.object_id\"=237 or \"gpcr.object_id\"=110 or \"gpcr.object_id\"=303 or \"gpcr.object_id\"=176 or \"gpcr.object_id\"=82 or \"gpcr.object_id\"=754 or \"gpcr.object_id\"=18 or \"gpcr.object_id\"=372 "
      + "or \"gpcr.object_id\"=22 or \"gpcr.object_id\"=118 or \"gpcr.object_id\"=665 or \"gpcr.object_id\"=219 or \"gpcr.object_id\"=668 or \"gpcr.object_id\"=318) and (\"ligand.ligand_id\"=8928 or \"ligand.ligand_id\"=5411 "
      + "or \"ligand.ligand_id\"=7909 or \"ligand.ligand_id\"=3303 or \"ligand.ligand_id\"=9191 or \"ligand.ligand_id\"=7785 or \"ligand.ligand_id\"=5643 or \"ligand.ligand_id\"=6287 or \"ligand.ligand_id\"=241 or \"ligand.ligand_id\"=9362 "
      + "or \"ligand.ligand_id\"=1076 or \"ligand.ligand_id\"=8661 or \"ligand.ligand_id\"=7765 or \"ligand.ligand_id\"=4757 or \"ligand.ligand_id\"=4213 or \"ligand.ligand_id\"=886 or \"ligand.ligand_id\"=2808 or \"ligand.ligand_id\"=4664 "
      + "or \"ligand.ligand_id\"=5468 or \"ligand.ligand_id\"=2077 or \"ligand.ligand_id\"=94 or \"ligand.ligand_id\"=9375) and (\"object.object_id\"=64 or \"object.object_id\"=1249 or \"object.object_id\"=1378 or \"object.object_id\"=1380 "
      + "or \"object.object_id\"=1479 or \"object.object_id\"=2408 or \"object.object_id\"=232 or \"object.object_id\"=425 or \"object.object_id\"=2026 or \"object.object_id\"=2475 or \"object.object_id\"=335 or \"object.object_id\"=2031 "
      + "or \"object.object_id\"=1201 or \"object.object_id\"=2263 or \"object.object_id\"=2520 or \"object.object_id\"=1017 or \"object.object_id\"=1977 or \"object.object_id\"=1817 or \"object.object_id\"=2522 or \"object.object_id\"=2874 "
      + "or \"object.object_id\"=1052 or \"object.object_id\"=1436))";
  
  static String [] relations = {"object", "ligand", "gpcr"};
  
  static int head_arg_size = 28;
  
  public static void connect(String url, String usr_name, String passwd) throws ClassNotFoundException, SQLException
  {
    System.setProperty("java.library.path", "./lib");
    
    Native.setProtected(true);
    
    String driverURL = "org.postgresql.Driver";
    
    Class.forName("org.gprom.jdbc.driver.GProMDriver");
    Class.forName(driverURL);
    
    PropertyConfigurator.configureDefaultConsoleLogger(Level.OFF);
    
    Properties info = new Properties();
    
    info.setProperty("user", "wuyinjun");
    info.setProperty("password", "12345678");
//    log.error("made it this far");
    
    try{
      con = (GProMConnection) DriverManager.getConnection(url,info);
  } catch (Exception e){
      e.printStackTrace();
      System.err.println("Something went wrong while connecting to the database.");
      System.exit(-1);
  }
    
  }
  
  public static ResultSet get_provenance4query(String sql) throws SQLException, FileNotFoundException, UnsupportedEncodingException
  {
    
    Statement st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    
    ResultSet rs = null;
    
    rs = st.executeQuery(sql);
    
    return rs;
    
    
  }
  
  
  public static void reset() throws SQLException
  {
    con.close();
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
  
  static Vector<Head_strs> get_tuples(ResultSet rs, Vector<Integer> attr_nums) throws SQLException
  {
    Vector<Head_strs> curr_tuples = new Vector<Head_strs>();
    
    Vector<String> provenance = new Vector<String>();
    
//    int total_col_count = provenance_row.length;
    
    int col_nums = head_arg_size;
    
    for(int i = 0; i<attr_nums.size(); i++)
    {
      provenance.clear();
      
      for(int j = 0; j<attr_nums.get(i); j++)
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
  
  private static void printResult(ResultSet rs, Vector<Integer> attr_nums) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
    
    int rows = 0;
    
    while(rs.next())
    {
      
      Head_strs values = get_query_result(rs, head_arg_size);
      
      Vector<Head_strs> curr_tuples = get_tuples(rs, attr_nums);
      
      System.out.println(rows);
//      
//      System.out.println(Runtime.getRuntime().totalMemory());
//      
//      System.out.println(Runtime.getRuntime().freeMemory());
//      
//      System.out.println(curr_tuples);
      
      
      
      if(tuple_why_prov_mappings.get(values) == null)
      {
        ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
        
        curr_tokens.add(rows);
        
        tuple_why_prov_mappings.put(values, curr_tokens);
        
//        System.out.println(values + "::" + curr_tokens);
        
      }
      else
      {
        tuple_why_prov_mappings.get(values).add(rows);
        
//        System.out.println(values + "::" + tuple_why_prov_mappings.get(values));
      }
      
      all_why_tokens.add(curr_tuples);
      
      rows ++;
      
      
    }
    
    
    
    
  }
  
  
  public static Vector<Integer> get_attri_nums(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Integer> attri_nums = new Vector<Integer>();
    
    String q_base = "select count(column_name) from information_schema.columns where table_name = ";
    
    for(int i = 0; i<relations.length; i++)
    {
      String relation = relations[i];
      
      String q = q_base + "'" + relation + "'";
      
      Statement st = c.createStatement();
      
      ResultSet rs = st.executeQuery(q);
      
      if(rs.next())
        attri_nums.add(rs.getInt(1));
    }
    
    return attri_nums;
    
  }
  
  public static void main(String [] args) throws ClassNotFoundException, SQLException, FileNotFoundException, UnsupportedEncodingException
  {
    
    String usr_name = args[0];
    
    String passwd = args[1];
    
    String db_name = args[2];
    
    Connection c = null;
    PreparedStatement pst = null;
    
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(db_url + db_name, usr_name , passwd);
  
  
    Vector<Integer> attr_nums = get_attri_nums(c, pst);

    
    
    
    
    
    
    connect(db_prov_url + db_name, usr_name, passwd);
    
    ResultSet rs = get_provenance4query(sql);
    
    printResult(rs, attr_nums);
    
    reset();
    
    System.out.println("finished");
  }
  
  
private static class Head_strs {
    
    public Vector<String> head_vals;
    
    public Head_strs(Vector<String> vec_str)
    {
        
        head_vals = new Vector<String> ();
        this.head_vals.addAll(vec_str);
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        Head_strs vec_str = (Head_strs) obj;
        
        if(head_vals.size() != vec_str.head_vals.size())
            return false;
        
        
        for(int i = 0; i<head_vals.size(); i++)
        {
            if(head_vals.get(i) == null && vec_str.head_vals.get(i) == null)
                continue;
            
            
            if(head_vals.get(i) == null && vec_str.head_vals.get(i) != null)
                return false;
            
            if(head_vals.get(i) != null && vec_str.head_vals.get(i) == null)
                return false;
            
            if(!head_vals.get(i).equals(vec_str.head_vals.get(i)))
                return false;
        }
        
        return true;
        
    }
    
    @Override
    public int hashCode() {
        
//      int hashCode = 0;
//      
//      for (int i = 0; i < head_vals.size(); i ++) {
//        hashCode += head_vals.get(i).hashCode();
//      }
        
        return toString().hashCode();
      }
    
    
    @Override
    public String toString()
    {
        
        String output = new String();
        
        for(int i = 0; i<head_vals.size(); i++)
        {
            if(i >= 1)
                output += " ";
            
            output += head_vals.get(i);
        }
        
        return output;
    }
    
    public void clear()
    {
        this.head_vals.clear();
    }

}


}
