package edu.upenn.cis.citation.multi_thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import edu.upenn.cis.citation.citation_view.Head_strs;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.views.Single_view;

public class Executing_batch_query implements Runnable {
  static String prefix = "jdbc:postgresql://localhost:5432/";
  private Thread t;
  private String threadName;
  private String database;
  private String user_name;
  private String password;
  public ConcurrentHashMap<String, Integer> result = null;
  private Connection c;
  private PreparedStatement pst;
  private int head_size;
  private Single_view view;
  private HashMap<String, String> sql_other_clauses;
  private String where_clause_attrs;
  private Vector<String> where_clause_values;
  private int start;
  private int end;
  private ConcurrentHashMap<String, String> view_grouping_value_query_grouping_value_mappings = null;
  
  
  Executing_batch_query( String name, int head_size, Single_view view, HashMap<String, String> sql_other_clauses, 
      String where_clause_attrs, Vector<String> where_clause_values, int start, int end, ConcurrentHashMap<String, Integer> counts_per_group, Connection c, PreparedStatement pst) {
     threadName = name;
     this.c = c;
     this.pst = pst;
     this.head_size = head_size;
     this.view = view;
     this.sql_other_clauses = sql_other_clauses;
     this.where_clause_attrs = where_clause_attrs;
     this.where_clause_values = where_clause_values;
     this.start = start;
     this.end = end;
     this.result = counts_per_group;
//     System.out.println("Creating " +  threadName );
  }
  
  Executing_batch_query( String name, int head_size, Single_view view, HashMap<String, String> sql_other_clauses, 
      String where_clause_attrs, Vector<String> where_clause_values, int start, int end, ConcurrentHashMap<String, Integer> counts_per_group, ConcurrentHashMap<String, String> view_grouping_value_query_grouping_value_mappings, Connection c, PreparedStatement pst) {
     threadName = name;
     this.c = c;
     this.pst = pst;
     this.head_size = head_size;
     this.view = view;
     this.sql_other_clauses = sql_other_clauses;
     this.where_clause_attrs = where_clause_attrs;
     this.where_clause_values = where_clause_values;
     this.start = start;
     this.end = end;
     this.result = counts_per_group;
     this.view_grouping_value_query_grouping_value_mappings = view_grouping_value_query_grouping_value_mappings;
//     System.out.println("Creating " +  threadName );
  }
  
  public void run() {
    
  try {
//    String sql = gen_sql(view, sql_other_clauses, where_clause_attrs, where_clause_values, start, end);
//    pst = c.prepareStatement(sql);
//    ResultSet rs = pst.executeQuery();
    execute_query_batch();
    System.out.println("done");
    
//    retrive_result(result, view_grouping_value_query_grouping_value_mappings, rs, head_size);
    
  } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
  } catch (ClassNotFoundException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
  }
    
  }
  
  void execute_query_batch() throws SQLException, ClassNotFoundException
  {
    int partition_size = 20000;//(int) Math.sqrt(end -start);
    int num = start;
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
    while(num < end)
    {
      String sql = view.local_with_clause + " select ";
      
      if(sql_other_clauses.get("select") != null)
      {
        sql += sql_other_clauses.get("select") + sql_other_clauses.get("select_agg");
      }
      else
      {
        sql += sql_other_clauses.get("select_agg");
      }
      
      sql += " from " + sql_other_clauses.get("from");
      int start1 = num;
      int end1 = (start1 + partition_size) < where_clause_values.size() ? (start1 + partition_size): where_clause_values.size();
      
      if(sql_other_clauses.get("where") != null)
      {
        sql += " where " + sql_other_clauses.get("where") + " and " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start1, end1) + ")";
      }
      else
      {
        sql += " where " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start1, end1) + ")";
      }
      
      
      
      if(sql_other_clauses.get("group_by") != null)
      {
        sql += " group by " + sql_other_clauses.get("group_by"); 
      }
      
      if(sql_other_clauses.get("having") != null)
      {
        sql += " having " + sql_other_clauses.get("having");
      }
      
//      System.out.println(sql);
//      System.out.println(sql);
      
      
      
      pst = c.prepareStatement(sql);
      
      
      ResultSet rs = pst.executeQuery();
//      t2 = System.nanoTime();
//      time = (t2 - t1)*1.0/1000000000;
//      System.out.println("sql_time::" + time);
      retrive_result(result, view_grouping_value_query_grouping_value_mappings, rs, head_size);
//      counts_per_group.putAll();
//      merge_result(counts_per_group, retrive_result(rs, grouping_attr_num));
//      System.out.println(num);
      num += partition_size;
    }
    c.close();
  }
  
  static String gen_value_partitions(Vector<String> where_clause_values, int start, int end)
  {
    StringBuilder sb = new StringBuilder(110*(end -start));
    for(int i = start; i<end; i++)
    {
      if(i >= start + 1)
        sb.append(",");
      sb.append(where_clause_values.get(i));
    }
    return sb.toString();
  }
  
  static String gen_sql(Single_view view, HashMap<String, String> sql_other_clauses, String where_clause_attrs, Vector<String> where_clause_values, int start, int end)
  {
    String sql = view.local_with_clause + " select ";
    
    if(sql_other_clauses.get("select") != null)
    {
      sql += sql_other_clauses.get("select") + sql_other_clauses.get("select_agg");
    }
    else
    {
      sql += sql_other_clauses.get("select_agg");
    }
    
    sql += " from " + sql_other_clauses.get("from");
//    int start = num;
//    int end = (start + partition_size) < where_clause_values.size() ? (start + partition_size): where_clause_values.size();
    
    
    if(sql_other_clauses.get("where") != null)
    {
      sql += " where " + sql_other_clauses.get("where") + " and " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start, end) + ")";
    }
    else
    {
      sql += " where " + where_clause_attrs + "=ANY(VALUES" + gen_value_partitions(where_clause_values, start, end) + ")";
    }
    
    if(sql_other_clauses.get("group_by") != null)
    {
      sql += " group by " + sql_other_clauses.get("group_by"); 
    }
    
    if(sql_other_clauses.get("having") != null)
    {
      sql += " having " + sql_other_clauses.get("having");
    }
    
    return sql;
  }
  
  static void retrive_result(ConcurrentHashMap<String, Integer> counts_per_group, ConcurrentHashMap<String, String> view_grouping_value_query_grouping_value_mappings, ResultSet rs, int head_size) throws SQLException
  {
    if(view_grouping_value_query_grouping_value_mappings == null)
    {
      while(rs.next())
      {
        String value = rs.getString(1);
        int count = rs.getInt(2);
        counts_per_group.put(value, count);
//        Vector<String> grouping_value = new Vector<String>();
//        for(int i = 0; i<head_size; i++)
//        {
//          grouping_value.add(rs.getString(i + 1));
//        }
//        int count = rs.getInt(head_size + 1);
//        res.put(new Head_strs(grouping_value), count);
      }
    }
    else
    {
      while(rs.next())
      {
        String value = rs.getString(1);
        String q_prov_value = rs.getString(2);
        int count = rs.getInt(3);
        view_grouping_value_query_grouping_value_mappings.put(value, q_prov_value);
        counts_per_group.put(value, count);
//        Vector<String> grouping_value = new Vector<String>();
//        for(int i = 0; i<head_size; i++)
//        {
//          grouping_value.add(rs.getString(i + 1));
//        }
//        int count = rs.getInt(head_size + 1);
//        res.put(new Head_strs(grouping_value), count);
      }
    }
    
    
//    return res;
  }
  
  public void start () {
//     System.out.println("Starting " +  threadName );
     if (t == null) {
        t = new Thread (this, threadName);
        t.start ();
     }
     else
       t.start();
  }
  
  public void join() throws InterruptedException
  {
    t.join();
  }
}