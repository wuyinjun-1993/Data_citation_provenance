package edu.upenn.cis.citation.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import edu.upenn.cis.citation.init.init;

public class test_sql {
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException
  {
    String sql1 = "select * from r,t where (r.a, t.e) in (select * from unnest(Array[1,2, 3], Array[1,3,4]))";
    
    String sql2 = "select * from r,t where (r.a, t.e) = any(values(1,1), (2,3), (3,4))";
    
    String sql3 = "select * from r,t where (r.a, t.e) in (values(1,1), (2,3), (3,4))";
    
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
  int round = 50000;
  
  long t1 = System.nanoTime();
  
  for(int i = 0; i<round; i++)
  {
    pst = c.prepareStatement(sql1);
    
    pst.executeQuery();
  }
  
  long t2 = System.nanoTime();
  
  double time1 = (t2 - t1)*1.0/1000000000;
  
  t1 = System.nanoTime();
  
  for(int i = 0; i<round; i++)
  {
    pst = c.prepareStatement(sql2);
    
    pst.executeQuery();
  }
  
  t2 = System.nanoTime();
  
  double time2 = (t2 - t1)*1.0/1000000000;
  
  
  t1 = System.nanoTime();
  
  for(int i = 0; i<round; i++)
  {
    pst = c.prepareStatement(sql3);
    
    pst.executeQuery();
  }
  
  t2 = System.nanoTime();
  
  double time3 = (t2 - t1)*1.0/1000000000;
  
  System.out.println(time1);
  
  System.out.println(time2);
  
  System.out.println(time3);
  
  }

}
