package edu.upenn.cis.citation.multi_thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view.Head_strs;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.views.Single_view;

public class Check_valid_view_mappings implements Runnable {
  private Thread t;
  private String threadName;
  
  public Single_view view;
  
  public HashSet<Tuple> view_mappings;
  
  public ArrayList<Vector<Head_strs>> curr_tuples;
  
  public HashMap<Tuple, Vector<Integer>> tuple_rows = new HashMap<Tuple, Vector<Integer>>();
  
  public Connection c;
  
  public PreparedStatement pst;
  
  public HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings;
  
  public Check_valid_view_mappings( String name, Single_view view, HashSet<Tuple> view_mappings, ArrayList<Vector<Head_strs>> curr_tuples, HashMap<String, HashMap<String, Vector<Integer>>> rel_attr_value_mappings, Connection c, PreparedStatement pst) {
     threadName = name;

     this.view = view;
     
     this.view_mappings = view_mappings;
     
     this.curr_tuples = curr_tuples;
     
     this.rel_attr_value_mappings = rel_attr_value_mappings;
     
     this.c = c;
     
     this.pst = pst;
     
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
      
      Vector<Integer> row_ids = new Vector<Integer>();
      
      tuple_rows.put(tuple, row_ids);
      
      for(int i = 0; i<curr_tuples.size(); i++)
      {
        view.evaluate_args(curr_tuples.get(i), tuple);
        
        try {
          if(!view.check_validity(tuple, rel_attr_value_mappings, c, pst))
          {
            continue;
          }
          else
          {
            tuple_rows.get(tuple).add(i);
          }
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      
    }
    
    
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

