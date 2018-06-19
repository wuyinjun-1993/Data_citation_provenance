package edu.upenn.cis.citation.multi_thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view1.Covering_set;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.views.Single_view;

public class Calculate_covering_sets implements Runnable {
  private Thread t;
//  private String threadName;
  
//  HashSet<citation_view_vector> covering_set1;
//  
//  HashSet<citation_view_vector> covering_set2;
  
  ArrayList<HashSet<Covering_set>> covering_sets;
  
  int start;
  
  int end;
  
  HashSet<Covering_set> resulting_covering_set;
  
  public HashSet<Covering_set> get_reasoning_result()
  {
    return resulting_covering_set;
  }
  
//  public Calculate_covering_sets(HashSet<citation_view_vector> covering_set1, HashSet<citation_view_vector> covering_set2) {
//    
//    this.covering_set1 = covering_set1;
//    
//    this.covering_set2 = covering_set2;
//     
//  }
  
  public Calculate_covering_sets(ArrayList<HashSet<Covering_set>> covering_sets, int k, int i) {
    // TODO Auto-generated constructor stub
    this.covering_sets = covering_sets;
    
    start = k;
    
    end = i;
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
    
    resulting_covering_set = covering_sets.get(start);
    
    for(int i = start + 1; i<end; i++)
    {
      resulting_covering_set = Prov_reasoning2.join_operation(resulting_covering_set, covering_sets.get(i));

    }
    
    
    
//    for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
//    {
////      view.reset_values();
//      
//      Tuple tuple = (Tuple) iter2.next();
//      
//      for(int i = 0; i<curr_tuples.size(); i++)
//      {
//        view.evaluate_args(curr_tuples.get(i), tuple);
//        
//        if(!view.check_validity(tuple))
//        {
//          iter2.remove();
//          break;
//        }
//      }
//      
//    }
    
    
  }
  
  public void start () {
//     System.out.println("Starting " +  threadName );
     if (t == null) {
        t = new Thread (this);
        t.start ();
     }
  }
  
  public void join() throws InterruptedException
  {
    t.join();
  }
  
}

