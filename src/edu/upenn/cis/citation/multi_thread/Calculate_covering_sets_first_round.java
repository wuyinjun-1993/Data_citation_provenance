package edu.upenn.cis.citation.multi_thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view.citation_view_vector;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.views.Single_view;

public class Calculate_covering_sets_first_round implements Runnable {
  private Thread t;
//private String threadName;

//HashSet<citation_view_vector> covering_set1;
//
//HashSet<citation_view_vector> covering_set2;

ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var;

int start;

int end;

HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();

Vector<Argument> args;

public HashSet<citation_view_vector> get_reasoning_result()
{
  return view_com;
}

//public Calculate_covering_sets(HashSet<citation_view_vector> covering_set1, HashSet<citation_view_vector> covering_set2) {
//  
//  this.covering_set1 = covering_set1;
//  
//  this.covering_set2 = covering_set2;
//   
//}

public Calculate_covering_sets_first_round(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args, int k, int i) {
  // TODO Auto-generated constructor stub
  this.valid_view_mappings_per_head_var = valid_view_mappings_per_head_var;
  
  start = k;
  
  end = i;
  
  this.args = args;
}

public void run() {
//   System.out.println("Running " +  threadName );
//   try {
//      for(int i = 4; i > 0; i--) {
//         System.out.println("Thread: " + threadName + ", " + i);
//         // Let the thread sleep for a while.
//         Thread.sleep(50);
//      }
//   } catch (InterruptedException e) {
//      System.out.println("Thread " +  threadName + " interrupted.");
//   }
//   System.out.println("Thread " +  threadName + " exiting.");
  
//  start = j;
//  
//  end = j+gap*i;
  
  for(int k = start; k<end && k < valid_view_mappings_per_head_var.size(); k++)
  {
    HashMap<Single_view, HashSet<Tuple>> valid_view_mappings = valid_view_mappings_per_head_var.get(k);
    
    Set<Single_view> views = valid_view_mappings.keySet();
    
    HashSet<Tuple> all_tuples = new HashSet<Tuple>();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = valid_view_mappings.get(view);
      
      all_tuples.addAll(tuples);
      
      
    }
    
    view_com = Prov_reasoning2.join_views_curr_relation(all_tuples, view_com, args);
  
  }
  
//  for(Iterator iter2 = view_mappings.iterator(); iter2.hasNext();)
//  {
////    view.reset_values();
//    
//    Tuple tuple = (Tuple) iter2.next();
//    
//    for(int i = 0; i<curr_tuples.size(); i++)
//    {
//      view.evaluate_args(curr_tuples.get(i), tuple);
//      
//      if(!view.check_validity(tuple))
//      {
//        iter2.remove();
//        break;
//      }
//    }
//    
//  }
  
  
}

public void start () {
//   System.out.println("Starting " +  threadName );
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
