package edu.upenn.cis.citation.multi_thread;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import edu.upenn.cis.citation.Corecover.Tuple;

public interface Check_valid_view_mappings extends Runnable{
  
  static String temp_table_name = "_temp";
  
  public ConcurrentHashMap get_tuple_rows();
  
  public void join() throws InterruptedException;
  
  public void start();

}
