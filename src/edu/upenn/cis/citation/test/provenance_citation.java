package edu.upenn.cis.citation.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import org.json.JSONException;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view.citation_view_vector;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning2;
import edu.upenn.cis.citation.user_query.query_storage;
import edu.upenn.cis.citation.views.Single_view;

public class provenance_citation {
  
  static String path = "/home/wuyinjun/workspace/Data_citation_demo/reasoning_results/";
  
  public static void main(String [] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException, JSONException
  {
    
       
    Connection c = null;
    PreparedStatement pst = null;
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
  
  Connection c2 = DriverManager
      .getConnection(init.db_url2, init.usr_name , init.passwd);
    
//    Vector<Query> views = Load_views_and_citation_queries.get_views("views", c, pst);
//    
//    for(int i = 0; i<views.size(); i++)
//    {
//      Query view = views.get(i);
//      
//      Single_view view_obj = new Single_view(view, view.name, c, pst);
//      
//      view_objs.add(view_obj);
//    }
    
  boolean multi_thread = Boolean.valueOf(args[0]);
  
    Prov_reasoning2.init_from_database(c, pst);
    
//    Vector<Query> query = Load_views_and_citation_queries.get_views(path + "user_queries", c, pst);
    
    Query query = query_storage.get_query_by_id(1, c2, pst);
    
    
    HashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
    
    double start = 0;
    
    double end = 0;
    
    start = System.nanoTime();
    
    HashSet<citation_view_vector> covering_sets = Prov_reasoning2.reasoning(query, curr_valid_view_mappings, multi_thread, c, pst);
    
    end = System.nanoTime();
    
    double time = (end - start)*1.0/1000000000;
    
    if(multi_thread)
      System.out.println("reasoning time 2:" + time);
    else
      System.out.println("reasoning time 1:" + time);
    
    System.out.println("view_mapping_time:" + Prov_reasoning2.view_mapping_time);
    
    System.out.println("covering_set_time:" + Prov_reasoning2.covering_set_time);
    
    write2file(path + "covering_sets2", covering_sets);
    
//    System.out.println(covering_sets);
    
    System.out.println(Prov_reasoning2.rows);
    
    HashSet<String> formatted_citations = Prov_reasoning2.gen_citations(curr_valid_view_mappings, covering_sets, c, pst);
    
    write2file(path + "citation2", formatted_citations);
    
    
    c.close();
    
    c2.close();
  }
  
  public static void write2file(String file_name, HashSet views) throws IOException
  {
      File fout = new File(file_name);
      FileOutputStream fos = new FileOutputStream(fout);
   
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
   
      for (Object view: views) {
          bw.write(view.toString());
          bw.newLine();
      }
   
      bw.close();
  }

}
