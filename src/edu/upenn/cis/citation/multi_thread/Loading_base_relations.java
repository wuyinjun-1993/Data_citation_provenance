package edu.upenn.cis.citation.multi_thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.query.Query_provenance_2;
import edu.upenn.cis.citation.views.Query_converter;

public class Loading_base_relations implements Runnable{

  private Thread t;
  
  HashMap<String, HashMap<String, long[]>> query_prov_index;
  
  Query query;
  
  int i;
  
  String db_name;
  
  HashMap<String, String[]> base_relation_content;
  
  public HashMap<String, String[]> retrieve_base_relation_content()
  {
    return base_relation_content;
  }
  
  public void start () {
    System.out.println("Starting " +  i );
    if (t == null) {
       t = new Thread (this, String.valueOf(i));
       t.start ();
    }
    else
      t.start();
 }
  
  public Loading_base_relations(String db_name, HashMap<String, HashMap<String, long[]>> query_prov_index2, Query query, int i)
  {
    this.db_name = db_name;
    
    this.query = query;
    
    this.query_prov_index = query_prov_index2;
    
    this.i = i;
  }
  
//  @Override
  public void run() {
    // TODO Auto-generated method stub
    try {
      Class.forName("org.postgresql.Driver");
      Connection c = DriverManager
          .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
      
      PreparedStatement pst = null;
      load_base_relation_content(query_prov_index, query, i, c, pst);
    } catch (ClassNotFoundException | SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
  
  void load_base_relation_content(HashMap<String, HashMap<String, long[]>> query_prov_index2, Query query, int i, Connection c, PreparedStatement pst) throws SQLException
  {
    StringBuilder sb = new StringBuilder();
    
    Set<String> curr_query_prov_sets = query_prov_index2.keySet();
    
    Subgoal subgoal = (Subgoal) query.body.get(i);
    
    String sql = Query_converter.construct_query_base_relations(sb, curr_query_prov_sets, subgoal, query.subgoal_name_mapping.get(subgoal.name));
    
    base_relation_content = Query_provenance_2.construct_base_relation_sets(sql, subgoal, curr_query_prov_sets, c, pst);

  }
  
  public void join() throws InterruptedException
  {
    t.join();
  }

}
