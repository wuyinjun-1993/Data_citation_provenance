package edu.upenn.cis.citation.stress_test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.query_view_generators.query_generator;
import edu.upenn.cis.citation.query_view_generators.view_generator;

public class stress_test {
  
  public static void main(String [] args) throws ClassNotFoundException, SQLException
  {
    Connection c = null;
    PreparedStatement pst = null;
    
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
    
    Query query = query_generator.generate_query(3, c, pst);
    Vector<Query> queries = new Vector<Query>();
    queries.add(query);
    
    Vector<String> query_strings = Load_views_and_citation_queries.views2text_strings(queries);
    Load_views_and_citation_queries.write2files("query", query_strings);
//    Vector<String> parameters = new Vector<String>();
//    parameters.add(query.lambda_term.get(0).toString());
//    Load_views_and_citation_queries.write2files("user_query_subsets", parameters);
    
    Vector<Query> views = view_generator.gen_views(gen_unique_subgoal_names(query), query, 5, query.body.size(), c, pst);
    view_generator.store_views_with_citation_queries(views);
    System.out.println(query);
  }
  
  static HashSet<String> gen_unique_subgoal_names(Query query)
  {
    HashSet<String> unique_subgoal_names = new HashSet<String>();
    for(Entry<String, String> mapping: query.subgoal_name_mapping.entrySet())
    {
      unique_subgoal_names.add(mapping.getValue());
    }
    return unique_subgoal_names;
  }

}
