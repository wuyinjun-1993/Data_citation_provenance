package edu.upenn.cis.citation.query_view_generators;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.views.Query_converter;

public class Query_view_generator_synthetic_random {

  static String directory = "synthetic_example/";
  
//  static String directory = "./";
  
  static String query_file = directory + "query";
  
  static String view_file = directory + "views";
  
  public static void main(String [] args) throws ClassNotFoundException, SQLException
  {
    
    Connection c = null;
    PreparedStatement pst = null;
    
  String db = args[0];

  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url_prefix + db, init.usr_name , init.passwd);
  
//  output(c, pst);
  
  boolean new_query = Boolean.valueOf(args[1]);
  
  int query_subgoal_num = Integer.valueOf(args[2]);
  
  int query_grouping_attr_num = Integer.valueOf(args[3]);
  
  int query_agg_attr_num = Integer.valueOf(args[4]);
  
  int view_num = Integer.valueOf(args[5]);
  
  int view_offset = Integer.valueOf(args[6]);
  
  int instance_size = Integer.valueOf(args[7]);
  
  int view_instance_size = Integer.valueOf(args[8]);
  
  query_generator.query_result_size = instance_size;
  
  view_generator.view_instance_size = view_instance_size;
  
  if(new_query)
  {
    Query query = query_generator.generate_query(query_subgoal_num, query_grouping_attr_num, query_agg_attr_num, c, pst);
    String string = Query_converter.datalog2sql(query, false);
//    print_query_result(string, c, pst);
    Vector<Query> queries = new Vector<Query>();
    queries.add(query);
    
    Vector<String> query_strings = Load_views_and_citation_queries.views2text_strings(queries);
    Load_views_and_citation_queries.write2files(query_file, query_strings);
    Vector<Query> views = view_generator.gen_views_random(false, gen_unique_subgoal_names(query), query, view_num, query.body.size(),0, c, pst);
    view_generator.store_views_with_citation_queries(views, view_file);
    
  }
//  else
//  {
//    query_generator.build_relation_primary_key_mapping(c, pst);
//    
//    query_generator.init_parameterizable_attributes(c, pst);
//   
//    Query query = Load_views_and_citation_queries.get_query_test_case(query_file, c, pst).get(0);
//    
//    query = query_generator.update_instance_size(query, c, pst);
//    
//    Vector<Query> queries = new Vector<Query>();
//    queries.add(query);
//    
//    Vector<String> query_strings = Load_views_and_citation_queries.views2text_strings(queries);
//    
//    Load_views_and_citation_queries.write2files(query_file, query_strings);
//    
//    Vector<Query> views = Load_views_and_citation_queries.get_query_test_case(view_file, c, pst);
//    
//    Vector<Query> updated_views = view_generator.update_instance_size(views, c, pst);
//    
//    view_generator.store_views_with_citation_queries(updated_views, view_file);
//  }
  else
  {
    Query query = query_generator.generate_query(3, query_grouping_attr_num, query_agg_attr_num, c, pst);
    
//  Vector<Query> queries = new Vector<Query>();
//  queries.add(query);
//  
//  Vector<String> query_strings = Load_views_and_citation_queries.views2text_strings(queries);
//Load_views_and_citation_queries.write2files("query", query_strings);
  query = Load_views_and_citation_queries.get_query_test_case(query_file, c, pst).get(0);
  
//  Vector<String> parameters = new Vector<String>();
//  parameters.add(query.lambda_term.get(0).toString());
//  Load_views_and_citation_queries.write2files("user_query_subsets", parameters);
  Vector<Query> views = view_generator.gen_views_random(false, gen_unique_subgoal_names(query), query, view_num, query.body.size(),view_offset,  c, pst);
//  view_generator.store_views_with_citation_queries(views);
  view_generator.append_views_with_citation_queries(views, view_offset, view_file);
  System.out.println(query);

  }
  
  c.close();
}
  
  static void print_query_result(String sql, Connection c, PreparedStatement pst) throws SQLException
  {
    pst = c.prepareStatement(sql);
    
    ResultSet rs = pst.executeQuery();
    
    int count = 0;
    while(rs.next())
    {
      String value = rs.getString(1);
      
      count++;
    }
    System.out.println("query_instance_size::" + count);
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
