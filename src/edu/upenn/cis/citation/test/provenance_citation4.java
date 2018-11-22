package edu.upenn.cis.citation.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import org.json.JSONException;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view1.Covering_set;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning3;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.query.Build_query_prov_index;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.query.Query_provenance_2;
import edu.upenn.cis.citation.user_query.query_storage;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class provenance_citation4 {
  
//  static String path = "/home/wuyinjun/workspace/Data_citation_demo/reasoning_results/";
  
  static String path = "reasoning_results/";
  
  public static void main(String [] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException, JSONException
  {
    Connection c = null;
    PreparedStatement pst = null;
    
    boolean iscluster = Boolean.valueOf(args[0]);
    
    boolean covering_set_opt = Boolean.valueOf(args[1]);
    
    boolean is_materialized = Boolean.valueOf(args[2]);
    
    boolean test_case = Boolean.valueOf(args[3]);
    
    boolean sortcluster = true;//Boolean.valueOf(args[1]);
    
    int thread_num = 5;//Integer.valueOf(args[1]);
    
    int qid = 0;
    
    String db_name = args[4];
    
    Prov_reasoning.db_name = db_name;
    
    Class.forName("org.postgresql.Driver");
    c = DriverManager
        .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
    
    if(args.length > 5)
    {
      Query_provenance.query_file = args[5];
      
      Query_provenance.view_file = args[6];
      
      Query_provenance.sql_result_file = args[7];
      
      if(args.length > 8)
        qid = Integer.valueOf(args[8]);
    }
    
    Prov_reasoning.init(c, pst);
    
    Prov_reasoning.init_from_files(is_materialized, c, pst);
    
    Query query = Load_views_and_citation_queries.get_query_test_case(Query_provenance.query_file, c, pst).get(qid);
    
    HashMap<String, String[][]> grouping_values_prov_mappings = new HashMap<String, String[][]>();
    
    HashMap<String, Head_strs> query_instance = new HashMap<String, Head_strs>();
    
    HashMap<String, Integer> query_group_value_initial_count = new HashMap<String, Integer>();
    
//    HashMap<String, HashSet<Integer>>[] query_prov_index = new HashMap[query.body.size()];
    
    HashMap<String, HashMap<String, long[]>>[] query_prov_index = new HashMap[query.body.size()];
    
//    Vector<String>[] query_prov_lists = new Vector[query.body.size()];
//    
//    Vector<long[]>[] prov_index_lists = new Vector[query.body.size()];
    
    HashMap<String, Integer> grouping_value_prov_count_mappings = new HashMap<String, Integer>();
    
    for(int i = 0; i<query.body.size(); i++)
    {
      query_prov_index[i] = new HashMap<String, HashMap<String, long[]>>(); 
//      query_prov_lists[i] = new Vector<String>();
//      
//      prov_index_lists[i] = new Vector<long[]>();
    }
    
    HashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
    
    double t1 = System.nanoTime();
    
    double t11 = System.nanoTime();
    
    ResultSet rs = Query_provenance_2.get_query_provenance(query, test_case, c, pst);
    
//    Query_provenance_2.retrieve_query_instance_provenance2(rs, query, grouping_value_prov_count_mappings, grouping_values_prov_mappings, query_instance, query_prov_index);
    
    Query_provenance_2.retrieve_query_instance_provenance3(rs, query, grouping_value_prov_count_mappings, grouping_values_prov_mappings, query_instance, query_prov_index, query_group_value_initial_count);
    
    
    double t22 = System.nanoTime();
//    Query_provenance_2.retrieve_query_instance_provenance4(rs, query, grouping_value_prov_count_mappings, grouping_values_prov_mappings, query_instance, query_prov_lists, prov_index_lists);
    
//    Build_query_prov_index.print_query_index(query_prov_index);
    
    HashMap<String, String[]>[] base_relation_contents = Query_provenance_2.retrieve_base_relations_multi_thread(db_name, query_prov_index, query, c, pst);
    
    double t33 = System.nanoTime();
    
    HashSet<Covering_set> covering_sets = Prov_reasoning.reasoning(query, curr_valid_view_mappings, iscluster, is_materialized, grouping_value_prov_count_mappings, grouping_values_prov_mappings, query_prov_index, query_group_value_initial_count, base_relation_contents, c, pst);
    
    
    double t2 = System.nanoTime();
    
    double time = (t2 - t1)*1.0/1000000000;
    
    double prov_query_time = (t22 - t11)*1.0/1000000000;
    
    
    
    String sql = Query_converter.datalog2sql(query, false);
    
    pst = c.prepareStatement(sql);
    
    t1 = System.nanoTime();
    ResultSet rs2 = pst.executeQuery();
    
    Query_provenance_2.retrieve_query_instance(rs2, query, query_instance);
    
    t2 = System.nanoTime();
    
    double query_time = (t2 - t1)*1.0/1000000000;
    
    double load_content_time = (t33 - t22)*1.0/1000000000;
    
    System.out.println("time::" + time);
    System.out.println("query_time::" + query_time);
    System.out.println("prov_query_time::" + prov_query_time);
    System.out.println("load_content_time::" + load_content_time);
    System.out.println("view_mapping_size::" + Prov_reasoning.valid_view_mappings_schema_level.size());
    
    if(is_materialized)
      provenance_citation.write2file_view_mappings(path + "view_mapping_rows3", Prov_reasoning.tuple_valid_rows);
    else
      provenance_citation.write2file_view_mappings(path + "view_mapping_rows4", Prov_reasoning.tuple_valid_rows);

//    System.out.println(covering_sets);
    
    System.out.println("Covering_set_size::" + covering_sets.size());
    
//    System.out.println("Covering_set::" + covering_sets);
    
    System.out.println("Group_num::" + Prov_reasoning.group_view_mappings.size());
    
    Set<String> group_ids = Prov_reasoning.group_covering_sets.keySet();
    
    for(String group_id: group_ids)
    {
      HashMap<Tuple, Integer> view_mapping_ids = Prov_reasoning.group_view_mappings.get(group_id);
      
      Set<Tuple> curr_view_mappings = view_mapping_ids.keySet();
      
      Vector<String> view_mapping_strings = new Vector<String>();
      
      for(Tuple view_mapping: curr_view_mappings)
      {
        view_mapping_strings.add(view_mapping.name);
        
//      System.out.print(view_mapping.name + "   ");
      }
      
      Collections.sort(view_mapping_strings);

      System.out.println(view_mapping_strings.size());
      
      for(String view_mapping: view_mapping_strings)
      {
        System.out.print(view_mapping + "   ");
      }
      
      System.out.println();
    }
    
    
    if(is_materialized)
      write2file(path + "covering_sets3", covering_sets);
    else
      write2file(path + "covering_sets4", covering_sets);
    
    HashSet<String> formatted_citations = Prov_reasoning.gen_citations(curr_valid_view_mappings, covering_sets, c, pst);
    
    if(is_materialized)
      write2file(path + "citation3", formatted_citations);
    else    
      write2file(path + "citation4", formatted_citations);
    
    if(is_materialized)
      provenance_citation.write2file(path + "covering_sets_per_group3", Prov_reasoning.group_covering_sets);
    else
      provenance_citation.write2file(path + "covering_sets_per_group4", Prov_reasoning.group_covering_sets);
    
//    output_valid_view_mappings(Prov_reasoning.valid_view_mappings_schema_level);
    
    
//    System.out.println(Prov_reasoning.valid_view_mappings_schema_level);
    System.out.println(covering_sets.size());
    
  }
  
  public static void output_valid_view_mappings(ArrayList<HashSet<Tuple>> valid_view_mappings)
  {
    for(int i = 0; i<valid_view_mappings.size(); i++)
    {
      HashSet<Tuple> curr_view_mapping_set = valid_view_mappings.get(i);
      
      System.out.println(i);
      
      for(Tuple view_mapping: curr_view_mapping_set)
      {
        System.out.print(view_mapping.name + ",");
      }
      
      System.out.println();
    }
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
