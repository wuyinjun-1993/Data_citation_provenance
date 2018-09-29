package edu.upenn.cis.citation.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
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
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONException;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view1.Covering_set;
import edu.upenn.cis.citation.citation_view1.Head_strs;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning3;
import edu.upenn.cis.citation.prov_reasoning.Prov_reasoning4;
import edu.upenn.cis.citation.query.Query_provenance;
import edu.upenn.cis.citation.user_query.query_storage;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class provenance_citation {
  
//  static String path = "/home/wuyinjun/workspace/Data_citation_demo/reasoning_results/";
  
  static String path = "reasoning_results/";
  
  public static Vector<Head_strs> tuple_why_prov_mappings = new Vector<Head_strs>();
  
  public static ArrayList<Vector<Head_strs>> all_why_tokens = new ArrayList<Vector<Head_strs>>();

  
  public static void main(String [] args) throws SQLException, ClassNotFoundException, IOException, InterruptedException, JSONException
  {
//    Query query = load_query();
    
//    test_query_time();
    
    use_reasoning4(args);
//    use_reasoning3(args, query);
    
  }
  
  static void test_query_time() throws ClassNotFoundException, SQLException
  {
    Class.forName("org.postgresql.Driver");
    Connection c = DriverManager
        .getConnection(init.db_url, init.usr_name , init.passwd);
    
    PreparedStatement pst = null;
    
    Query query = Load_views_and_citation_queries.get_query_test_case(Query_provenance.query_file, c, pst).get(0);
    
    
    double t1 = System.nanoTime();

    String sql = Query_converter.datalog2sql(query, false);
    
    pst = c.prepareStatement(sql);
    
    pst.executeQuery();
    
    double t2 = System.nanoTime();
    
    double query_time = (t2 - t1)*1.0/1000000000;
    
    System.out.println("query_time::" + query_time);
    
    System.out.println(query);
    
    System.out.println(sql);
    
    c.close();
    
    
    
  }
  
  
  static Query load_query() throws SQLException, ClassNotFoundException
  {
    
    Connection c = null;
    
    Class.forName("org.postgresql.Driver");
    c = DriverManager
        .getConnection(init.db_url, init.usr_name , init.passwd);
    
    PreparedStatement pst = null;
    
    Query query = Load_views_and_citation_queries.get_views("query", c, pst).get(0);
//    Query query = query_storage.get_query_by_id(1, c, pst);
    
    c.close();
    
    return query;

  }
  
  static void reasoning_with_covering_set_opt(Query query, boolean iscluster, int thread_num, boolean sortcluster, boolean is_materialized, Connection c, PreparedStatement pst) throws SQLException, IOException, InterruptedException, JSONException
  {
    Prov_reasoning4.batch_size = thread_num;
    
//    int factor = 1;//Integer.valueOf(args[1]);
    
//    Prov_reasoning4.factor = factor;
    
    Prov_reasoning4.sort_cluster = sortcluster;
    
    Prov_reasoning4.init(c, pst);
  
//    Prov_reasoning4.init_from_database(c, pst);
    
    Prov_reasoning4.init_from_files(is_materialized, c, pst);
    
    double start = 0;
    
    double end = 0;
    
    Vector<String[]> provenance_instances = Query_provenance.get_provenance_instance(query);
    
    start = System.nanoTime();
    
    ConcurrentHashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
    
    HashSet<Covering_set> covering_sets = Prov_reasoning4.reasoning(query, curr_valid_view_mappings, iscluster, is_materialized, provenance_instances, c, pst);

    end = System.nanoTime();
    
    double time = (end - start)*1.0/1000000000;
    
    if(iscluster)
    {
      System.out.println("reasoning time 3:" + time);
      
      System.out.println("view_mapping_time 3:" + Prov_reasoning4.view_mapping_time);
      
      System.out.println("covering_set_time 3:" + Prov_reasoning4.covering_set_time);

    }
    else
    {
      System.out.println("reasoning time 4:" + time);
      
      System.out.println("view_mapping_time 4:" + Prov_reasoning4.view_mapping_time);
      
      System.out.println("covering_set_time 4:" + Prov_reasoning4.covering_set_time);

    }
    
    double t1 = System.nanoTime();
    
    String sql = Query_converter.datalog2sql(query, false);
    
    pst = c.prepareStatement(sql);
    
//    System.out.println(sql);
    
    pst.executeQuery();
    
    double t2 = System.nanoTime();
    
    double query_time = (t2 - t1)*1.0/1000000000;
    
    System.out.println("query_time::" + query_time);
    
    
    Set<Tuple> view_mappings = Prov_reasoning4.tuple_valid_rows.keySet();
    
    
    System.out.println("view_mapping_num::" + view_mappings.size());
    
    
    if(is_materialized)
      write2file_view_mappings(path + "view_mapping_rows1", Prov_reasoning4.tuple_valid_rows);
    else
      write2file_view_mappings(path + "view_mapping_rows2", Prov_reasoning4.tuple_valid_rows);

//    System.out.println(covering_sets);
    
    System.out.println("Covering_set_size::" + covering_sets.size());
    
//    System.out.println("Covering_set::" + covering_sets);
    
    System.out.println("Group_num::" + Prov_reasoning4.group_view_mappings.size());
    
    Set<String> group_ids = Prov_reasoning4.group_covering_sets.keySet();
    
    for(String group_id: group_ids)
    {
      ConcurrentHashMap<Tuple, Integer> view_mapping_ids = Prov_reasoning4.group_view_mappings.get(group_id);
      
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
      write2file(path + "covering_sets1", covering_sets);
    else
      write2file(path + "covering_sets2", covering_sets);
    
    HashSet<String> formatted_citations = Prov_reasoning4.gen_citations(curr_valid_view_mappings, covering_sets, c, pst);
    
    if(is_materialized)
      write2file(path + "citation1", formatted_citations);
    else    
      write2file(path + "citation2", formatted_citations);
    
    if(is_materialized)
      write2file(path + "covering_sets_per_group1", Prov_reasoning4.group_covering_sets);
    else
      write2file(path + "covering_sets_per_group2", Prov_reasoning4.group_covering_sets);
    
  }
  
  static void reasoning_without_covering_set_opt(Query query, boolean iscluster, int thread_num, boolean sortcluster, boolean is_materialized, Connection c, PreparedStatement pst) throws SQLException, IOException, JSONException, InterruptedException
  {

    Prov_reasoning3.batch_size = thread_num;
    
//    int factor = 2;//Integer.valueOf(args[1]);
//    
//    Prov_reasoning3.factor = factor;
    
    Prov_reasoning3.sort_cluster = sortcluster;
    
    Prov_reasoning3.init();
  
//    Prov_reasoning4.init_from_database(c, pst);
    
    Prov_reasoning3.init_from_files(c, pst);
    
    double start = 0;
    
    double end = 0;
    
    Vector<String[]> provenance_instances = Query_provenance.get_provenance_instance(query);
    
    start = System.nanoTime();
    
    ConcurrentHashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
    
    HashSet<edu.upenn.cis.citation.citation_view0.Covering_set> covering_sets = Prov_reasoning3.reasoning(query, curr_valid_view_mappings, iscluster, provenance_instances, c, pst);

    end = System.nanoTime();
    
    double time = (end - start)*1.0/1000000000;
    
    if(iscluster)
    {
      System.out.println("reasoning time 3:" + time);
      
      System.out.println("view_mapping_time 3:" + Prov_reasoning3.view_mapping_time);
      
      System.out.println("covering_set_time 3:" + Prov_reasoning3.covering_set_time);

    }
    else
    {
      System.out.println("reasoning time 4:" + time);
      
      System.out.println("view_mapping_time 4:" + Prov_reasoning3.view_mapping_time);
      
      System.out.println("covering_set_time 4:" + Prov_reasoning3.covering_set_time);

    }
    
    double t1 = System.nanoTime();
    
    String sql = Query_converter.datalog2sql(query, false);
    
    pst = c.prepareStatement(sql);
    
    pst.executeQuery();
    
    double t2 = System.nanoTime();
    
    double query_time = (t2 - t1)*1.0/1000000000;
    
    System.out.println("query_time::" + query_time);
    
    
    Set<Tuple> view_mappings = Prov_reasoning3.tuple_valid_rows.keySet();
    
    write2file_view_mappings(path + "view_mapping_rows2", Prov_reasoning3.tuple_valid_rows);

    System.out.println(covering_sets);
    
    System.out.println("Covering_set_size::" + covering_sets.size());
    
    if(iscluster)
      write2file(path + "covering_sets3", covering_sets);
    else
      write2file(path + "covering_sets4", covering_sets);
    
    HashSet<String> formatted_citations = Prov_reasoning3.gen_citations(curr_valid_view_mappings, covering_sets, c, pst);
    
    write2file(path + "citation2", formatted_citations);
    
    write2file(path + "covering_sets_per_group2", Prov_reasoning4.group_covering_sets);
    
  
  }
  
  static void use_reasoning4(String [] args) throws ClassNotFoundException, SQLException, IOException, InterruptedException, JSONException
  {
    Connection c = null;
    PreparedStatement pst = null;
    
    boolean iscluster = Boolean.valueOf(args[0]);
    
    boolean covering_set_opt = Boolean.valueOf(args[1]);
    
    boolean is_materialized = Boolean.valueOf(args[2]);
    
    boolean sortcluster = true;//Boolean.valueOf(args[1]);
    
    int thread_num = 5;//Integer.valueOf(args[1]);
    
    int qid = 0;
    
    String db_name = args[3];
    
    Prov_reasoning4.db_name = db_name;
    
    Prov_reasoning3.db_name = db_name;

    Class.forName("org.postgresql.Driver");
    c = DriverManager
        .getConnection(init.db_url_prefix + db_name, init.usr_name , init.passwd);
    
    if(args.length > 4)
    {
      Query_provenance.query_file = args[4];
      
      Query_provenance.view_file = args[5];
      
      Query_provenance.sql_result_file = args[6];
      
      if(args.length > 7)
        qid = Integer.valueOf(args[7]);
    }
    
    Query query = Load_views_and_citation_queries.get_query_test_case(Query_provenance.query_file, c, pst).get(qid);
    
    System.out.println(query);
    
    if(covering_set_opt)
    {
      reasoning_with_covering_set_opt(query, iscluster, thread_num, sortcluster, is_materialized, c, pst);
    }
    else
    {
      reasoning_without_covering_set_opt(query, iscluster, thread_num, sortcluster, is_materialized, c, pst);
    }
    
    if(is_materialized)
    {
      String view_instance_size_mappings = get_materialized_view_size(c, pst);
      
      System.out.println("view_instance_size_mappings::" + view_instance_size_mappings);
    }
    
    c.close();
  }
  
  static Head_strs get_query_result(ResultSet rs, int head_arg_size) throws SQLException
  {
    Vector<String> values = new Vector<String>();
    
    for(int i = 0; i<head_arg_size; i++)
    {
      String value = rs.getString(i + 1);
      
      values.add(value);
    }
    
    Head_strs curr_query_result = new Head_strs(values);
    
    return curr_query_result;
  }
  
  static Vector<Head_strs> get_tuples(ResultSet rs, Query query) throws SQLException
  {
    Vector<Head_strs> curr_tuples = new Vector<Head_strs>();
    
    Vector<String> provenance = new Vector<String>();
    
//    int total_col_count = provenance_row.length;
    
    int col_nums = query.head.size();
    
    for(int i = 0; i<query.body.size(); i++)
    {
      provenance.clear();
      
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      for(int j = 0; j<subgoal.args.size(); j++)
      {
        provenance.add(rs.getString(col_nums + 1));
        
        col_nums++;
      }
      
      Head_strs curr_tuple = new Head_strs(provenance);
      
      curr_tuples.add(curr_tuple);
    }
    
//    System.out.println(total_col_count + "::" + col_nums);
    
    return curr_tuples;
    
  }
  
  private static void printResult(ResultSet rs, Query query) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
    
    int rows = 0;
    
    while(rs.next())
    {
      
      Head_strs values = get_query_result(rs, query.head.size());
      
      Vector<Head_strs> curr_tuples = get_tuples(rs, query);
      
      System.out.println(rows);
//      
//      System.out.println(Runtime.getRuntime().totalMemory());
//      
//      System.out.println(Runtime.getRuntime().freeMemory());
//      
//      System.out.println(curr_tuples);
      
      
      
//      if(tuple_why_prov_mappings.get(values) == null)
//      {
//        ArrayList<Integer> curr_tokens = new ArrayList<Integer>();
//        
//        curr_tokens.add(rows);
//        
//        tuple_why_prov_mappings.add(values);
//        
////        System.out.println(values + "::" + curr_tokens);
//        
//      }
//      else
      {
        tuple_why_prov_mappings.add(values);
        
//        System.out.println(values + "::" + tuple_why_prov_mappings.get(values));
      }
      
      all_why_tokens.add(curr_tuples);
      
      rows ++;
      
      
    }
    
    
    
    
  }
  
  
  static String get_materialized_view_size(Connection c, PreparedStatement pst) throws SQLException
  {
      String query = "SELECT relname AS objectname "
//          + ", relkind AS objecttype"
//          + ", reltuples AS entries"
          + ", pg_size_pretty(pg_table_size(oid)) "
          + "AS size FROM pg_class"
          + " WHERE  relkind IN ('m') ORDER  BY pg_table_size(oid) DESC";
      
      pst = c.prepareStatement(query);
      
      ResultSet rs = pst.executeQuery();
      
      String result = new String();
      
      int num = 0;
      
      while(rs.next())
      {
        if(num >= 1)
          result += ",";
        
        result += rs.getString(1) + "::" + rs.getString(2);
        
      }
      
      return result;
  }
  
  static void use_reasoning3(String [] args, Query query) throws ClassNotFoundException, SQLException, IOException, InterruptedException, JSONException
  {
    PreparedStatement pst = null;
    
    boolean iscluster = Boolean.valueOf(args[0]);
    
    boolean sortcluster = false;//Boolean.valueOf(args[1]);
    
//    int factor = 2;//Integer.valueOf(args[1]);
    
    Query_provenance.connect(init.db_prov_url, init.usr_name, init.passwd);
    
    Prov_reasoning4.test_case = false;
    
//    Query query = query_storage.get_query_by_id(1, Query_provenance.con, pst);
    
    ResultSet rs = Query_provenance.get_provenance4query(query, Prov_reasoning4.test_case);
    
//    printResult(rs, query);
    
    
    
    Connection c = null;
    
  Class.forName("org.postgresql.Driver");
  c = DriverManager
      .getConnection(init.db_url, init.usr_name , init.passwd);
  
//    Prov_reasoning4.factor = factor;
    
    Prov_reasoning4.sort_cluster = sortcluster;
    
    Prov_reasoning4.init(c, pst);
  
    Prov_reasoning4.init_from_files(false, c, pst);//(c, pst);
    
    double start = 0;
    
    double end = 0;
    
    start = System.nanoTime();
    
    ConcurrentHashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = new ConcurrentHashMap<Single_view, HashSet<Tuple>>();
    
//    HashSet<citation_view_vector> covering_sets = new HashSet<citation_view_vector>();
    
    HashSet<Covering_set> covering_sets = Prov_reasoning4.reasoning(query, curr_valid_view_mappings, iscluster, false, rs, c, pst);

    end = System.nanoTime();
    
    double time = (end - start)*1.0/1000000000;
    
    if(iscluster)
    {
      System.out.println("reasoning time 3:" + time);
      
      System.out.println("view_mapping_time 3:" + Prov_reasoning4.view_mapping_time);
      
      System.out.println("covering_set_time 3:" + Prov_reasoning4.covering_set_time);

    }
    else
    {
      System.out.println("reasoning time 4:" + time);
      
      System.out.println("view_mapping_time 4:" + Prov_reasoning4.view_mapping_time);
      
      System.out.println("covering_set_time 4:" + Prov_reasoning4.covering_set_time);

    }
    Set<Tuple> view_mappings = Prov_reasoning4.tuple_valid_rows.keySet();
    
    write2file_view_mappings(path + "view_mapping_rows2", Prov_reasoning4.tuple_valid_rows);

    if(iscluster)
      write2file(path + "covering_sets3", covering_sets);
    else
      write2file(path + "covering_sets4", covering_sets);
    
    HashSet<String> formatted_citations = Prov_reasoning4.gen_citations(curr_valid_view_mappings, covering_sets, c, pst);
    
    write2file(path + "citation2", formatted_citations);
    
    write2file(path + "covering_sets_per_group2", Prov_reasoning4.group_covering_sets);
    
    Query_provenance.reset();
    
    c.close();
  }
  
  
  static void output_view_mapping_valid_rids()
  {
    Set<Tuple> tuples = Prov_reasoning4.tuple_valid_rows.keySet();
    
    for(Tuple tuple: tuples)
    {
      System.out.print(tuple.name + "   " + Prov_reasoning4.tuple_valid_rows.get(tuple).size());
      
      System.out.println();
    }
  }
  
  static void output_view_mappings_per_group()
  {
    Set<String> strings = Prov_reasoning4.group_view_mappings.keySet();
    
    int num = 0;
    
    for(String string: strings)
    {
      System.out.println("group" + num);
      
      ConcurrentHashMap<Tuple, Integer> tuple_indexes = Prov_reasoning4.group_view_mappings.get(string);
      
      Set<Tuple> tuples = tuple_indexes.keySet();
      
      String[] tuple_strs = new String[tuples.size()];
      
      int id = 0;
      
      for(Tuple tuple: tuples)
      {
        tuple_strs[id++] = tuple.name;
        
      }

      Arrays.sort(tuple_strs);
      
      for(String tuple_str: tuple_strs)
      {
        System.out.print(tuple_str + "  ");
      }
      
      System.out.println();
      
      num++;
      
    }
  }
  
  static String get_sorted_mapping_string(Tuple view_mapping, HashMap<String, String> subgoal_name_mappings)
  {
    Set<String> subgoal_names =  view_mapping.mapSubgoals_str.keySet(); 
    
    Vector<String> subgoal_name_list = new Vector<String>(subgoal_names);
    
    Collections.sort(subgoal_name_list);
    
    String sorted_mapping_string = new String();
    
    int count = 0;
    
    for(String subgoal_name: subgoal_name_list)
    {
      if(count >= 1)
        sorted_mapping_string += ",";
      
      sorted_mapping_string += subgoal_name + "=" + subgoal_name_mappings.get(subgoal_name);
      
      count ++;
    }
    
    return sorted_mapping_string;
    
  }
  
  static void write2file_view_mappings(String file_name, ConcurrentHashMap<Tuple, HashSet> view_mapping_count) throws IOException
  {
      File fout = new File(file_name);
      FileOutputStream fos = new FileOutputStream(fout);
   
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
   
      Set<Tuple> view_mappings = view_mapping_count.keySet();
      
      for(Tuple view_mapping: view_mappings)
      {
        int count = view_mapping_count.get(view_mapping).size();
        
        String sorted_mapping_string = get_sorted_mapping_string(view_mapping, view_mapping.mapSubgoals_str);
        
        String view_mapping_str = view_mapping.query.view_name + "|" + sorted_mapping_string + ":" + count;
        
        if(count == 0)
          continue;
        
//        System.out.println(view_mapping_str);
        
        bw.write(view_mapping_str);
        
        bw.newLine();
      }
   
      bw.close();
  }
  
  public static void write2file(String file_name, ConcurrentHashMap<String, HashSet<Covering_set>> views) throws IOException
  {
    File fout = new File(file_name);
    FileOutputStream fos = new FileOutputStream(fout);
 
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
 
    Set<String> group_label = views.keySet();
    
    int num = 0;
    
    for(String label: group_label)
    {
      bw.write("group " + num);
      bw.newLine();
      
      HashSet<Covering_set> covering_sets = views.get(label);
      
      String [] covering_set_string = new String [covering_sets.size()];
      
      int id = 0;
      
      for(Covering_set covering_set: covering_sets)
      {
        covering_set_string[id ++] = covering_set.toString(); 
      }
      
      Arrays.sort(covering_set_string);
      
      for(String covering_set_str: covering_set_string)
      {
        bw.write(covering_set_str);
        bw.newLine();
      }
      
      num++;
      
    }
    
    bw.close();

  }
  
  public static void write2file(String file_name, HashSet views) throws IOException
  {
      File fout = new File(file_name);
      FileOutputStream fos = new FileOutputStream(fout);
   
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
   
      if(views != null && !views.isEmpty())
      for (Object view: views) {
          bw.write(view.toString());
          bw.newLine();
      }
   
      bw.close();
  }

}
