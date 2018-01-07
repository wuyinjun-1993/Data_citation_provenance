package edu.upenn.cis.citation.prov_reasoning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Tuple;
import edu.upenn.cis.citation.citation_view.citation_view;
import edu.upenn.cis.citation.citation_view.citation_view_parametered;
import edu.upenn.cis.citation.citation_view.citation_view_unparametered;
import edu.upenn.cis.citation.citation_view.citation_view_vector;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.pre_processing.view_operation;
import edu.upenn.cis.citation.views.Query_converter;
import edu.upenn.cis.citation.views.Single_view;

public class Prov_reasoning {
  
  
  public static Vector<Single_view> view_objs = new Vector<Single_view>();
  
  public static String read_file()
  {
    String line = new String();
    
    try {
      File file = new File("string1");
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      StringBuffer stringBuffer = new StringBuffer();
      
      while ((line = bufferedReader.readLine()) != null) {
          stringBuffer.append(line);
      }
      fileReader.close();
      System.out.println("Contents of file:");
      
      line = stringBuffer.toString();
      System.out.println(stringBuffer.toString());
  } catch (IOException e) {
      e.printStackTrace();
  }
    
    return line;
  }
  
  public static String get_string()
  {
    String string = new String();
    
    for(int i = 0; i<1000000; i++)
    {
      string += "(family|family_id|" + i +",family|name|"+i+",family|"+i+")";
    }
    
    return string;
  }
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException
  {
    String string2 = "(family\\|family_id\\|114,.*?,family\\|114)";
    
    String string1 = get_string();
    
    double time1 = System.nanoTime();
    
    for(int i = 0; i<100000; i++)
    {
      string1.matches(string2);
    }
    
    double time2 = System.nanoTime();
    
    double time = (time2 - time1)/(1000000 * 100000);
    
    System.out.println(time);
    
    
    
//    Connection c = null;
//    PreparedStatement pst = null;
//  Class.forName("org.postgresql.Driver");
//  c = DriverManager
//      .getConnection(init.db_url, init.usr_name , init.passwd);
//    
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
//    
//    
//    Vector<Query> query = Load_views_and_citation_queries.get_views("query", c, pst);
//    
//    HashSet<citation_view_vector> covering_sets = reasoning(query.get(0), c, pst);
//    
//    System.out.println(covering_sets);
//    
//    c.close();
    
  }
  
  

  static void init(Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> views = get_all_views(c, pst);
        
    for(int i = 0; i<views.size(); i++)
    {
      Single_view curr_view_obj = new Single_view(views.get(i), views.get(i).name, c, pst);
      
      view_objs.add(curr_view_obj);
    }
  }
  
  
  static Vector<Query> get_all_views(Connection c, PreparedStatement pst) throws SQLException
  {
    return view_operation.get_all_views(c, pst);
  }
  
  
  static HashMap<Single_view, HashSet<Tuple>> clone_view_mappings(HashMap<Single_view, HashSet<Tuple>> view_mappings)
  {
    Set<Single_view> views = view_mappings.keySet();
    
    HashMap<Single_view, HashSet<Tuple>> view_mappings_copy = new HashMap<Single_view, HashSet<Tuple>>();
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = view_mappings.get(view);
      
      view_mappings_copy.put(view, (HashSet<Tuple>) tuples.clone());
      
    }
    
    return view_mappings_copy;
  }
  
  
  
  
//  static ArrayList<HashMap<Single_view, HashSet<Tuple>>> reasoning_covering_sets_conjunctive_query(Query user_query, Connection c, PreparedStatement pst) throws SQLException
//  {
//    HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = get_all_possible_view_mappings(user_query);
//    
//    String sql = Query_converter.data2sql_with_token_columns(user_query);
//    
//    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
//    
//    ResultSet rs = pst.executeQuery();
//    
//    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
//    
//    int num = 0;
//    
//    while(rs.next())
//    {
//      
//      if(num == 0)
//      {        
//        for(int i = 0; i<user_query.head.args.size(); i++)
//        {
//          valid_view_mappings_per_head_var.add(clone_view_mappings(all_possible_view_mappings));
//        }
//      }
//      
//      Vector<String> why_tokens = new Vector<String>();
//      
//      for(int i = 0; i < user_query.body.size(); i++)
//      {
//        why_tokens.add(rs.getString(user_query.head.args.size() + i + 1));
//        
//      }
//      
//      checking_why_provenance_tokens(why_tokens, all_possible_view_mappings);
//      
//      for(int i = 0; i<user_query.head.args.size(); i++)
//      {
//        String where_token = rs.getString(i + 1);
//        
//        check_where_tokens(where_token, valid_view_mappings_per_head_var.get(i));
//        
//        checking_where_why_provenance_tokens(valid_view_mappings_per_head_var.get(i), where_token, why_tokens, all_possible_view_mappings);
//      }
//      
//      num++;
//      
//    }
//    
//    return valid_view_mappings_per_head_var;
//    
//  }
//  
  static Vector<String> get_curr_where_token_seq(ResultSet rs, Vector<String> where_token_seqs, int subgoal_size, int head_arg_size) throws SQLException
  {
    
    Vector<String> where_tokens = new Vector<String>();
    
    for(int i = 0; i<head_arg_size; i++)
    {
      String where_token = rs.getString(i+1);
      
      where_token = where_token.replaceAll("\\|", "\\\\|");
      
      where_tokens.add(where_token);
      
      if(i >= where_token_seqs.size())
      {
        
        //(?=.*\bjack\b)
        String where_token_seq = "^(?=.*\\b" + where_token + "\\b)";
        
        where_token_seqs.add(where_token_seq);
        
      }
      else
      {
        String where_token_seq = where_token_seqs.get(i);
        
        where_token_seq += "(?=.*\\b" + where_token + "\\b)";
        
        where_token_seqs.set(i, where_token_seq);
        
      }
    }
    
    return where_tokens;
  }
  
  static Vector<String> get_curr_why_token_seq(ResultSet rs, Vector<String> why_token_seqs, int subgoal_size, int head_arg_size, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings) throws SQLException
  {
    Vector<String> why_tokens = new Vector<String>();
    
    for(int i = head_arg_size; i<subgoal_size + head_arg_size; i++)
    {
      String why_token = rs.getString(i+1);
      
      why_token = why_token.replaceAll("\\|", "\\\\|");
      
      why_tokens.add(why_token);
    }
    
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    int num = 0;
    
    for(Iterator iter = views.iterator(); iter.hasNext();)
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
        Tuple tuple = (Tuple) iter2.next();
        
        String why_token_seq = view.get_q_why_provenance_token_seq(why_tokens, tuple);
        
        if(num >= why_token_seqs.size())
        {
          why_token_seqs.add(why_token_seq);
        }
        else
        {
          String curr_why_token_seq = why_token_seqs.get(num);
          
          curr_why_token_seq += why_token_seq;
          
          why_token_seqs.set(num, curr_why_token_seq);
        }
        
        num++;
      }
      
    }
    
    return why_tokens;
  }
  
  
  static void get_curr_where_why_token_seq(ResultSet rs, Vector<String> where_tokens, Vector<String> why_tokens, Vector<HashMap<Single_view, Vector<String>>> where_why_token_seqs, int subgoal_size, int head_arg_size, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings) throws SQLException
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    for(int i = 0; i<where_tokens.size(); i++)
    {
      
      String where_token = where_tokens.get(i);
      
      HashMap<Single_view, Vector<String>> where_why_token_seq;
      
      if(i >= where_why_token_seqs.size())
      {
        where_why_token_seq = new HashMap<Single_view, Vector<String>>();
        
        where_why_token_seqs.add(where_why_token_seq);
      }
      else
      {
        where_why_token_seq = where_why_token_seqs.get(i);
      }
      
      
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
        
        int num = 0;
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          Tuple tuple = (Tuple) iter2.next();
          
          String why_token_seq = view.get_q_where_why_provenance_token_seq(where_token, why_tokens, tuple);
          
          if(where_why_token_seq.get(view) == null)
          {
            
            Vector<String> curr_token_seqs = new Vector<String>();
            
            curr_token_seqs.add(why_token_seq);
            
            where_why_token_seq.put(view, curr_token_seqs);
          }
          else
          {
            
            Vector<String> curr_why_token_seqs = where_why_token_seq.get(view);
            
            if(num >= curr_why_token_seqs.size())
            {
              curr_why_token_seqs.add(why_token_seq);
            }
            else
            {
              String curr_why_token_seq = curr_why_token_seqs.get(num);
              
              curr_why_token_seq += why_token_seq;
              
              curr_why_token_seqs.set(num, curr_why_token_seq);
            }
          }
          
          num++;
        }
        
      }
    }
  }
  
  static void init_view_mappings_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> head_args, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
        
    HashMap<Tuple, HashSet<String>> tuple_targeted_subgoal_mappings = new HashMap<Tuple, HashSet<String>>();
    
    for(int i = 0; i<head_args.size(); i++)
    {
      HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings_copy = clone_view_mappings(all_possible_view_mappings);
      
      Set<Single_view> views = all_possible_view_mappings_copy.keySet();
      
      Argument arg = head_args.get(i);
      
      String arg_rel_name = arg.relation_name;
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = all_possible_view_mappings_copy.get(view);
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          Tuple tuple = (Tuple) iter2.next();
          
          if(i == 0)
          {
            HashSet<String> target_subgoal_names = tuple.getTargetSubgoal_strs();
            
            tuple_targeted_subgoal_mappings.put(tuple, target_subgoal_names);
            
            if(!target_subgoal_names.contains(arg_rel_name))
            {
              iter2.remove();
            }
          }
          else
          {
            HashSet<String> target_subgoal_names = tuple_targeted_subgoal_mappings.get(tuple);
            
            if(!target_subgoal_names.contains(arg_rel_name))
            {
              iter2.remove();
            }
          }
          
        }
        
      }
      
      valid_view_mappings_per_head_var.add(all_possible_view_mappings_copy);
      
    }
    
    
    
    
    
  }
  
  static void add_suffix_token_seq(Vector<String> token_seqs)
  {
    for(int i = 0; i<token_seqs.size(); i++)
    {
      String token_seq = token_seqs.get(i);
      
      token_seq += ".*$";
      
      token_seqs.set(i, token_seq);
    }
  }
  
  
  static ArrayList<HashMap<Single_view, HashSet<Tuple>>> reasoning_valid_view_mappings_conjunctive_query(Query user_query, ResultSet rs, Connection c, PreparedStatement pst) throws SQLException
  {
    HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings = get_all_possible_view_mappings(user_query);
    
    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var = new ArrayList<HashMap<Single_view, HashSet<Tuple>>>();
        
    Vector<String> where_token_seqs = new Vector<String>();
    
    Vector<String> why_token_seqs = new Vector<String>();
    
    Vector<HashMap<Single_view, Vector<String>>> where_why_token_seqs = new Vector<HashMap<Single_view, Vector<String>>>();
    
    int subgoal_size = user_query.body.size();
    
    int head_var_size = user_query.head.args.size();
    
//    for(int i = 0; i<user_query.head.args.size(); i++)
//    {
//      valid_view_mappings_per_head_var.add(clone_view_mappings(all_possible_view_mappings));
//    }
    
    init_view_mappings_conjunctive_query(valid_view_mappings_per_head_var, user_query.head.args, all_possible_view_mappings);
    
    while(rs.next())
    {
      
      Vector<String> where_tokens = get_curr_where_token_seq(rs, where_token_seqs, subgoal_size, head_var_size);
      
      Vector<String> why_tokens = get_curr_why_token_seq(rs, why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
      
      get_curr_where_why_token_seq(rs, where_tokens, why_tokens, where_why_token_seqs, subgoal_size, head_var_size, all_possible_view_mappings);
      
    }
    
    add_suffix_token_seq(where_token_seqs);
    
    check_where_tokens(where_token_seqs, valid_view_mappings_per_head_var, where_why_token_seqs);
    
    checking_why_provenance_tokens(why_token_seqs, all_possible_view_mappings);

    
    checking_where_why_provenance_tokens(valid_view_mappings_per_head_var, where_why_token_seqs, all_possible_view_mappings);
    
    return valid_view_mappings_per_head_var;
    
  }
  
  static HashSet<citation_view_vector> reasoning(Query user_query, Connection c, PreparedStatement pst) throws SQLException
  {
    String sql = Query_converter.data2sql_with_token_columns(user_query);
  
    pst = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
  
    ResultSet rs = pst.executeQuery();
    
    
    ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings = reasoning_valid_view_mappings_conjunctive_query(user_query, rs, c, pst);
    
    HashSet<citation_view_vector> covering_sets = reasoning_covering_set_conjunctive_query(valid_view_mappings, user_query.head.args);
    
    return covering_sets;
  }
  
  static HashSet<citation_view_vector> reasoning_covering_set_conjunctive_query(ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings_per_head_var, Vector<Argument> args)
  {
    
    int loop_time = (int) Math.ceil(Math.log(valid_view_mappings_per_head_var.size())/Math.log(2));
    
    Vector<HashSet<citation_view_vector>> covering_sets = new Vector<HashSet<citation_view_vector>>();
    
    for(int i = 1; i<=loop_time; i++)
    {
      int j = 0;
      
      if(i == 1)
      {
        
        for(j = 0; j<valid_view_mappings_per_head_var.size() + 2*i; j = j+2*i)
        {
          HashSet<citation_view_vector> view_com = new HashSet<citation_view_vector>();
          
          for(int k = j; k<j+2*i && k < valid_view_mappings_per_head_var.size(); k++)
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
            
            view_com = join_views_curr_relation(all_tuples, view_com, args);
            
          }
          
          if(!view_com.isEmpty())
             covering_sets.add(view_com);
        }
        
        
      }
      else
      {
        int merge_times = (int) Math.ceil(valid_view_mappings_per_head_var.size()/(2*i));
        
        for(int k = 0; k<merge_times; k=k+2)
        {
          if(k + 1 < covering_sets.size())
          {
            HashSet<citation_view_vector> updated_covering_set = join_operation(covering_sets.get(k), covering_sets.get(k + 1));
            
            covering_sets.set(k/2, updated_covering_set);
          }
          else
          {
            covering_sets.set(k/2, covering_sets.get(k));
          }
        }
        
        for(int k = merge_times; k<valid_view_mappings_per_head_var.size()/(2*(i - 1)); k++)
        {
          covering_sets.remove(covering_sets.size() - 1);
        }
      }
    }
    
    return covering_sets.get(0);
  }
  
  public static HashSet<citation_view_vector> join_operation(HashSet<citation_view_vector> c_combinations, HashSet<citation_view_vector> insert_citations)
  {
/*      if(i == 0)
      {

          c_combinations.addAll(insert_citations);
          
          return c_combinations;
      }
      else*/
      {
          
          HashSet<citation_view_vector> updated_c_combinations = new HashSet<citation_view_vector>();
          
          for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
          {
              
              citation_view_vector curr_combination1 = (citation_view_vector) iter.next();
                              
              for(Iterator it = insert_citations.iterator(); it.hasNext();)
              {
                  
                  citation_view_vector curr_combination2 = (citation_view_vector)it.next(); 
                  
                  citation_view_vector new_citation_vec = curr_combination2.clone();
                  
                  citation_view_vector new_covering_set = curr_combination1.merge(new_citation_vec);
                  
                  remove_duplicate(updated_c_combinations, new_covering_set);
              }
          }
                      
          return updated_c_combinations;
          
      }
  }
  
  public static HashSet<citation_view_vector> join_views_curr_relation(HashSet<Tuple> tuples, HashSet<citation_view_vector> curr_view_com, Vector<Argument> args)
  {
      if(curr_view_com.isEmpty())
      {
          if(tuples.isEmpty())
              return new HashSet<citation_view_vector>();
          else
          {
              HashSet<citation_view_vector> new_view_com = new HashSet<citation_view_vector>();
              
              for(Tuple tuple:tuples)
              {
                  
                  Tuple valid_tuple = (Tuple) tuple.clone();
                  
                  valid_tuple.args.retainAll(args);
                                    
                  if(valid_tuple.lambda_terms.size() > 0)
                  {
                      
                      citation_view_parametered c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple);
                      
                      citation_view_vector curr_views = new citation_view_vector(c);
                      
                      remove_duplicate(new_view_com, curr_views);
                  }   
                  else
                  {
                      
                      citation_view_unparametered c = new citation_view_unparametered(valid_tuple.name, valid_tuple);
                      
                      citation_view_vector curr_views = new citation_view_vector(c);
                      
                      remove_duplicate(new_view_com, curr_views);
                  }
              }
              
              return new_view_com;
          }
      }
      
      else
      {
          HashSet<citation_view_vector> new_view_com = new HashSet<citation_view_vector>();
          
          for(Tuple tuple:tuples)
          {
              Tuple valid_tuple = (Tuple)tuple.clone();
              
              valid_tuple.args.retainAll(args);
              
              citation_view c = null;
              
              if(valid_tuple.lambda_terms.size() > 0)
              {
                  
                  c = new citation_view_parametered(valid_tuple.name, valid_tuple.query, valid_tuple);
              }   
              else
              {
                  
                  c = new citation_view_unparametered(valid_tuple.name, valid_tuple);
              }
              
              for(Iterator iter = curr_view_com.iterator(); iter.hasNext();)
              {
                  citation_view_vector old_view_com = (citation_view_vector)iter.next();
                  
                  citation_view_vector old_view_com_copy = old_view_com.clone(); 
                  
                  citation_view_vector view_com = citation_view_vector.merge(old_view_com_copy, c);
                  
//                  HashSet<String> string_list = new HashSet<String>();
//                  
//                  for(int j = 0; j<view_com.c_vec.size(); j++)
//                  {
//                      string_list.add(view_com.c_vec.get(j).get_name());
//                  }
//                  
//                if(string_list.contains("v4") && string_list.contains("v8") && string_list.contains("v11") && string_list.contains("v6") && string_list.contains("v14") && string_list.contains("v20"))
//                {
//                    int y = 0;
//                    
//                    y++;
//                }
//                if(string_list.contains("v4") && string_list.contains("v8"))
//                {
//                    int y = 0;
//                    
//                    y++;
//                }
                  
                  remove_duplicate(new_view_com, view_com);
              }
          }
          
          return new_view_com;
      }
  }
  
  public static HashSet<citation_view_vector> remove_duplicate_arg(HashSet<citation_view_vector> c_combinations, citation_view_vector c_view)
  {
      int i = 0;
      
      if(c_combinations.contains(c_view))
          return c_combinations;
              
      for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
      {
//        String str = (String) iter.next();
                      
          citation_view_vector c_combination = (citation_view_vector) iter.next();
          {
              {
                  citation_view_vector curr_combination = c_view;
                  if(view_vector_contains(c_combination, curr_combination)&& curr_combination.head_variables.containsAll(c_combination.head_variables) && c_combination.index_vec.size() > curr_combination.index_vec.size())
                  {
                      iter.remove();                      
                  }
                  
                  if(view_vector_contains(curr_combination, c_combination) && c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
                  {
                      break;
                  }
              }
              
          }
          
          i++;
      }
      
      
      if(i >= c_combinations.size())
          c_combinations.add(c_view);
      
              
      return c_combinations;
  }
  
  public static HashSet<citation_view_vector> remove_duplicate(HashSet<citation_view_vector> c_combinations, citation_view_vector c_view)
  {
      int i = 0;
      
      if(c_combinations.contains(c_view))
          return c_combinations;
              
      for(Iterator iter = c_combinations.iterator(); iter.hasNext();)
      {
//        String str = (String) iter.next();
                      
          citation_view_vector c_combination = (citation_view_vector) iter.next();
          
//        if(c_combination.toString().equals("v11*v20*v4*v8"))
//        {
//            int y = 0;
//            
//            y++;
//        }
          {
              {
                  citation_view_vector curr_combination = c_view;
                  if(view_vector_contains(c_combination, curr_combination)&& table_names_contains(c_combination, curr_combination)&& curr_combination.head_variables.containsAll(c_combination.head_variables) && c_combination.index_vec.size() > curr_combination.index_vec.size())
                  {
                      iter.remove();                      
                  }
                  
                  if(view_vector_contains(curr_combination, c_combination) && table_names_contains(curr_combination, c_combination)&& c_combination.head_variables.containsAll(curr_combination.head_variables) && curr_combination.index_vec.size() > c_combination.index_vec.size())
                  {
                      break;
                  }
              }
              
          }
          
          i++;
      }
      
      
      if(i >= c_combinations.size())
          c_combinations.add(c_view);
      
              
      return c_combinations;
  }
  
  static boolean view_vector_contains(citation_view_vector c_vec1, citation_view_vector c_vec2)
  {
      
      String s1 = ".*";
      
      String s2 = c_vec1.index_str;
      
      for(int i = 0; i<c_vec2.index_vec.size(); i++)
      {
          String str = c_vec2.index_vec.get(i);
          
          str = str.replaceAll("\\(", "\\\\(");
          
          str = str.replaceAll("\\)", "\\\\)");
          
          str = str.replaceAll("\\[", "\\\\[");
          
          str = str.replaceAll("\\]", "\\\\]");
          
          str = str.replaceAll("\\/", "\\\\/");
          
          s1 += "\\(" + str + "\\).*";
      }
      
      return s2.matches(s1);

  }
  
  static boolean table_names_contains(citation_view_vector c_vec1, citation_view_vector c_vec2)
  {
      String s1 = ".*";
      
      String s2 = c_vec1.table_name_str;
      
      for(int i = 0; i<c_vec2.c_vec.size(); i++)
      {
          
          String str = c_vec2.c_vec.get(i).get_table_name_string();
          
          str = str.replaceAll("\\[", "\\\\[");
          
          str = str.replaceAll("\\]", "\\\\]");
          
          s1 += str + ".*";
          
      }
      
      return s2.matches(s1);
  }
  
  
  static void check_where_tokens(Vector<String> where_tokens, ArrayList<HashMap<Single_view, HashSet<Tuple>>> valid_view_mappings, Vector<HashMap<Single_view, Vector<String>>> where_why_token_seqs)
  {
    
    for(int i = 0; i<where_tokens.size(); i++)
    {
      String curr_where_token = where_tokens.get(i);
      
      HashMap<Single_view,HashSet<Tuple>> curr_valid_view_mappings = valid_view_mappings.get(i);
      
      Set<Single_view> views = curr_valid_view_mappings.keySet();
      
      HashMap<Single_view, Vector<String>> curr_where_why_token_seqs = where_why_token_seqs.get(i);
      
      for(Iterator iter = views.iterator(); iter.hasNext();)
      {
        Single_view view = (Single_view) iter.next();
        
        HashSet<Tuple> tuples = curr_valid_view_mappings.get(view);
        
        if(!view.check_where_provenance_token(curr_where_token))
        {
          iter.remove();
          
          curr_where_why_token_seqs.remove(view);
          
        }
    
      }
      
    }
  }
  
  static void checking_why_provenance_tokens(Vector<String> why_tokens, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    Set<Single_view> views = all_possible_view_mappings.keySet();
    
    int num = 0;
    
    for(Iterator iter = views.iterator(); iter.hasNext(); )
    {
      Single_view view = (Single_view) iter.next();
      
      HashSet<Tuple> tuples = all_possible_view_mappings.get(view);
      
      for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
      {
        String curr_why_token = why_tokens.get(num);
        
        Tuple tuple = (Tuple) iter2.next();
        
        if(!view.check_provenance_tokens(curr_why_token))
        {
          iter2.remove();
        }
        
        num++;
      }
      
      if(tuples.isEmpty())
      {
        iter.remove();
      }
      
      
    }
  }
  
  static void checking_where_why_provenance_tokens(ArrayList<HashMap<Single_view, HashSet<Tuple>>> possible_valid_view_mappings, Vector<HashMap<Single_view, Vector<String>>> where_why_tokens, HashMap<Single_view, HashSet<Tuple>> all_possible_view_mappings)
  {
    
    for(int i = 0; i<possible_valid_view_mappings.size(); i++)
    {
      HashMap<Single_view, HashSet<Tuple>> curr_valid_view_mappings = possible_valid_view_mappings.get(i);
            
      HashMap<Single_view, Vector<String>> curr_where_why_token_seqs = where_why_tokens.get(i); 
      
      Set<Single_view> views = curr_valid_view_mappings.keySet();
      
      
      
      for(Iterator iter = views.iterator(); iter.hasNext(); )
      {
        Single_view view = (Single_view) iter.next();
        
        Vector<String> curr_token_seqs = curr_where_why_token_seqs.get(view);
        
        HashSet<Tuple> tuples = curr_valid_view_mappings.get(view);
        
        HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
        
        tuples.retainAll(tuples2);
        
        int k = 0;
        
        for(Iterator iter2 = tuples.iterator(); iter2.hasNext();)
        {
          String curr_why_token = curr_token_seqs.get(k);
          
          Tuple tuple = (Tuple) iter2.next();
          
          if(!tuples2.contains(tuple))
          {
            iter2.remove();
            
            k++;
            
            continue;
          }
          
          if(!view.check_provenance_tokens(curr_why_token))
          {
            iter2.remove();
          }
          
          k++;
        }
        
        if(tuples.isEmpty())
        {
          iter.remove();
        }
        
      }
      
    }
//    Set<Single_view> views = possible_valid_view_mappings.keySet();
//    
//    for(Iterator iter = views.iterator(); iter.hasNext(); )
//    {
//      Single_view view = (Single_view) iter.next();
//      
//      HashSet<Tuple> tuples1 = possible_valid_view_mappings.get(view);
//      
//      HashSet<Tuple> tuples2 = all_possible_view_mappings.get(view);
//      
//      tuples1.retainAll(tuples2);
//      
//      view.check_where_why_provenance_tokens(tuples1, where_token, where_why_tokens);
//            
//      if(tuples1.isEmpty())
//      {
//        iter.remove();
//      }
//    }
  }
  
  static HashMap<Single_view, HashSet<Tuple>> get_all_possible_view_mappings(Query q)
  {
    
    HashMap<Single_view, HashSet<Tuple>> view_mappings = new HashMap<Single_view, HashSet<Tuple>>();
    
    for(int i = 0; i<view_objs.size(); i++)
    {
      Single_view view = view_objs.get(i);
      
      view.build_view_mappings(q.body, q.subgoal_name_mapping);
      
      if(!view.view_mappings.isEmpty())
        view_mappings.put(view, view.view_mappings);
      
    }
    
    return view_mappings;
  }
  
}
