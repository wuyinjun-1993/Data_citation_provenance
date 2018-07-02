package edu.upenn.cis.citation.query_view_generators;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Argument;
import edu.upenn.cis.citation.Corecover.Lambda_term;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.Corecover.Subgoal;
import edu.upenn.cis.citation.Operation.Conditions;
import edu.upenn.cis.citation.Operation.op_equal;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.init.init;
import edu.upenn.cis.citation.views.Query_converter;

public class view_generator {
  
  static HashMap<String, Vector<String>> query_table_names = new HashMap<String, Vector<String>>();
  
  static HashMap<String, Vector<Argument>> query_head_names = new HashMap<String, Vector<Argument>>();
  
  static HashMap<String, Vector<Conditions>> query_conditions = new HashMap<String, Vector<Conditions>>();

  
  public static String [] citatable_tables = {"gpcr","object", "ligand", "family", "introduction"};
  static double head_var_rate = 0.8;
  
  static double global_predicate_rate = 1;
  static double local_predicates_rate = 0.7;
  static Vector<String> available_data_type_vec = new Vector<String>();
  static String [] available_data_type = {"integer", "text", "boolean", "character varying", "double precision", "real", "character varying", "bigint"};

  public static String view_file_name = "views";
  
  public static String citation_query_file_name = "citation_queries";
  
  public static String view_citation_query_mapping_file_name = "view_citation_query_mappings";
  
  public static int view_instance_size = 100000;
  
  public static void main(String[] args)
  {
    
  }
  
  public static void initial()
  {
      Vector<String> table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2object");
      
      table_names.add("object");
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"contributor_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"object_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("object" + init.separator +"object_id", "object"), "object", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      Vector<Argument> head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("object", table_names);
      
      query_head_names.put("object", head_variables);
      
      query_conditions.put("object", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2object");
      
      table_names.add("gpcr");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"contributor_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2object" + init.separator +"object_id", "contributor2object"), "contributor2object", new op_equal(), new Argument("gpcr" + init.separator +"object_id", "gpcr"), "gpcr", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("gpcr", table_names);
      
      query_head_names.put("gpcr", head_variables);
      
      query_conditions.put("gpcr", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2family");
      
      table_names.add("family");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2family" + init.separator +"contributor_id", "contributor2family"), "contributor2family", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2family" + init.separator +"family_id", "contributor2family"), "contributor2family", new op_equal(), new Argument("family" + init.separator +"family_id", "family"), "family", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("family", table_names);
      
      query_head_names.put("family", head_variables);
      
      query_conditions.put("family", conditions);
      
      table_names = new Vector<String>();
      
      table_names.add("contributor");
      
      table_names.add("contributor2intro");
      
      table_names.add("introduction");
      
      conditions = new Vector<Conditions>();
      
      conditions.add(new Conditions(new Argument("contributor2intro" + init.separator +"contributor_id", "contributor2intro"), "contributor2intro", new op_equal(), new Argument("contributor" + init.separator +"contributor_id", "contributor"), "contributor", null, null));
      
      conditions.add(new Conditions(new Argument("contributor2intro" + init.separator +"family_id", "contributor2intro"), "contributor2intro", new op_equal(), new Argument("introduction" + init.separator +"family_id", "introduction"), "introduction", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor" + init.separator + "first_names", "contributor"));
      
      head_variables.add(new Argument("contributor" + init.separator + "surname", "contributor"));
      
      query_table_names.put("introduction", table_names);
      
      query_head_names.put("introduction", head_variables);
      
      query_conditions.put("introduction", conditions);
      
      table_names = new Vector<String>();
              
      table_names.add("contributor2ligand");
      
      table_names.add("ligand");
      
      conditions = new Vector<Conditions>();
              
      conditions.add(new Conditions(new Argument("contributor2ligand" + init.separator +"ligand_id", "contributor2ligand"), "contributor2ligand", new op_equal(), new Argument("ligand" + init.separator +"ligand_id", "ligand"), "ligand", null, null));
      
//    conditions.add("object_object_id = contributor2object_object_id");
      
      head_variables = new Vector<Argument>();
      
      head_variables.add(new Argument("contributor2ligand" + init.separator + "first_names", "contributor2ligand"));
      
      head_variables.add(new Argument("contributor2ligand" + init.separator + "surname", "contributor2ligand"));
      
      query_table_names.put("ligand", table_names);
      
      query_head_names.put("ligand", head_variables);
      
      query_conditions.put("ligand", conditions);
      
//    table_names = new Vector<String>();
//    
//    table_names.add("contributor");
//    
//    table_names.add("contributor2ligand");
//    
//    table_names.add("ligand");
//    
//    conditions = new Vector<String>();
//        
//    conditions.add("ligand_ligand_id = contributor2ligand_ligand_id");
//    
//    head_variables = new Vector<String>();
//    
//    head_variables.add("contributor2ligand_first_names");
//    
//    head_variables.add("contributor2ligand_surname");
//    
//    query_table_names.put("ligand", table_names);
//    
//    query_head_names.put("ligand", head_variables);
//    
//    query_conditions.put("ligand", conditions);
      
      
      
  }
  
  
  public static void store_views_with_citation_queries(Vector<Query> views, String view_file)
  {
    Vector<String> view_strings = Load_views_and_citation_queries.views2text_strings(views);
    Load_views_and_citation_queries.write2files(view_file, view_strings);
    Vector<Query> citation_queries = new Vector<Query>();
    Vector<String> view_citation_query_mappings = new Vector<String>();
//    for(int i = 0; i<views.size(); i++)
//    {
//      Query query = store_citation_queries(views.get(i), i, "q" + i);
//      citation_queries.add(query);
//      view_citation_query_mappings.add(views.get(i).name + "|" + query.name + "|" + "Contributor");
//    }
//    Vector<String> citation_query_strings = Load_views_and_citation_queries.views2text_strings(citation_queries);
//    Load_views_and_citation_queries.write2files(citation_query_file_name, citation_query_strings);
//    Load_views_and_citation_queries.write2files(view_citation_query_mapping_file_name, view_citation_query_mappings);
//    
    
  }
  
  public static void append_views_with_citation_queries(Vector<Query> views, int offset, String view_file_name)
  {
    Vector<String> view_strings = Load_views_and_citation_queries.views2text_strings(views);
    Load_views_and_citation_queries.append2files(view_file_name, view_strings);
//    Vector<Query> citation_queries = new Vector<Query>();
//    Vector<String> view_citation_query_mappings = new Vector<String>();
//    for(int i = 0; i<views.size(); i++)
//    {
//      int id = i + offset;
//      Query query = store_citation_queries(views.get(i), id, "q" + id);
//      citation_queries.add(query);
//      view_citation_query_mappings.add(views.get(i).name + "|" + query.name + "|" + "Contributor");
//    }
//    Vector<String> citation_query_strings = Load_views_and_citation_queries.views2text_strings(citation_queries);
//    Load_views_and_citation_queries.append2files(citation_query_file_name, citation_query_strings);
//    Load_views_and_citation_queries.append2files(view_citation_query_mapping_file_name, view_citation_query_mappings);
    
    
  }
  
  static Query store_citation_queries(Query view, int num, String qname)
  {
      Vector<String> citable_table_names = new Vector<String>();
      
      citable_table_names.addAll(Arrays.asList(citatable_tables));
      
      String subgoal = null;
      
//    while(true)
      {
          Random r = new Random();
          
          int index = r.nextInt(view.body.size());
          
          
          
          subgoal = view.subgoal_name_mapping.get(((Subgoal)view.body.get(index)).name);
          
//        if(citable_table_names.contains(subgoal))
//            break;
      }
      
      
      Vector<Conditions> conditions = query_conditions.get(subgoal);
      
      Vector<Argument> head_vars = query_head_names.get(subgoal);
      
      Vector<String> subgoals = query_table_names.get(subgoal);
      
      Subgoal head = new Subgoal(qname, head_vars);
      
      Vector<Subgoal> body = new Vector<Subgoal>();
      
      HashMap<String, String> subgoal_mapping = new HashMap<String, String>();
      
      for(int i = 0; i < subgoals.size(); i++)
      {
          Vector<Argument> args = new Vector<Argument>();
          
          body.add(new Subgoal(subgoals.get(i), args));
          
          subgoal_mapping.put(subgoals.get(i), subgoals.get(i));
      }
      
      Vector<Lambda_term> l_args = new Vector<Lambda_term>();
      
      for(int i = 0; i<view.lambda_term.size(); i++)
      {
          Lambda_term l = view.lambda_term.get(i);
          
          if(l.table_name.equals(subgoal))
          {
              l_args.add(l);
              
              break;
          }
      }
              
      Query q = new Query(qname, head, body, l_args, conditions, subgoal_mapping);
      
      return q;
      
  }
  
  static Vector<Integer> generator_random_numbers(int num, int max_num)
  {
      int i = 0;
      
      Vector<Integer> random_number = new Vector<Integer>();
      
      Random r = new Random();
      
      while(i < num)
      {
          double val = r.nextGaussian() * 5;
          
          if(val <= 0 || val > max_num)
              continue;
          
          int value = (int) Math.ceil(val);
          
          if(value == 0)
              continue;
          
          random_number.add(value);
          
          i++;
      }
      return random_number;
  }
  
  static HashMap<String, String> set_up_relation_primary_keys()
  {
    HashMap<String, String> relation_primary_key_mappings = new HashMap<String, String>();
    
    relation_primary_key_mappings.put("gpcr", "object_id");
    
    relation_primary_key_mappings.put("object", "object_id");
    
    relation_primary_key_mappings.put("family", "family_id");
    
    relation_primary_key_mappings.put("introduction", "family_id");
    
    relation_primary_key_mappings.put("ligand", "ligand_id");
    
    return relation_primary_key_mappings;
    
  }
  
  public static Vector<Query> gen_views(boolean has_agg, HashSet<String> subgoal_names, Query query, int num_views, int sizeofquety, int offset, Connection c, PreparedStatement pst) throws SQLException
  {
    initial();
    
      Vector<Integer> sizes = generator_random_numbers(num_views, sizeofquety);
      
      Vector<String> all_citable_tables = new Vector<String>();
      
      all_citable_tables.addAll(Arrays.asList(citatable_tables));
      
//    all_citable_tables.removeAll(subgoal_names);
      
      HashMap<String, String> relation_primary_key_mappings = set_up_relation_primary_keys();
      
      Vector<Query> queries = new Vector<Query>();
      
      int num = 0;
      
      if(!sizes.contains(1))
          sizes.set(0, 1);
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      while(queries.size() < sizes.size())
      {
          
          int size = sizes.get(num);
             
          Query view = generate_view_without_predicates_partial_mappings(conditions, subgoal_names, query, num + offset, sizeofquety, all_citable_tables, relation_primary_key_mappings, has_agg, c, pst);
                      
//      if(!queries.contains(query))
          {
              queries.add(view);
              
              System.out.println(view.lambda_term + "," + view.toString());
              
              num ++;
          }
          
          
      }
      
      return queries;
  }
  
  public static Vector<Query> gen_views_random(boolean has_agg, HashSet<String> subgoal_names, Query query, int num_views, int sizeofquety, int offset, Connection c, PreparedStatement pst) throws SQLException
  {
    initial();
    
      Vector<Integer> sizes = generator_random_numbers(num_views, sizeofquety);
      
      Vector<String> all_citable_tables = new Vector<String>();
      
      all_citable_tables.addAll(Arrays.asList(citatable_tables));
      
//    all_citable_tables.removeAll(subgoal_names);
      
      HashMap<String, String> relation_primary_key_mappings = set_up_relation_primary_keys();
      
      Vector<Query> queries = new Vector<Query>();
      
      int num = 0;
      
      if(!sizes.contains(1))
          sizes.set(0, 1);
      
      Vector<Conditions> conditions = new Vector<Conditions>();
      
      while(queries.size() < sizes.size())
      {
          
          int size = sizes.get(num);
             
          Query view = generate_view_without_predicates_partial_mappings_random(conditions, subgoal_names, query, num + offset, sizeofquety, all_citable_tables, relation_primary_key_mappings, has_agg, c, pst);
                      
//      if(!queries.contains(query))
          {
              queries.add(view);
              
              System.out.println(view.lambda_term + "," + view.toString());
              
              num ++;
          }
          
          
      }
      
      return queries;
  }
  
  public static Vector<Query> update_instance_size(Vector<Query> views, Connection c, PreparedStatement pst) throws SQLException
  {
    Vector<Query> updated_views = new Vector<Query>();
    
    Vector<Conditions> conditions = new Vector<Conditions>();
    
    for(int i = 0; i<views.size(); i++)
    {
      Query view = views.get(i);
      
      Vector<Conditions> q_conditions = new Vector<Conditions>();
      
      if(conditions.isEmpty())
      {
        q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, view.body, view.subgoal_name_mapping, c, pst));
      }
      else
      {
        q_conditions.addAll(conditions);
      }
      
      conditions.clear();
      
      conditions.addAll(q_conditions);
      
      Query updated_view = new Query(view.name, view.head, view.body, view.lambda_term, conditions, view.subgoal_name_mapping);
      
      System.out.println(updated_view);
      
      updated_views.add(updated_view);
    }
    
    return updated_views;
  }
  
  static HashMap<String, String> get_attr_types(String relation_name, Connection c, PreparedStatement pst) throws SQLException
  {
      Vector<String> attr_names = new Vector<String>();
      
      String query_attr_name = "SELECT column_name, data_type "
              + "FROM information_schema.columns "
              + "WHERE table_name = '"+ relation_name + "' "
              + "ORDER BY ordinal_position";
      pst = c.prepareStatement(query_attr_name);
      
      ResultSet rs = pst.executeQuery();
      
      HashMap<String, String> attr_type_mapping = new HashMap<String, String>();
      
      while(rs.next())
      {
          String attr_name = rs.getString(1);
          
          if(attr_name.endsWith(init.separator + "prov"))
            continue;
          
          String type = rs.getString(2);
          
          if(available_data_type_vec.size() == 0)
          {
              available_data_type_vec.addAll(Arrays.asList(available_data_type));
          }
          
          if(available_data_type_vec.contains(type))
          {
              attr_type_mapping.put(attr_name, type);
          }
      }
      
      return attr_type_mapping;
  }
  
  static String [] get_primary_key(String table_name, Connection c, PreparedStatement pst) throws SQLException
  {
      String query = "SELECT"
              + " c.column_name, c.data_type"
              + " FROM information_schema.table_constraints tc"
              + " JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)"
              + " JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name"
              + " where constraint_type = 'PRIMARY KEY' and tc.table_name = '"+ table_name  +"'";
      
      pst =c.prepareStatement(query);
      
      String [] results = new String [2];
      
      ResultSet rs = pst.executeQuery();
      
      if(rs.next())
      {
          results[0] = rs.getString(1);
          
          results[1] = rs.getString(2);
      }
      
      return results;
      
  }
  
  static Vector<Argument> gen_head_vars(String relation_name, Vector<String> attr_list, int size, Connection c, PreparedStatement pst)
  {
      
      HashSet<Integer> id_set = new HashSet<Integer>();
      
      Vector<Argument> head_vars = new Vector<Argument>();
      
      Random r = new Random();
      
      for(int i = 0; i<size; i++)
      {
          int index = 0;
          if(i == 0)
          {
              index = 0;
              
              id_set.add(index);          
          }
          else
          {
              index = r.nextInt(attr_list.size());
              
              if(index != 0)
                  id_set.add(index);          
          }
          
      }
      
      for(Iterator iter = id_set.iterator(); iter.hasNext();)
      {
          Integer index = (Integer) iter.next();
          
          Argument l = new Argument(relation_name + init.separator + attr_list.get(index), relation_name);
          
          head_vars.add(l);
          
      }
      
      return head_vars;
  }
  
  static Vector<Argument> gen_head_vars(String relation, String relation_name, Vector<String> attr_list, int size, Connection c, PreparedStatement pst)
  {
      
      HashSet<Integer> id_set = new HashSet<Integer>();
      
      Vector<Argument> head_vars = new Vector<Argument>();
      
      Random r = new Random();
      
      for(int i = 0; i<size; i++)
      {
          int index = r.nextInt(attr_list.size());
          
//        if(query_generator.parameterizable_attri.get(relation).contains(attr_list.get(index)))
          if(!id_set.contains(index))
          {
              id_set.add(index);
          }
          else
          {
              i--;
          }
//        
      }
      
      for(Iterator iter = id_set.iterator(); iter.hasNext();)
      {
          Integer index = (Integer) iter.next();
          
          Argument l = new Argument(relation_name+ init.separator + attr_list.get(index), relation_name);
          
          head_vars.add(l);
          
      }
      
      return head_vars;
  }
  
  static Vector<Argument> get_all_attributes(Query query)
  {
    Vector<Argument> args = new Vector<Argument>();
    for(int i = 0; i<query.body.size(); i++)
    {
      Subgoal subgoal = (Subgoal) query.body.get(i);
      
      args.addAll(subgoal.args);
    }
    return args;
  }
  
  static Query generate_view_without_predicates_partial_mappings(Vector<Conditions> conditions, HashSet<String> subgoal_names, Query query, int id, int size, Vector<String> all_citable_tables, HashMap<String, String> relation_primary_key_mappings, boolean has_agg, Connection c, PreparedStatement pst) throws SQLException
  {
    
    Vector<Argument> all_args = get_all_attributes(query);
    all_args.removeAll(query.head.args);
    
    Vector<Subgoal> body = new Vector<Subgoal>();
    
    body.addAll(query.body);
    
    Vector<Argument> head_grouping_attrs = new Vector<Argument>();
    
    head_grouping_attrs.addAll(query.head.args);
    
    Vector<Vector<Argument>> head_agg_attrs = new Vector<Vector<Argument>>();

    Vector<String> head_agg_functions = new Vector<String>();
    
    Random r = new Random();

    HashMap<String, String> maps = new HashMap<String, String>();
    
    if(has_agg)
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Argument head_grouping_arg = (Argument) query.head.agg_args.get(i).get(0);
        String rel_name = head_grouping_arg.relation_name;//.name.substring(0, head_grouping_arg.name.indexOf(init.separator));
        String origin_rel_name = query.subgoal_name_mapping.get(rel_name);
        String arg_name = head_grouping_arg.attribute_name;//.name.substring(head_grouping_arg.name.indexOf(init.separator) + 1, head_grouping_arg.name.length());
        
        if(query_generator.parameterizable_attri.get(origin_rel_name).contains(arg_name))
        {
          Boolean b = r.nextBoolean();
          
          if(b)
          {
            Vector<Argument> curr_head_arr = new Vector<Argument>();
            
            curr_head_arr.add((Argument) query.head.agg_args.get(i).get(0));
            
            head_agg_attrs.add(curr_head_arr);
            head_agg_functions.add((String) query.head.agg_function.get(i));
          }
        }
      }
      
      if(head_agg_attrs.isEmpty())
      {
        Vector<Argument> curr_head_arr = new Vector<Argument>();
        
        curr_head_arr.add((Argument) query.head.agg_args.get(0).get(0));
        
        head_agg_attrs.add(curr_head_arr);
        head_agg_functions.add((String) query.head.agg_function.get(0));
      }
    }
    else
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Boolean b = r.nextBoolean();
        
        if(b)
        {
          Vector<Argument> args = query.head.agg_args.get(i);
          
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
      }
    }
    

    maps.putAll(query.subgoal_name_mapping);
    
    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = all_args.get(i);
      
      String arg_relation = arg.relation_name;
      String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(query_generator.parameterizable_attri.get(arg_relation).contains(arg_name) && !query_generator.relation_primary_key_mapping.get(arg_relation).contains(arg_name))
      {
        Boolean b = r.nextBoolean();
        
        if(b && !head_grouping_attrs.contains(all_args.get(i)))
        {
          head_grouping_attrs.add(all_args.get(i));
          break;
        }
      }

    }
    
    Subgoal head = new Subgoal("v" + id, head_grouping_attrs, head_agg_attrs, head_agg_functions, true);
    
    Vector<Conditions> q_conditions = new Vector<Conditions>();
    
    if(has_agg)
    {
      if(conditions.isEmpty())
      {
        q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, body, maps, c, pst));
      }
      else
      {
        q_conditions.addAll(conditions);
      }
      
      conditions.clear();
      
      conditions.addAll(q_conditions);
    }
    
    return new Query("v" + id, head, body, new Vector<Lambda_term>(), q_conditions , maps);

  }
  
  static Query generate_view_without_predicates_partial_mappings_random(Vector<Conditions> conditions, HashSet<String> subgoal_names, Query query, int id, int size, Vector<String> all_citable_tables, HashMap<String, String> relation_primary_key_mappings, boolean has_agg, Connection c, PreparedStatement pst) throws SQLException
  {
    
    Vector<Argument> all_args = get_all_attributes(query);
    all_args.removeAll(query.head.args);
    
    Vector<Subgoal> body = new Vector<Subgoal>();
    
    body.addAll(query.body);
    
    Vector<Argument> head_grouping_attrs = new Vector<Argument>();
    
    head_grouping_attrs.addAll(query.head.args);
    
    Vector<Vector<Argument>> head_agg_attrs = new Vector<Vector<Argument>>();

    Vector<String> head_agg_functions = new Vector<String>();
    
    Random r = new Random();

    HashMap<String, String> maps = new HashMap<String, String>();
    
    if(has_agg)
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Argument head_grouping_arg = (Argument) query.head.agg_args.get(i).get(0);
        String rel_name = head_grouping_arg.relation_name;//.name.substring(0, head_grouping_arg.name.indexOf(init.separator));
        String origin_rel_name = query.subgoal_name_mapping.get(rel_name);
        String arg_name = head_grouping_arg.attribute_name;//.name.substring(head_grouping_arg.name.indexOf(init.separator) + 1, head_grouping_arg.name.length());
        
        if(query_generator.parameterizable_attri.get(origin_rel_name).contains(arg_name))
        {
          Boolean b = r.nextBoolean();
          
          if(b)
          {
            Vector<Argument> curr_head_arr = new Vector<Argument>();
            
            curr_head_arr.add((Argument) query.head.agg_args.get(i).get(0));
            
            head_agg_attrs.add(curr_head_arr);
            head_agg_functions.add((String) query.head.agg_function.get(i));
          }
        }
      }
      
      if(head_agg_attrs.isEmpty())
      {
        Vector<Argument> curr_head_arr = new Vector<Argument>();
        
        curr_head_arr.add((Argument) query.head.agg_args.get(0).get(0));
        
        head_agg_attrs.add(curr_head_arr);
        head_agg_functions.add((String) query.head.agg_function.get(0));
      }
    }
    else
    {
      for(int i = 0; i<query.head.agg_args.size(); i++)
      {
        Boolean b = r.nextBoolean();
        
        if(b)
        {
          Vector<Argument> args = query.head.agg_args.get(i);
          
          for(int k = 0; k<args.size(); k++)
          {
            Argument arg = args.get(k);
            
            if(!head_grouping_attrs.contains(arg))
            {
              head_grouping_attrs.add(arg);
            }
          }
        }
      }
    }
    

    maps.putAll(query.subgoal_name_mapping);
    
    for(int i = 0; i<all_args.size(); i++)
    {
      Argument arg = all_args.get(i);
      
      String arg_relation = arg.relation_name;
      String arg_name = arg.name.substring(arg.name.indexOf(init.separator) + 1, arg.name.length());
      
      if(query_generator.parameterizable_attri.get(arg_relation).contains(arg_name) && !query_generator.relation_primary_key_mapping.get(arg_relation).contains(arg_name))
      {
        Boolean b = r.nextBoolean();
        
        if(b && !head_grouping_attrs.contains(all_args.get(i)))
        {
          head_grouping_attrs.add(all_args.get(i));
          break;
        }
      }

    }
    
    Subgoal head = new Subgoal("v" + id, head_grouping_attrs, head_agg_attrs, head_agg_functions, true);
    
    Vector<Conditions> q_conditions = new Vector<Conditions>();
    
    q_conditions.addAll(query_generator.gen_conditions_random(false, view_instance_size, body, maps, c, pst));
    
    if(has_agg)
    {
//      if(conditions.isEmpty())
//      {
//          q_conditions.addAll(query_generator.gen_conditions(false, view_instance_size, body, maps, c, pst));
//      }
//      else
//      {
//        q_conditions.addAll(conditions);
//      }
//      
//      conditions.clear();
//      
//      conditions.addAll(q_conditions);
      
      boolean b = r.nextBoolean();
      
      if(b)      
        q_conditions.addAll(query_generator.gen_conditions_random(body, maps, c, pst));
    }
    
    return new Query("v" + id, head, body, new Vector<Lambda_term>(), q_conditions , maps);

  }
  

}
