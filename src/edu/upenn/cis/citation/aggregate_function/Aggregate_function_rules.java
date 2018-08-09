package edu.upenn.cis.citation.aggregate_function;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Aggregate_function_rules {
  
  static String mapping_symbol = ":";
  
  static String function_name_separator = ",";
  
  public static String unique_string_symbol = "unique"; 

  static HashMap<HashSet<String>, HashSet<String>> source_target_mappings = new HashMap<HashSet<String>, HashSet<String>>();
  
  
  static HashSet<String> trim_strings(String[] sets)
  {
    HashSet<String> updated_sets = new HashSet<String>();
    
    for(String set : sets)
    {
      updated_sets.add(set.trim());
    }
    
    return updated_sets;
  }
  
  public static void load_aggregate_function_rules(String file_name)
  {
    try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
      String line;
      
      while ((line = br.readLine()) != null) {
         // process the line.
        String[] agg_functions = line.split(mapping_symbol);
        
        String[] target_agg_functions = agg_functions[1].split(function_name_separator);
        
        String[] source_agg_functions = agg_functions[0].split(function_name_separator);  
        
        HashSet<String> source_agg_func_sets = trim_strings(source_agg_functions);
        
        HashSet<String> target_agg_func_sets = trim_strings(target_agg_functions);
        
        if(source_target_mappings.get(source_agg_func_sets) == null)
        {
          source_target_mappings.put(source_agg_func_sets, new HashSet<String>());
        }
        
        source_target_mappings.get(source_agg_func_sets).addAll(target_agg_func_sets);
        
      }
  } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
  } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
  }
    
  }
  
  
  public static HashSet<HashSet<String>> check_agg_function_satisfiable(HashSet<String> source_functions, String target)
  {
    HashSet<String> covered_functions = new HashSet<String>();
    
    covered_functions.addAll(source_functions);
    
    int origin_size = 0;
    
    HashSet<HashSet<String>> good_source_function_sets = new HashSet<HashSet<String>>();
    
    while(true)
    {
      Set<HashSet<String>> all_source_function_sets = source_target_mappings.keySet();
      
      for(HashSet<String> curr_source_function_set: all_source_function_sets)
      {
        HashSet<String> curr_target_function_set = source_target_mappings.get(curr_source_function_set);
        
        if(covered_functions.containsAll(curr_source_function_set))
        {
          if(curr_target_function_set.contains(target))
          {
            good_source_function_sets.add(curr_source_function_set);
          }

          covered_functions.addAll(curr_target_function_set);
        }
      }
      
      int new_size = good_source_function_sets.size();
      
      if(new_size == origin_size)
        break;
      
      origin_size = new_size;
    }
    
    return good_source_function_sets;
  }
  
  

}
