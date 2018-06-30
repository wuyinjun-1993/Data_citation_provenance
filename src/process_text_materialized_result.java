import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import org.apache.commons.lang3.StringUtils;

public class process_text_materialized_result {
  
  static String synthetic_result_path = "synthetic_example/";
  
  static String input_file = synthetic_result_path + "stress_test_instance_size.txt";
  
  static String output_file = synthetic_result_path + "stress_test_instance_size.csv";
  
  static String[] prefixes = {"query_final_size::", "view_final_size::", "materialized::false", "materialized::true"};
  
  public static void main(String[] strs)
  {
//    process_text_full_case(path + "final_stress_test_group_full.txt", path + "exp_final_group_test_full_tuple_agg_intersection.csv", path + "exp_final_group_test_full_tuple_agg_union.csv", path + "exp_final_group_test_full_semi_schema_agg_intersection.csv", path + "exp_final_group_test_full_semi_schema_agg_union.csv", path + "exp_final_group_test_full_schema.csv");
      
//    process_text_min_case(path + "final_stress_test_group_min.txt", path + "exp_final_stress_test_group_min_tuple.csv", path + "exp_final_stress_test_group_min_semi_schema.csv", path + "exp_final_stress_test_group_min_schema.csv");
    
    Vector<String> results = process(input_file);
    
    write(output_file, results);
  }
  
  static void process_result(int query_instance_size, int view_instance_size, Vector<Double> time, HashMap<Integer, HashMap<Integer, Vector<Double>>> results)
  {
    if(results.get(query_instance_size) == null)
    {
      HashMap<Integer, Vector<Double>> curr_res = new HashMap<Integer, Vector<Double>>();
      
      Vector<Double> time_vec = new Vector<Double>();
      
      time_vec.addAll(time);
      
      curr_res.put(view_instance_size, time_vec);
      
      results.put(query_instance_size, curr_res);
    }
    else
    {
      if(results.get(query_instance_size).get(view_instance_size) == null)
      {
        Vector<Double> time_vec = new Vector<Double>();
        
        time_vec.addAll(time);
        
        results.get(query_instance_size).put(view_instance_size, time_vec);
      }
      else
      {
        results.get(query_instance_size).get(view_instance_size).addAll(time);
      }
    }
  }
  
  static void process_result(int query_instance_size, int view_instance_size, String time, HashMap<Integer, HashMap<Integer, String>> results)
  {
    if(results.get(query_instance_size) == null)
    {
      HashMap<Integer, String> curr_res = new HashMap<Integer, String>();
      
      String time_vec = new String();
      
      time_vec = time;
      
      curr_res.put(view_instance_size, time_vec);
      
      results.put(query_instance_size, curr_res);
    }
    else
    {
      if(results.get(query_instance_size).get(view_instance_size) == null)
      {
        String time_vec = new String();
        
        time_vec = time;
        
        results.get(query_instance_size).put(view_instance_size, time_vec);
      }
    }
  }
  
  static String compose_string(int query_instance_size, int view_instance_size, double time1, double time2, String instance_size)
  {
    String curr_res = new String();
  
    curr_res = String.valueOf(query_instance_size) + "," + String.valueOf(view_instance_size) + "," + String.valueOf(time1) + "," + String.valueOf(time2) + "," + instance_size;
    
    return curr_res;
    //  
//  results.add(curr_res);
  }
  
  static void cal_average(HashMap<Integer, HashMap<Integer, Vector<Double>>> materialized_results, HashMap<Integer, HashMap<Integer, Vector<Double>>> non_materialized_results, HashMap<Integer, HashMap<Integer, String>> materialized_view_size_results, Vector<String> results)
  {
    Set<Integer> query_instance_size_sets = materialized_results.keySet();
    
    Vector<Integer> query_instance_size_vec = new Vector<Integer>();
    
    query_instance_size_vec.addAll(query_instance_size_sets);
    
    Collections.sort(query_instance_size_vec);
    
    for(Integer query_instance_size : query_instance_size_vec)
    {
      HashMap<Integer, Vector<Double>> view_instance_size_res1 = materialized_results.get(query_instance_size);
      
      HashMap<Integer, Vector<Double>> view_instance_size_res2 = non_materialized_results.get(query_instance_size);
      
      HashMap<Integer, String> view_instance_size_mappings = materialized_view_size_results.get(query_instance_size);
      
      Set<Integer> view_instance_size_sets = view_instance_size_res1.keySet();
      
      Vector<Integer> view_instance_size_vec = new Vector<Integer>();
      
      view_instance_size_vec.addAll(view_instance_size_sets);
      
      Collections.sort(view_instance_size_vec);
      
      for(Integer view_instance_size: view_instance_size_vec)
      {
        Vector<Double> curr_time_sets = view_instance_size_res1.get(view_instance_size);
        
        Vector<Double> curr_time_sets2 = view_instance_size_res2.get(view_instance_size);
        
        String instance_size = view_instance_size_mappings.get(view_instance_size);
        
        remove_outlier(curr_time_sets);
        
        remove_outlier(curr_time_sets2);
        
        
        double avg1 = 0.0;
        
        double avg2 = 0.0;
        
        for(double curr_time_set: curr_time_sets)
        {
           avg1 += curr_time_set;
        }
        
        for(double curr_time_set: curr_time_sets2)
        {
           avg2 += curr_time_set;
        }
        
        avg1 = avg1/curr_time_sets.size();
        
        avg2 = avg2/curr_time_sets2.size();
        
        results.add(compose_string(query_instance_size, view_instance_size, avg1, avg2, instance_size));
      }
      
    }
  }
  
  static void remove_outlier(Vector<Double> values)
  {
    Collections.sort(values);
    
    System.out.println(values);
    
    double q1 = values.get((int) Math.floor((((double)values.size()) / 4)));
    // Likewise for q3. 
    double q3 = values.get((int) Math.ceil(((double)values.size() * 3 / 4)));
    double iqr = q3 - q1;

    // Then find min and max values
    double maxValue = q3 + iqr*1.5;
    double minValue = q1 - iqr*1.5;
    
    for(int i = 0; i<values.size(); i++)
    {
      double value = values.get(i);
      
      if((value > maxValue) || (value < minValue))
      {
        values.remove(i);
        
        i--;
      }
    }
    
    System.out.println(String.valueOf(maxValue) + "::" + minValue + "::" + q1 + "::" + q3 + "::" + iqr);
    
    System.out.println(values);
    
  }
  
  static Vector<String> process(String file)
  {
    Vector<String> results = new Vector<String>();
    
    Vector<String> prefix_vector = new Vector<String>();
    
    prefix_vector.addAll(Arrays.asList(prefixes));
    
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;

      String query_instance_size_prefix = "query_final_size::";
      
      String view_instance_size_prefix = "view_final_size::";
      
      String materialized_false = "materialized::false";
      
      String materialized_true = "materialized::true";
      
      String prefix_prov = "reasoning time 3:";
      
      String view_size_prefix = "view_instance_size_mappings::";
      
      int state = 0;
      
      int query_instance_size = -1;
      
      int view_instance_size = -1;

      Vector<Double> non_materialized_time = new Vector<Double>();
      
      Vector<Double> materialized_time = new Vector<Double>();
      
      double time_prov_1 = 0.0;
      
      double time_prov_2 = 0.0;
      
      double time_tla_1 = 0.0;
      
      double time_tla_2 = 0.0;
      
      int temp1 = -1;
      
      int temp2 = -1;
      
      String curr_view_size_result = new String();
      
      HashMap<Integer, HashMap<Integer, Vector<Double>>> materialized_results = new HashMap<Integer, HashMap<Integer, Vector<Double>>>();
      
      HashMap<Integer, HashMap<Integer, Vector<Double>>> non_materialized_results = new HashMap<Integer, HashMap<Integer, Vector<Double>>>();
      
      HashMap<Integer, HashMap<Integer, String>> materialized_view_size_results = new HashMap<Integer, HashMap<Integer, String>>();
      
      while ((line = br.readLine()) != null) {
         // process the line.
//        System.out.println(line);
        switch(state)
        {
          case 0:
          {
            if(line.startsWith(query_instance_size_prefix))
            {
                query_instance_size = (int)get_value(query_instance_size_prefix, line);
                
                System.out.println("query_instance_size::" + query_instance_size);
                
                state = 1;
                
            }
            break;
          }
          
          case 1:
          {
            if(line.startsWith(view_instance_size_prefix))
            {
                view_instance_size = (int) get_value(view_instance_size_prefix, line);
                
                System.out.println("view_instance_size::" + view_instance_size);
                
                state = 2;
                
            }
            break;
          }
          
          case 2:
          {
            if(line.startsWith(materialized_false))
            {
                state = 3;
            }
            break;
          }
          case 3:
          {
            if(line.startsWith(prefix_prov))
            {
                non_materialized_time.add(get_value(prefix_prov, line));
                
                System.out.println("non_materialized_time::" + non_materialized_time);
                
                state = 4;
                
            }
            break;
          }
          case 4:
          {
            
            if(line.startsWith(prefix_prov))
            {
                non_materialized_time.add(get_value(prefix_prov, line));
                
                System.out.println("non_materialized_time::" + non_materialized_time);
                
                state = 4;
                
            }
            else
            {
              if(line.startsWith(materialized_true))
              {
                  state = 5;
              }
            }
            break;
          }
          case 5:
          {
            if(line.startsWith(prefix_prov))
            {
              materialized_time.add(get_value(prefix_prov, line));
              
              System.out.println("materialized_time::" + materialized_time);
              
              state = 6;
              
//              String curr_res = new String();
//              
//              curr_res = String.valueOf(query_instance_size) + "," + String.valueOf(view_instance_size) + "," + String.valueOf(non_materialized_time) + "," + String.valueOf(materialized_time);
//              
//              results.add(curr_res);
            }
            
            break;
          }
          
          case 6:
          {
            
            if(line.startsWith(prefix_prov))
            {
              materialized_time.add(get_value(prefix_prov, line));
              
              System.out.println("materialized_time::" + materialized_time);
              
              state = 6;
              
//              String curr_res = new String();
//              
//              curr_res = String.valueOf(query_instance_size) + "," + String.valueOf(view_instance_size) + "," + String.valueOf(non_materialized_time) + "," + String.valueOf(materialized_time);
//              
//              results.add(curr_res);
            }
            else
            {
              
              if(line.startsWith(view_size_prefix))
              {
                curr_view_size_result = line.substring(view_size_prefix.length(), line.length());
              }
              else
              {
                if(line.startsWith(query_instance_size_prefix))
                {
                  temp1 = (int) get_value(query_instance_size_prefix, line);
                  
                  state = 7;
                }
                else
                {
                  if(line.startsWith(view_instance_size_prefix))
                  {
                    process_result(query_instance_size, view_instance_size, materialized_time, materialized_results);
                    
                    process_result(query_instance_size, view_instance_size, non_materialized_time, non_materialized_results);
                    
                    process_result(query_instance_size, view_instance_size, curr_view_size_result, materialized_view_size_results);
                    
                    view_instance_size = (int) get_value(view_instance_size_prefix, line);
                    
                    state = 2;
               
                    materialized_time.clear();
                    
                    non_materialized_time.clear();
                  }
                }
              }
            }
            
            break;
          }
          
          case 7:
          {
            
            
            if(line.startsWith(view_instance_size_prefix))
            {
              temp2 = (int) get_value(view_instance_size_prefix, line);
              
              process_result(query_instance_size, view_instance_size, materialized_time, materialized_results);
              
              process_result(query_instance_size, view_instance_size, non_materialized_time, non_materialized_results);
              
              process_result(query_instance_size, view_instance_size, curr_view_size_result, materialized_view_size_results);
              
              query_instance_size = temp1;
              
              view_instance_size = temp2;
              
              state = 2;
              
              materialized_time.clear();
              
              non_materialized_time.clear();
              
            }
            
            break;
          }
        }
        
//        if(query_instance_size > 0 && state == 5)
//        {
//          String curr_result = String.valueOf(query_instance_size);
//          
//          curr_result += "," + time_prov_1 + "," + time_prov_2 + "," + time_tla_1 + "," + time_tla_2;
//          
//          System.out.println(curr_result);
//          
//          results.add(curr_result);
//        }
        
        
              
      }

      process_result(query_instance_size, view_instance_size, materialized_time, materialized_results);
      
      process_result(query_instance_size, view_instance_size, non_materialized_time, non_materialized_results);
      
      process_result(query_instance_size, view_instance_size, curr_view_size_result, materialized_view_size_results);
      
      cal_average(materialized_results, non_materialized_results, materialized_view_size_results, results);
      
  } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
  } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
  }
    
    
    return results;
  }
  
  static double get_value(String prefix, String line)
  {
    String value_string = line.substring(prefix.length(), line.length());
    
    return Double.valueOf(value_string);
  }

  
  static void process(String file, String f1)
  {
      
      Vector<String> content1 = new Vector<String>();
              
      int num1 = 0;
      
      int num2 = 0;
      
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          String line;
          
         
          
          
          
          while ((line = br.readLine()) != null) {
             // process the line.
//            String[] str = line.split(" ");
//            
//            if(StringUtils.isNumeric(str[0]) && line.length() > 10)
//            {
//                content1.add(line);
//                
//                num1 ++;
//            }
//            else
              {
                  if(line.startsWith("original_relation_size::"))
                  {
                      String prefix = "original_relation_size::";
                      
                      
                      line = line.substring(prefix.length(), line.length());
                      
                      content1.add(line);
                      
                      num2++;
                  }
              }
          }
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      write(f1, content1);
              
      System.out.println(content1.size());
      
  }
  
  static void process(String file, String f1, String f2)
  {
      
      Vector<String> content1 = new Vector<String>();
      
      Vector<String> content2 = new Vector<String>();
      
      int num1 = 0;
      
      int num2 = 0;
      
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          String line;
          
         
          
          
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String[] str = line.split(" ");
              
              if(StringUtils.isNumeric(str[0]) && line.length() > 10)
              {
                  content1.add(line);
                  
                  num1 ++;
              }
              else
              {
                  if(line.startsWith("execution time"))
                  {
                      String prefix = "execution time::";
                      
                      
                      line = line.substring(prefix.length(), line.length());
                      
                      content2.add(line);
                      
                      num2++;
                  }
              }
          }
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      write(f1, content1);
      
      write(f2, content2);
      
      System.out.println(content1.size());
      
      System.out.println(content2.size());
  }
  
  static void process(String file, String f1, String f2, String f3)
  {
      
      Vector<String> content1 = new Vector<String>();
      
      Vector<String> content2 = new Vector<String>();
      
      Vector<String> content3 = new Vector<String>();
      
      int num1 = 0;
      
      int num2 = 0;
      
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          String line;
          
         
          
          
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String[] str = line.split(" ");
              
              if(StringUtils.isNumeric(str[0]) && line.length() > 10)
              {
                  
                  if(num1 % 20 < 10)
                  {
                      content1.add(line);
                  }
                  else
                  {
                      content2.add(line);
                  }
                  
                  num1 ++;
              }
              else
              {
                  if(line.startsWith("execution time"))
                  {
                      String prefix = "execution time::";
                      
                      
                      line = line.substring(prefix.length(), line.length());
                      
                      content3.add(line);
                      
                      num2++;
                  }
              }
          }
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      write(f1, content1);
      
      write(f2, content2);
      
      write(f3, content3);
      
      System.out.println(content1.size());
      
      System.out.println(content2.size());
  }
  
  
  static void write(String file_name, Vector<String> line)
  {
       BufferedWriter bw = null;
        try {
           //Specify the file name and path here
       File file = new File(file_name);

       /* This logic will make sure that the file 
        * gets created if it is not present at the
        * specified location*/
        if (!file.exists()) {
           file.createNewFile();
        }

        FileWriter fw = new FileWriter(file);
        bw = new BufferedWriter(fw);
        
        for(int i = 0; i<line.size(); i++)
        {
            bw.append(line.get(i));
            bw.newLine();
        }
                
        bw.close();


        
        } catch (IOException ioe) {
         ioe.printStackTrace();
      }
        
  }
  
  static void process_text_full_case(String file_name, String f1, String f2, String f3, String f4, String f5)
  {
      Vector<Vector<Double>> tuple_level_agg_intersection = new Vector<Vector<Double>>();
      
      Vector<Vector<Double>> tuple_level_agg_union = new Vector<Vector<Double>>();
      
      Vector<Vector<Double>> semi_schema_agg_intersection = new Vector<Vector<Double>>();
      
      Vector<Vector<Double>> semi_schema_agg_union = new Vector<Vector<Double>>();
      
      Vector<Vector<Double>> schema = new Vector<Vector<Double>>();
      
      int m = 3;
      
      try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
          String line;            
          
          int num = 0;
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String[] str = line.split(" ");
              
              if(StringUtils.isNumeric(str[0]) && line.length() > 10)
              {
                  Vector<Double> curr_values = new Vector<Double>();
                  
//                System.out.println(num + "::" + line);
                  
                  for(int i = 0; i<str.length; i++)
                  {
                      if(str[i].contains("::"))
                      {
                          String num_val = str[i].split("::")[1];
                          
//                        System.out.print(str[i].split("::")[0] + "  ");
                          
                          if(isNumeric(num_val))
                          {
                              double value = Double.valueOf(num_val);
                              
                              curr_values.add(value);
                          }
                      }
                  }
                  
//                System.out.println(curr_values);
                  
                  if(num % (5*m) <m)
                  {
                      tuple_level_agg_intersection.add(curr_values);
                  }
                  else
                  {
                      if(num % (5*m) <2*m && num % (5*m) >=m)
                      {
                          tuple_level_agg_union.add(curr_values);
                      }
                      else
                      {
                          if(num % (5*m) < 3*m && num % (5*m) >= 2*m)
                          {
                              semi_schema_agg_intersection.add(curr_values);
                          }
                          else
                          {
                              if(num % (5*m) < 4*m && num % (5*m) >= 3*m)
                              {
                                  semi_schema_agg_union.add(curr_values);
                                  
//                                System.out.println(num + " " + curr_values);
                              }
                              else
                              {
                                  schema.add(curr_values);
                              }
                          }
                      }
                  }
                  
                  num ++;

              }
              
          }
          
          
          
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      Vector<Vector<Double>> tuple_level_agg_intersection_average = cal_average(tuple_level_agg_intersection, m);         
      
      Vector<Vector<Double>> tuple_level_agg_union_average = cal_average(tuple_level_agg_union, m);

      Vector<Vector<Double>> semi_schema_agg_intersection_average = cal_average(semi_schema_agg_intersection, m);

      Vector<Vector<Double>> semi_schema_agg_union_average = cal_average(semi_schema_agg_union, m);
      
      Vector<Vector<Double>> schema_average = cal_average(schema, m);
      
      write(f1, covert2vec_str(tuple_level_agg_intersection_average));
      
      write(f2, covert2vec_str(tuple_level_agg_union_average));
      
      write(f3, covert2vec_str(semi_schema_agg_intersection_average));
      
      write(f4, covert2vec_str(semi_schema_agg_union_average));
      
      write(f5, covert2vec_str(schema_average));
  }
  
  static void process_text_min_case(String file_name, String f1, String f2, String f3)
  {
      Vector<Vector<Double>> tuple_level = new Vector<Vector<Double>>();
              
      Vector<Vector<Double>> semi_schema = new Vector<Vector<Double>>();
              
      Vector<Vector<Double>> schema = new Vector<Vector<Double>>();
      
      int m = 10;
      
      try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
          String line;            
          
          int num = 0;
          
          while ((line = br.readLine()) != null) {
             // process the line.
              String[] str = line.split(" ");
              
              if(StringUtils.isNumeric(str[0]) && line.length() > 10)
              {
                  Vector<Double> curr_values = new Vector<Double>();
                  
//                System.out.println(num + "::" + line);
                  
                  for(int i = 0; i<str.length; i++)
                  {
                      if(str[i].contains("::"))
                      {
                          String num_val = str[i].split("::")[1];
                          
                          System.out.print(str[i].split("::")[0] + "  ");
                          
                          if(isNumeric(num_val))
                          {
                              double value = Double.valueOf(num_val);
                              
                              curr_values.add(value);
                          }
                      }
                  }
                  
                  System.out.println();
                  
                  if(num % (2*m) <m)
                  {
                      tuple_level.add(curr_values);
                  }
                  else
                  {
                      
                      semi_schema.add(curr_values);
                                          
                  }
                  
                  num ++;

              }
              
              if(str[0].startsWith("execution time::"))
              {
                  Vector<Double> curr_values = new Vector<Double>();
                  
//                System.out.println(num + "::" + line);
                  
                  for(int i = 0; i<str.length; i++)
                  {
                      if(str[i].contains("::"))
                      {
                          String num_val = str[i].split("::")[1];
                          
                          System.out.print(str[i].split("::")[0] + "  ");
                          
                          if(isNumeric(num_val))
                          {
                              double value = Double.valueOf(num_val);
                              
                              curr_values.add(value);
                          }
                      }
                  }
                  
                  schema.add(curr_values);
                  
                  System.out.println();
              }
              
          }
          
          
          
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
      Vector<Vector<Double>> tuple_level_average = cal_average(tuple_level, m);           
              
      Vector<Vector<Double>> semi_schema_average = cal_average(semi_schema, m);
              
      Vector<Vector<Double>> schema_average = cal_average(schema, m);
      
      write(f1, covert2vec_str(tuple_level_average));
              
      write(f2, covert2vec_str(semi_schema_average));
      
      write(f3, covert2vec_str(schema_average));

  }
  
  public static boolean isNumeric(String str)  
  {  
    try  
    {  
      double d = Double.parseDouble(str);  
    }  
    catch(NumberFormatException nfe)  
    {  
      return false;  
    }  
    return true;  
  }
  
  static Vector<String> covert2vec_str(Vector<Vector<Double>> values)
  {
      Vector<String> value_strs = new Vector<String>();
      
      
      for(int i = 0; i<values.size(); i++)
      {
          Vector<Double> curr_values = values.get(i);
          
          String curr_value_str = new String();
          
          for(int j = 0; j<curr_values.size(); j++)
          {
              
              if(j >= 1)
                  curr_value_str += ",";
              
              curr_value_str += curr_values.get(j);
          }
          
          value_strs.add(curr_value_str);
      }
      
      return value_strs;
  }
  
  static Vector<Vector<Double>> cal_average(Vector<Vector<Double>> values, int num)
  {
      Vector<Vector<Double>> averages = new Vector<Vector<Double>>();
      
      for(int i = 0; i<values.size()/num; i++)
      {
          
          Vector<Double> average = new Vector<Double>();
          
          for(int j = 0; j<values.get(0).size(); j++)
          {
              if(i *num + num - 1 < values.size())
              {
                  
                  double value = 0;
                  
                  for(int k = 0; k<num; k++)
                  {
                      value += values.get(i * num + k).get(j);
                  }
                  
                  value = value / num;
                                      
                  average.add(value);
              }
          }
          
          if(!average.isEmpty())
              averages.add(average);
      }
      
      return averages;
  }


}
