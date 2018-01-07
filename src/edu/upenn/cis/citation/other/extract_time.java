package edu.upenn.cis.citation.other;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Vector;

public class extract_time {
  
  public static void main(String [] args) throws IOException
  {
    input_files("prov_result_single_thread.txt");
    
    input_files("prov_result.txt");
  }
  
  static void input_files(String file_name) throws IOException
  {

    Vector<Double> time1 = new Vector<Double>();
    
    Vector<Double> time2 = new Vector<Double>();
    
    try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
        String line;
        
        while ((line = br.readLine()) != null) {
           // process the line.
            
          if(line.startsWith("reasoning_time"))
          {
            String time_str = line.split("::")[1];
            
            double time = Double.valueOf(time_str);
            
            time1.add(time);
          }
          
          if(line.startsWith("reasoning time 2"))
          {
            String time_str = line.split(":")[1];
            
            double time = Double.valueOf(time_str);
            
            time2.add(time);
          }
          
        }
    } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    
    output2file(file_name + "1", time1, time2);
  }
  
  static void output2file(String file_name, Vector<Double> time1, Vector<Double> time2) throws IOException
  {
    File fout = new File(file_name);
    FileOutputStream fos = new FileOutputStream(fout);
 
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
 
    for (int i = 0; i<time2.size(); i++) {
        bw.write(time1.get(i) + "," + time2.get(i));
        bw.newLine();
    }
 
    bw.close();
  }

}
