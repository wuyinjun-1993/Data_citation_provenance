import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import org.roaringbitmap.RoaringBitmap;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.views.Query_converter;

public class main {
  
  static String input_file = "real_example/views";
  
  static String usr_name = "wuyinjun";
  
  static String passwd = "12345678";
  
  static String db_url = "jdbc:postgresql://localhost:5432/" + "genecode";
  
  static String output_file = "real_example/view_strings";
  
  public static int[] sum_two_array2(int[] a, int[]b)
  {
    int[] result = new int[a.length];
    
    Arrays.setAll(result, i -> a[i] + b[i]);
    
    return result;
  }
  
  public static int[] sum_two_array1(int[] a, int[]b)
  {
    int[] result = new int[a.length];
    
    for(int i = 0; i<result.length; i++)
    {
      result[i] = a[i] + b[i];
    }
    
    return result;
  }
  
  
  public static void test(int length, int rep)
  {
    int [] a = new int[length];
    int [] b = new int[length];
    
    Random r = new Random();
    
    for(int i = 0; i<length; i++)
    {
      a[i] = r.nextInt();
    }
    
    for(int i = 0; i<length; i++)
    {
      b[i] = r.nextInt();
    }
    
    double t1 = System.nanoTime();
    
    for(int i = 0; i<rep; i++)
    {
      sum_two_array1(a, b);
    }
    
    double t2 = System.nanoTime();
    
    for(int i = 0; i<rep; i++)
    {
      sum_two_array2(a, b);
    }
    
    double t3 = System.nanoTime();
    
    double time1 = (t2 - t1)*1.0/1000000000;
    
    double time2 = (t3 - t2)*1.0/1000000000;
    
    System.out.println("time1::" + time1);
    
    System.out.println("time2::" + time2);
    
    
  }
  
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException
  {
    
    StringBuilder sb = new StringBuilder();
    
    sb.append("123");
    
    sb.append("456");
    
    System.out.println(sb.toString());
    
    sb.delete(0, sb.toString().length());
    
    System.out.println(sb.toString().length());
    
    
//    test(10000000,1000);
//    Class.forName("org.postgresql.Driver");
//    Connection c = DriverManager
//       .getConnection(db_url, usr_name, passwd);
//    PreparedStatement pst = null;
//    
//    Vector<Query> views = Load_views_and_citation_queries.get_views(input_file, c, pst);
//    
//    Vector<String> view_strings = new Vector<String>();
//    
//    for(int i = 0; i<views.size(); i++)
//    {
//      view_strings.add(Query_converter.datalog2sql(views.get(i), false));
//    }
//    
//    write2files(output_file, view_strings);
  }
  
  public static void write2files(String file_name, Vector<String> views)
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
    
    for(int i = 0; i<views.size(); i++)
    {
      bw.append(views.get(i));
      bw.newLine();
    }
    
    bw.close();

    } catch (IOException ioe) {
     ioe.printStackTrace();
  }
    
    
  }

}
