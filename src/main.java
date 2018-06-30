import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Vector;
import edu.upenn.cis.citation.Corecover.Query;
import edu.upenn.cis.citation.examples.Load_views_and_citation_queries;
import edu.upenn.cis.citation.views.Query_converter;

public class main {
  
  static String input_file = "real_example/views";
  
  static String usr_name = "wuyinjun";
  
  static String passwd = "12345678";
  
  static String db_url = "jdbc:postgresql://localhost:5432/" + "genecode";
  
  static String output_file = "real_example/view_strings";
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException
  {
    Class.forName("org.postgresql.Driver");
    Connection c = DriverManager
       .getConnection(db_url, usr_name, passwd);
    PreparedStatement pst = null;
    
    Vector<Query> views = Load_views_and_citation_queries.get_views(input_file, c, pst);
    
    Vector<String> view_strings = new Vector<String>();
    
    for(int i = 0; i<views.size(); i++)
    {
      view_strings.add(Query_converter.datalog2sql(views.get(i), false));
    }
    
    write2files(output_file, view_strings);
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
