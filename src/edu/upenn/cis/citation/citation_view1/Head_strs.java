package edu.upenn.cis.citation.citation_view1;

import java.util.Vector;

import edu.upenn.cis.citation.Corecover.Argument;

public class Head_strs {
	
	public Vector<String> head_vals;
	
	 static String parser = "####";
	 
	String output_string = new String();
	
	public Head_strs(Vector<String> vec_str)
	{
		
//		head_vals = new Vector<String> ();
//		this.head_vals.addAll(vec_str);
	  this.head_vals = vec_str;
		
		output_string = compute_string();
	}
	
	public static String concatenate_strings(StringBuilder sb, String s1, String s2)
	{
	  sb.append(s1);
	  
	  sb.append(parser);
	  
	  sb.append(s2);
	  
	  String res = sb.toString();
	  
	  sb.setLength(0);
	  
	  return res;
	}
	
	
	@Override
	public boolean equals(Object obj)
	{
		Head_strs vec_str = (Head_strs) obj;
		
		return output_string.equals(vec_str.output_string);
		
//		if(head_vals.size() != vec_str.head_vals.size())
//			return false;
//		
//		
//		for(int i = 0; i<head_vals.size(); i++)
//		{
//			if(head_vals.get(i) == null && vec_str.head_vals.get(i) == null)
//				continue;
//			
//			
//			if(head_vals.get(i) == null && vec_str.head_vals.get(i) != null)
//				return false;
//			
//			if(head_vals.get(i) != null && vec_str.head_vals.get(i) == null)
//				return false;
//			
//			if(!head_vals.get(i).equals(vec_str.head_vals.get(i)))
//				return false;
//		}
//		
//		return true;
		
	}
	
	@Override
	public int hashCode() {
		
//		int hashCode = 0;
//		
//	    for (int i = 0; i < head_vals.size(); i ++) {
//	      hashCode += head_vals.get(i).hashCode();
//	    }
	    
	    return toString().hashCode();
	  }
	
	
	String compute_string()
	{
	  String output = new String();
      
      for(int i = 0; i<head_vals.size(); i++)
      {
          if(i >= 1)
              output += parser;
          
          output += head_vals.get(i);
      }
      
      return output;
	}
	
	@Override
	public String toString()
	{
		
		return output_string;
	}
	
	public void clear()
	{
		this.head_vals.clear();
	}

}
