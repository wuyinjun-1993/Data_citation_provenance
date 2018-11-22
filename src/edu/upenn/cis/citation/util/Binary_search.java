package edu.upenn.cis.citation.util;

import java.util.Vector;

public class Binary_search {
  
  static int binarySearch(int arr[], int x) 
  { 
      int l = 0, r = arr.length - 1; 
      while (l <= r) 
      { 
          int m = l + (r-l)/2; 

          // Check if x is present at mid 
          if (arr[m] == x) 
              return m; 

          // If x greater, ignore left half 
          if (arr[m] < x) 
              l = m + 1; 

          // If x is smaller, ignore right half 
          else
              r = m - 1; 
      } 

      // if we reach here, then element was  
      // not present 
      return -(r+1); 
  } 
  
  public static int binarySearch(Vector<String> arr, String x) 
  { 
      int l = 0, r = arr.size() - 1; 
      while (l <= r) 
      { 
          int m = l + (r-l)/2; 

          // Check if x is present at mid 
          if (arr.get(m).compareTo(x) == 0 ) 
              return m; 

          // If x greater, ignore left half 
          if (arr.get(m).compareTo(x) < 0) 
              l = m + 1; 

          // If x is smaller, ignore right half 
          else
              r = m - 1; 
      } 

      // if we reach here, then element was  
      // not present 
      return -(r+1); 
  }
  
  public static boolean check_exists(Vector<String> arr, String x, int pos)
  {
    if(arr.isEmpty())
      return false;
    
    if(pos < 0)
      return false;
    if(pos == 0)
    {
      if(!arr.get(0).equals(x))
        return false;
    }
    
    return true;
  }
  
  public static void insert2list(Vector<String> arr, Vector<long[]> indexes, String x, long[] new_index, int pos)
  {
    if(arr.isEmpty())
    {
      arr.add(x);
      indexes.add(new_index);
    }
    
//    int pos = binarySearch(arr, x);
    
    if(pos < 0)
    {
//      arr.setSize(arr.size() + 1);
//      
//      indexes.setSize(indexes.size() + 1);
//      
//      System.arraycopy(arr, -pos, arr, -pos + 1, arr.size() - (-pos));
//      
//      arr.setElementAt(x, -pos);
      arr.insertElementAt(x, -pos);
//      System.arraycopy(indexes, -pos, indexes, -pos + 1, indexes.size() - (-pos));
//      
//      indexes.setElementAt(new_index, -pos);
      indexes.insertElementAt(new_index, -pos);
    }
    else
    {
      if(pos == 0)
      {
        if(!arr.get(0).equals(x))
        {
//          arr.setSize(arr.size() + 1);
//          
//          indexes.setSize(indexes.size() + 1);
//          
//          System.arraycopy(arr, -pos, arr, -pos + 1, arr.size() - (-pos));
//          
//          arr.setElementAt(x, -pos);
          arr.insertElementAt(x, -pos);
          
          indexes.insertElementAt(new_index, -pos);
//          System.arraycopy(indexes, -pos, indexes, -pos + 1, indexes.size() - (-pos));
//          
//          indexes.setElementAt(new_index, -pos);
        }
      }
    }
  }
  
  
  public static void main(String args[]) 
  { 
      int arr[] = {2, 3, 4, 10, 40}; 
      int n = arr.length; 
      int x = 10; 
      int result = binarySearch(arr, 2); 
      System.out.println(result);
  } 
  
  

}
