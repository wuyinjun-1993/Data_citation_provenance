package edu.upenn.cis.citation.util;

public class Bit_operation {
  
  public static long[] init(int length)
  {
    long [] bit_sequence = new long[(length + Long.SIZE - 1)/Long.SIZE];
    
    return bit_sequence;
  }
  
  
  public static void set_bit(long [] bit_sequence, int index)
  {
    bit_sequence[index/Long.SIZE] |= (1L << (index % Long.SIZE));
  }

  
  public static void unset_bit(long [] bit_sequence, int index)
  {
    bit_sequence[index/Long.SIZE] &= ~(1L << (index % Long.SIZE));
  }
  
  
  public static long[] create_index(int length)
  {
    return new long[(length + Long.SIZE - 1)/Long.SIZE];
  }
  
  public static int  bitCount(long i) {
    i = i - ((i >>> 1) & 0x5555555555555555L);
    i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
    i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
    i = i + (i >>> 8);
    i = i + (i >>> 16);
    i = i + (i >>> 32);
    return (int)i & 0x7f;
 }
 
 public static int numberOfSetBits(long[] num)
 {
   int number_set_bit = 0;
   
   for(int i = 0; i<num.length; i++)
   {
     number_set_bit += bitCount(num[i]);
   }
   
   return number_set_bit;
 }
 
 public static long[] clone_array(long[] array1)
 {
   long[] array2 = new long[array1.length]; 
   
   System.arraycopy(array1, 0, array2, 0, array1.length);
   
   return array2;
 }
 
 public static void and(long [] arr1, long []arr2)
 {
   for(int i = 0; i<arr1.length; i++)
   {
     arr1[i] = arr1[i] & arr2[i];
   }
 }
 
 public static void setAllzeros(long []arr)
 {
   for(int i = 0; i<arr.length; i++)
   {
     arr[i] = 0L;
   }
 }
}
