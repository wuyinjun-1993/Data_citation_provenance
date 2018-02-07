package edu.upenn.cis.citation.bit_operation;

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
}
