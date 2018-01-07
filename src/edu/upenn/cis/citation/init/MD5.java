package edu.upenn.cis.citation.init;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.swing.JPopupMenu.Separator;

public class MD5 {
  public static String getMD5(String input) {
      try {
          MessageDigest md = MessageDigest.getInstance("MD5");
          byte[] messageDigest = md.digest(input.getBytes());
          BigInteger number = new BigInteger(1, messageDigest);
          String hashtext = number.toString(16);
          // Now we need to zero pad it if you actually want the full 32 chars.
          while (hashtext.length() < 32) {
              hashtext = "0" + hashtext;
          }
          return hashtext;
      }
      catch (NoSuchAlgorithmException e) {
          throw new RuntimeException(e);
      }
  }

  public static void main(String[] args) throws NoSuchAlgorithmException {
      System.out.println(getMD5("Javarmi.com"));
  }
  
  public static String get_MD5_encoding(String relation)
  {
    return getMD5(relation);
  }
  
  public static String get_MD5_encoding(String relation, String attribute)
  {
    return getMD5(relation + init.separator + attribute);
  }
}