package edu.upenn.cis.citation.multi_thread;

public class test_multi_thread {

  public static void main(String args[]) throws InterruptedException {
    RunnableDemo  R1 = new RunnableDemo ( "Thread-1");
     
     
     RunnableDemo  R2 = new RunnableDemo ( "Thread-2");
     
     R1.start();
     
     R2.start();
     
     R1.join();
     
     R2.join();
     
     R1.start();
     
     R2.start();
  }   
}
