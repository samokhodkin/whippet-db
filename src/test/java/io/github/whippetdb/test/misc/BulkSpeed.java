package io.github.whippetdb.test.misc;

import java.io.IOException;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleFileDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;

@SuppressWarnings("unused")
public class BulkSpeed {
   public static void main(String[] args) throws IOException {
      //MemDataIO d1 = new SimpleFileDataBuffer("tmp", 10);
      //MemDataIO d2 = new SimpleFileDataBuffer("tmp", 10);
      MemDataIO d1 = new SimpleDirectDataBuffer(10);
      MemDataIO d2 = new SimpleDirectDataBuffer(10);
      MemDataIO h1 = new SimpleHeapDataBuffer(10);
      MemDataIO h2 = new SimpleHeapDataBuffer(10);
      byte[] b1 = new byte[]{1,2,3,4,5,6,7,8,9,0};
      byte[] b2 = new byte[10];
      
      int N = 100_000_000;
      
      d1.writeLong(0, 0x0807060504030201L);
      h1.writeLong(0, 0x0807060504030201L);
      
//      System.out.println(runCopy(h1, d2, 2*N));
//      System.exit(0);
      
      
      
//      System.out.println(d1);
//      runCopy(d1, d2, 1);
//      System.out.println(d2);
//      runCopy(h1, d2, 1);
//      System.out.println(d2);
//      System.exit(0);
      
      System.out.println("d1 -> d2: " + runCopy(d1, d2, N));
      System.out.println("d1 -> h2: " + runCopy(d1, h2, N));
      System.out.println("h1 -> d2: " + runCopy(h1, d2, N));
      System.out.println("h1 -> h2: " + runCopy(h1, h2, N));
      System.out.println("b1 -> b2: " + runCopy(b1, b2, N));
      
      System.out.println("d1 == d2: " + runEquals(d1, d2, N));
      System.out.println("d1 == h2: " + runEquals(d1, h2, N));
      System.out.println("h1 == d2: " + runEquals(h1, d2, N));
      System.out.println("h1 == h2: " + runEquals(h1, h2, N));
      System.out.println("b1 == b2: " + runEquals(b1, b2, N));
   }
   
   static long runCopy(MemDataIO m1, MemDataIO m2, int N) {
      long t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         m2.write(0, m1, 0, 8);
      }
      return System.currentTimeMillis() - t0;
   }
   
   static long runEquals(MemDataIO m1, MemDataIO m2, int N) {
      long t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         if(!m2.equals(0, m1, 0, 8)) throw new Error();
      }
      return System.currentTimeMillis() - t0;
   }
   
   static long runCopy(byte[] b1, byte[] b2, int N) {
      long t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         System.arraycopy(b1, 0, b2, 0, 8);
      }
      return System.currentTimeMillis() - t0;
   }
   
   static long runEquals(byte[] b1, byte[] b2, int N) {
      long t0 = System.currentTimeMillis();
      for(int n = N; n --> 0;) {
         for(int i = 0; i < 8; i++) if(b1[i] != b2[i]) throw new Error();
      }
      return System.currentTimeMillis() - t0;
   }
}
