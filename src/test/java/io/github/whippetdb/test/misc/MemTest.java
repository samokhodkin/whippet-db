package io.github.whippetdb.test.misc;

import java.util.Random;

import io.github.whippetdb.util.FastHash;

public class MemTest {
   static Random r = new Random();
   
   /**
    * measure speed of non-cached reads
    */
   public static void main(String[] args) throws Exception {
      int size = 128 << 20;
      int mask = size - 1;
      long[] keys = new long[size];
      
      
      for(int i = 0 ; i  < keys.length; i++) {
         keys[i] = FastHash.hash64(i);
         //keys[i] = r.nextLong();
      }
      
      long t0, dt;
      System.out.println("started");
      
      t0 = System.currentTimeMillis();
      long v = 0;
      long N = size * 3;
      for(long i = N; i --> 0;) {
         v = keys[(int) (v & mask)];
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((1000f*N/dt) + " jumps/sec");
   }
}
