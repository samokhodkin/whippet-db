package io.github.whippetdb.test.misc;

import java.util.Random;

public class MemTest {
   public static void main(String[] args) throws Exception {
      int size = 128 << 20;
      int mask = size - 1;
      long[] keys = new long[size];
      Random r = new Random();
      for(int i = 0 ; i  < keys.length; i++) {
         keys[i] = r.nextLong();
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
