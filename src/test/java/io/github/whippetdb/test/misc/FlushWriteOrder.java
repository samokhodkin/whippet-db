package io.github.whippetdb.test.misc;

import java.io.IOException;
import java.util.Arrays;

import io.github.whippetdb.memory.basic.SimpleFileDataBuffer;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;

/**
 * Demonstrates how random write speed depends on flush intervals 
 */

@SuppressWarnings("unused")
public class FlushWriteOrder {
   public static void main(String[] args) throws IOException, InterruptedException {
      long[] addresses1 = new long[16 << 20]; // 16m addresses, 128mb
      
      // initialize addresses1 sequentially 
      for(int i = 0; i < addresses1.length; i++)  addresses1[i] = i * 8;
      
      // same addresses but shuffled 
      long[] addresses2 = (long[]) addresses1.clone();
      shuffle(addresses2);
      
      SimpleFileDataBuffer mem = new SimpleFileDataBuffer("tmp", addresses1.length * 8);
      
      // flush after each 1<<lgFlushSize bytes written
      // each 128mb - random writes are about 1.3 times slower than sequential
      // each 64mb ~ random writes are about 2.4 times slower than sequential
      // each 32mb ~ random writes are about 5.5 times slower than sequential
      // each 8mb ~ random writes take infinity
      int lgFlushSize = 25;
      
      int N = 10;
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int c = N; c --> 0;) {
         write(mem, addresses1, lgFlushSize);
         System.out.println((N-c) + " of " + N + ": " + (System.currentTimeMillis() - t0));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("Seq. writes: " + dt + ", " + (N*8*addresses1.length*1000d/dt) + " bytes/sec");
      
      Thread.sleep(3000);
      
      t0 = System.currentTimeMillis();
      for(int c = N; c --> 0;) {
         write(mem, addresses2, lgFlushSize);
         System.out.println((N-c) + " of " + N + ": " + (System.currentTimeMillis() - t0));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("Random writes: " + dt + ", " + (N*8*addresses1.length*1000d/dt) + " bytes/sec");
   }
   
   static void write(SimpleFileDataBuffer mem, long[] addresses, int logFlushSize) {
      int flushMask = (1<<(logFlushSize-3))-1;
      for(int i = 1 ; i <= addresses.length; i++) {
         mem.writeLong(addresses[i-1], i);
         if((i & flushMask) == 0) mem.flush();
      }
   }
   
   static void shuffle(long[] data) {
      for(int i = 0; i < data.length; i++) {
         int j = i + (int)Math.floor((data.length - i - 1)*Math.random());
         long tmp = data[j];
         data[j] = data[i];
         data[i] = tmp;
      }
   }
}
