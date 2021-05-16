package io.github.whippetdb.db.api;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.db.MemMap.Cursor;
import io.github.whippetdb.util.Util;

public class LongLongDbPerf {
   static Random r = new Random();
   
   public static void main(String[] args) throws Exception {
      runWdb();
      //runHm();
   }
   
   @SuppressWarnings("unused")
   static void runWdb() throws Exception {
      new File("tmp").mkdir();
      long t0, dt, total = 0;
      long N = 40_000_000;
      
      DbBuilder<Long, Long> builder = new DbBuilder<>(new LongIO(), new LongIO());
      builder.synchronize(true);
      builder.speedVsCapacity(0.4f);
      builder.create();
      //builder.create("tmp");
      System.out.println("builder="+builder);
      
      Map<Long,Long> map = builder.asMap();
      Random rand = new Random();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i), old;
         if((old = map.put(k, v)) != null) {
            System.out.println("collision: " + old + ", " + v + " -> " + k);
         }
         // num keys, allocated, actually used, time
         if((i & 0x1fffff)==0) System.out.printf("%16d  %16d  %16d  %16d\n", i, builder.db().allocatedSize(), builder.db().allocatedSize(), System.currentTimeMillis()-t0);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      // for synchronized case
      MemMap.Cursor db = (Cursor) Util.getField(Util.getField(builder.db(), "val$db"), "val$db");
      //MemMap.Cursor db = (Cursor) Util.getField(builder.db(), "val$db");
      System.out.println(db.stat());
      System.out.println();
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         Util.assertEquals(map.get(k), v);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println();
      
      System.out.println("Deleting...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(!map.remove(k).equals(v)) {
            System.out.println("incorrect or missing value: " + k + " -> " + map.get(k) + " instead of " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(map.put(k, v) != null) {
            System.out.println("collision: " + map.get(k) + ", " + k + " -> " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("mean write-read-delete-write: " + (4*N*1000f/total) + " op/sec");
   }

   static void runHm() throws Exception {
      long t0, dt, total = 0;
      long N = 50_000_000; //50_000_000;
      
      Map<Long,Long> map = new HashMap<>();
      Runtime runtime = Runtime.getRuntime();
      long usedMemory0 = runtime.totalMemory()-runtime.freeMemory();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(map.put(k, v) != null) {
            System.out.println("collision: " + map.get(k) + ", " + k + " -> " + v);
         }
         // num keys, allocated, actually used, time
         //long usedMemory = runtime.totalMemory()-runtime.freeMemory()-usedMemory0;
         //if((i&0x1fffff)==0) System.out.printf("%16d  %16d  %16d  %16d\n", i, usedMemory, usedMemory, System.currentTimeMillis()-t0);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println(((runtime.totalMemory()-runtime.freeMemory()-usedMemory0)/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(!map.get(k).equals(v)) {
            System.out.println("incorrect or missing value: " + k + " -> " + map.get(k) + " instead of " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println();
      
      System.out.println("Deleting...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(!map.remove(k).equals(v)) {
            System.out.println("incorrect or missing value: " + k + " -> " + map.get(k) + " instead of " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println(((runtime.totalMemory()-runtime.freeMemory()-usedMemory0)/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = 0; i < N; i++) {
         Long k = key(i);
         Long v = value(i);
         if(map.put(k, v) != null) {
            System.out.println("collision: " + map.get(k) + ", " + k + " -> " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println(((runtime.totalMemory()-runtime.freeMemory()-usedMemory0)/N) + " bytes/key");
      System.out.println();
      
      System.out.println("mean write-read-delete-write: " + (4*N*1000f/total) + " op/sec");
   }
   
   static MemDataBuffer keybuf = new SimpleDirectDataBuffer();
   static Random random = new Random();
   
   static Long key(long i) {
      return i;
//      return FastHash.hash64(i); // non-randomized keys
//      return FastHash.fnb64(i*FastHash.fnb64(i*i)); // moderately random keys
//      return FastHash.hash64(i^FastHash.hash64(i^FastHash.hash64(i^FastHash.hash64(i)))); // strongly random
//    String s = "" + FastHash.hash64(i);
//      keybuf.write(0, s);
//      return FastHash.hash64(keybuf, 0, s.length()<<1);
//      return random.nextLong();
   }
   
   static Long value(long i) {
      return i;
   }
}
