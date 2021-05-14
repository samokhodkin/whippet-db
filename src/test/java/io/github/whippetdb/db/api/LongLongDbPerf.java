package io.github.whippetdb.db.api;

import java.io.File;
import java.util.Map;
import java.util.Random;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.db.MemMap.Cursor;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

public class LongLongDbPerf {
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      new File("tmp").mkdir();
      long t0, dt, total = 0;
      long N = 50_000_000;
      
      DbBuilder<Long, Long> builder = new DbBuilder<>(new LongIO(), new LongIO());
      builder.synchronize(true);
      builder.create();
      System.out.println("builder="+builder);
      
      Map<Long,Long> map = builder.asMap();
      
      Random r = new Random();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         Long k = key(i);
         Long v = value(i);
         if(map.put(k, v) != null) {
            System.out.println("collision: " + map.get(k) + ", " + k + " -> " + v);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      MemMap.Cursor db = (Cursor) Util.getField(Util.getField(builder.db(), "val$db"), "val$db");
      System.out.println(db.stat());
      System.out.println();
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
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
      for(long i = N; i --> 0;) {
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
      for(long i = N; i --> 0;) {
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

   static Long key(long i) {
      return FastHash.hash64(i); // less random keys
      //return FastHash.hash64(i*FastHash.hash64(i)); // highly random keys
   }
   
   static Long value(long i) {
      return i;
   }
}
