package io.github.whippetdb.db.api;

import static io.github.whippetdb.util.FastHash.hash64;

import java.io.File;
import java.util.Map;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.LongIO;

public class LongLongDbPerf {
   public static void main(String[] args) throws Exception {
      new File("tmp").mkdir();
      long t0, dt, total = 0;
      long N = 50_000_000;
      
      DbBuilder<Long, Long> builder = new DbBuilder<>(new LongIO(), new LongIO());
      builder.synchronize(true);
      builder.create();
      
      Map<Long,Long> map = builder.asMap();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         long k = hash64(i);
         if(map.put(k, i) != null) {
            System.out.println("collision: " + map.get(k) + ", " + i + " -> " + k);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.exit(0);
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         long k = hash64(i);
         if(map.get(k) != i) {
            System.out.println("incorrect or missing value: " + k + " -> " + map.get(k) + " instead of " + i);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println();
      
      System.out.println("Deleting...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         long k = hash64(i);
         if(map.remove(k) != i) {
            System.out.println("incorrect or missing value: " + k + " -> " + map.get(k) + " instead of " + i);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         long k = hash64(i);
         if(map.put(k, i) != null) {
            System.out.println("collision: " + map.get(k) + ", " + i + " -> " + k);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("write-read-delete-write combination: " + (N*1000f/total) + " op/sec");
   }
}
