package io.github.whippetdb.db.api;

import java.io.File;
import java.util.Map;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;

public class ShortStringDbPerf {
   static final String path = "tmp/" + ShortStringDbPerf.class.getSimpleName();
   static {
      new File("tmp").mkdir();
   }
   
   public static void main(String[] args) throws Exception {
      DbBuilder<CharSequence, CharSequence> builder = new DbBuilder<>(new CharsIO(8,null), new CharsIO(8,null));
      long N = 14_000_000;
      
      Map<CharSequence,CharSequence> db;
      long t0, dt, total = 0;
      
      //builder.synchronize(true);
      System.out.println("Creating");
      //builder.speedVsCapacity(0.f);
      db = builder.create().asMap();
      System.out.println(builder);
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         String key = ""+ i; //FastHash.hash64(i);
         String value = ""+ i;
         if(db.put(key, value) != null) {
            System.out.println("key exists: " + key);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
//      VarMap.Cursor vmc = (VarMap.Cursor)Util.getField(builder.db(), "val$db"); // only for var maps
//      System.out.println("mmStat: " + vmc.mmStat());
      System.out.println();
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         String key = ""+ i; //FastHash.hash64(i);
         String value = ""+ i;
         if(!db.get(key).toString().equals(value)) {
            System.out.println("incorrect or missing value: " + key + " -> " + db.get(key) + " instead of " + value);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println();
      
      System.out.println("Deleting...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         String key = ""+ i; //FastHash.hash64(i);
         String value = ""+ i;
         if(!db.remove(key).toString().equals(value)) {
            System.out.println("incorrect or missing value: " + key + " -> " + db.get(key) + " instead of " + value);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         String key = ""+ i; //FastHash.hash64(i);
         String value = ""+ i;
         if(db.put(key, value) != null) {
            System.out.println("key exists: " + key);
         }
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("write-read-delete-write mean: " + (4*N*1000f/total) + " op/sec");
   }
}
