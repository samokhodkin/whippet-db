package io.github.whippetdb.db.api;

import java.io.File;
import java.util.Map;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.util.Util;

public class ShortStringDbPerf {
   static final String path = "tmp/" + ShortStringDbPerf.class.getSimpleName();
   static {
      new File("tmp").mkdir();
   }
   
   public static void main(String[] args) throws Exception {
      DbBuilder<CharSequence, CharSequence> builder = new DbBuilder<>(new CharsIO(8), new CharsIO(15));
      long N = 15_000_000;
      
      Map<CharSequence,CharSequence> map;
      long t0, dt, total = 0;
      
      //builder.synchronize(true);
      System.out.println("Creating");
      map = builder.create().asMap();
      System.out.println("db type:" + Util.getField(builder, "type"));
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         map.put("" + i, "Value=" + i);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Reading...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         Util.assertEquals(String.valueOf(map.get("" + i)), "Value="+i);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println();
      
      System.out.println("Deleting...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         Util.assertEquals(String.valueOf(map.remove("" + i)), "Value="+i);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("Writing...");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         map.put("" + i, "Value=" + i);
      }
      total += dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      System.out.println();
      
      System.out.println("write-read-delete-write mean: " + (4*N*1000f/total) + " op/sec");
   }
}
