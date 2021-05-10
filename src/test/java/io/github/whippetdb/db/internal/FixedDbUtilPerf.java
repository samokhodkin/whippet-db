package io.github.whippetdb.db.internal;

import java.io.IOException;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.internal.FixedDbUtil;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.util.Util;

public class FixedDbUtilPerf {
   public static void main(String[] args) throws IOException {
      long t0, dt;
      int N = 50_000_000;
      Db db = FixedDbUtil.create(8, 8, 10);
      MemDataBuffer buf = new SimpleDirectDataBuffer();
      
      System.out.println("Writing");
      t0 = System.currentTimeMillis();
      for(int i=N; i --> 0;) {
         int key = i;
         buf.writeLong(0, key);
         db.put(buf, (created, value)->{
            if(!created) System.out.println("collision: " + key);
            value.writeLong(0, key);
         });
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((db.allocatedSize()/N) + " bytes/key");
      
      System.out.println("Reading");
      t0 = System.currentTimeMillis();
      for(int i=N; i --> 0;) {
         int key = i;
         buf.writeLong(0, key);
         db.seek(buf, value->{
            if(value == null) System.out.println("key not found: " + key);
            Util.assertEquals((long)key, value.readLong(0));
         });
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
   }
}
