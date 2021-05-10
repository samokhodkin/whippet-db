package io.github.whippetdb.test.misc;

import java.io.IOException;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.util.Util;

@SuppressWarnings("unused")
public class MemMapSpeed {
   public static void main(String[] args) throws IOException {
      System.out.println("Testing speed");
      //MemDataSpace ms=new SimpleHeapDataSpace();
      MemDataSpace ms=new SimpleFileDataSpace("tmp", 0);
      
      long t0,dt;
      long N=10_000_000;
      MemMap.Cursor db=new MemMap(ms,8,8,5).new Cursor(ms);
      MemDataIO key=new SimpleDirectDataBuffer(8);
      
      t0=System.currentTimeMillis();
      for(long i=N; i-->0;) {
         key.writeLong(0, i);
         db.put(key, 0);
         db.value.writeLong(0,~i);
      }
      dt=System.currentTimeMillis()-t0;
      System.out.println(N+" puts took "+dt+" mls, "+(N*1000f/dt)+" op/sec");
      
      t0=System.currentTimeMillis();
      for(long i=N; i-->0;) {
         key.writeLong(0, i);
         db.seek(key, 0);
         db.value.readLong(0);
      }
      dt=System.currentTimeMillis()-t0;
      System.out.println(N+" gets took "+dt+" mls, "+(N*1000f/dt)+" op/sec");
      
      System.out.println("checking");
      for(long i=N; i-->0;){
         key.writeLong(0, i);
         Util.assertEquals(db.seek(key,0), true, "seek");
         Util.assertEquals(db.value.readLong(0), ~i, "value");
      }
      System.out.println("ok");
   }
}
