package io.github.whippetdb.memory.db;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.FastHash;

public class HashVariable {
   static int keyPageSize = 40;
   static int valuePageSize = 20;
   
   static String key(long i) {
      return "" + FastHash.hash64(i);
   }
   static String value(long i) {
      return "" + i;
   }
   
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      MemDataSpace ms = new SimpleDirectDataSpace();
      VarMap.Cursor db = new VarMap(ms, keyPageSize, valuePageSize, 11).new Cursor(ms);
      long N = 10_000_000;
      long t0, dt;
      
      System.out.println("Writing..");
      t0 = System.currentTimeMillis();
      MemDataBuffer buf = new SimpleDirectDataBuffer();
      for(long i = N; i --> 0;) {
         db.put(key(i, buf));
         db.value.write(0, value(i));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) +  " op/sec");
      System.out.println("stat=" + db.mmStat());
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
      
      System.out.println("Checking..");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         db.seek(key(i, buf));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) +  " op/sec");
      
      System.out.println("Reading..");
      t0 = System.currentTimeMillis();
      for(long i = N; i --> 0;) {
         db.seek(key(i, buf));
         db.value.readString(0);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) +  " op/sec");
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
   }
   
   static MemDataArray key(long i, MemDataBuffer buf)  {
      if(buf != null) {
         String s = key(i);
         buf.write(0, s);
         buf.setSize(s.length()<<1);
         return buf;
      }
      return new StringWrapper(key(i));
   }
}
