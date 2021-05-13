package io.github.whippetdb.memory.db;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.FastHash;

public class HashVariable {
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      MemDataSpace ms = new SimpleDirectDataSpace();
      VarMap.Cursor db = new VarMap(ms, 40, 20, 11).new Cursor(ms);
      long N = 10_000_000;
      long t0, dt;
      
      System.out.println("Writing..");
      t0 = System.currentTimeMillis();
      MemDataBuffer buf = new SimpleDirectDataBuffer();
      for(long i = N; i --> 0;) {
         db.put(new StringWrapper("" + FastHash.hash64(i)));
         //db.put(new StringWrapper("" + i));
         db.value.write(0, "" + i);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) +  " op/sec");
      System.out.println("stat=" + db.mmStat());
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
   }
}
