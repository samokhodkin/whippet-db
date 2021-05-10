package io.github.whippetdb.db.internal;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.internal.FixedDbUtil;
import io.github.whippetdb.db.internal.HybridDb;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.Util;

public class HybridDbPerf {
   public static void main(String[] args) {
      int keySize = 16; 
      int valueSize = 16; 
      Db parent = FixedDbUtil.create(keySize, valueSize, 10);
      HybridDb db = new HybridDb(parent, true, true);
      int N = 10_000_000;
      long t0, dt;
      t0 = System.currentTimeMillis();
      
      System.out.println("writing: ");
      for(int i = N; i --> 0;) {
         int k = i;
         db.put(new StringWrapper("" + k), (created,value) -> {
            Util.assertEquals(created, true);
            value.write(0, "" + k);
         });
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         int k = i;
         db.seek(new StringWrapper("" + k), value -> {
            Util.assertNotEquals(value, null);
            Util.assertEquals(value.readString(0).toString(), "" + k);
         });
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
   }
}
