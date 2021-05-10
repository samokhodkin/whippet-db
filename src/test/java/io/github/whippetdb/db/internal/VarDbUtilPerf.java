package io.github.whippetdb.db.internal;

import java.io.IOException;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.internal.VarDbUtil;
import io.github.whippetdb.memory.api.MemSpace;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.Util;

public class VarDbUtilPerf {
   public static void main(String[] args) throws IOException {
    int N = 10_000_000;
    Db db = VarDbUtil.create("tmp", 16, 30, 10, 0, false);
    long t0, dt;
    
    System.out.println("key size: " + db.keySize());
    System.out.println("max key size: " + db.maxKeySize());
    System.out.println("value size: " + db.valueSize());
    System.out.println("max value size: " + db.maxValueSize());
    
    System.out.println("Writing:");
    t0 = System.currentTimeMillis();
    for(int i=N; i --> 0;) {
       int key = i;
       db.put(new StringWrapper(String.valueOf(key)), (created,value)->{
          Util.assertEquals(created, true);
          value.write(0, "value("+key+")");
       });
    }
    dt = System.currentTimeMillis() - t0;
    System.out.println((N*1000f/dt) + " op/sec");
    System.out.println((((MemSpace)Util.getField(db, "val$ms")).allocatedSize()/N) + " bytes/key");
    
    System.out.println("Reading:");
    t0 = System.currentTimeMillis();
    for(int i=N; i --> 0;) {
       int key = i;
       db.seek(new StringWrapper(String.valueOf(key)), value -> {
          Util.assertNotEquals(value, null);
          Util.assertEquals(value.readString(0).toString(), "value("+key+")");
       });
    }
    dt = System.currentTimeMillis() - t0;
    System.out.println((N*1000f/dt) + " op/sec");
 }
}
