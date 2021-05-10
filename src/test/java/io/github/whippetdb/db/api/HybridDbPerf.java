package io.github.whippetdb.db.api;

import java.io.File;
import java.util.Map;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

public class HybridDbPerf {
   static final String path = "tmp/" + HybridDbPerf.class.getSimpleName();
   static {
      new File("tmp").mkdir();
   }
   
   public static void main(String[] args) throws Exception {
      DbBuilder<CharSequence,CharSequence> builder = new DbBuilder<>(new CharsIO(8), new CharsIO(15));
      
      Map<CharSequence,CharSequence> map;
      long t0, dt;
      int N = 14_000_000;
      
      System.out.println("Creating");
      map = builder.create(path).asMap();
      Util.assertEquals(builder.db().maxKeySize(), 16);
      Util.assertEquals(builder.db().maxValueSize(), 30);
      Util.assertEquals(Util.getField(builder, "type"), Db.Type.Fixed);
      
      System.out.println("Writing, wait..");
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         map.put("" + i, "Value=" + i);
      }
      dt = System.currentTimeMillis() - t0;
      Util.assertEquals(map.size(), N);
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      
      builder.db().close();
      System.out.println("Closed");
      
      System.out.println("Reopening");
      map = builder.open(path).asMap();
      
      System.out.println("Reading, wait..");
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         Util.assertEquals(String.valueOf(map.get("" + i)), "Value="+i);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      Util.assertEquals(map.size(), N);
      
      builder.db().close();
      FileUtil.deleteLater(path);
    }
}
