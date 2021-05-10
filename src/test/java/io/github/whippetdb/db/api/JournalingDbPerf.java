package io.github.whippetdb.db.api;

import java.io.File;
import java.util.Map;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.util.FileUtil;
import io.github.whippetdb.util.Util;

public class JournalingDbPerf {
   public static void main(String[] args) throws Exception {
      String path = "tmp/" + JournalingDbPerf.class.getSimpleName();
      DbBuilder<CharSequence,CharSequence> builder = new DbBuilder<>(new CharsIO(7), new CharsIO(15));
      builder.journaling(true);
      builder.journalSizeVsSpeed(0.55f);
      builder.journalDurabilityVsSpeed(1);
      
      Map<CharSequence,CharSequence> map;
      long t0, dt;
      int N = 5_000_000;
      
      System.out.println("Creating");
      new File("tmp").mkdir();
      map = builder.create(path).asMap();
      System.out.println("builder -> " + builder);
      
      System.out.println("Writing, wait..");
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         Util.assertEquals(map.put("" + i, "Value("+i+")"), null);
      }
      dt = System.currentTimeMillis() - t0;
      Util.assertEquals(map.size(), N);
      System.out.println((N*1000f/dt) + " op/sec");
      System.out.println((builder.db().allocatedSize()/N) + " bytes/key");
      
      builder.db().close();
      System.out.println("Closed");
      
      System.gc();
      Thread.sleep(100);
      
      System.out.println("Reopening");
      map = builder.open(path).asMap();
      Util.assertEquals(map.size(), N);
      
      System.out.println("Reading, wait..");
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         Util.assertEquals(String.valueOf(map.get("" + i)), "Value("+i+")");
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/dt) + " op/sec");
      
      builder.db().close();
      FileUtil.deleteLater("tmp/JournalingDbPerf");
    }
}
