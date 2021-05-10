package io.github.whippetdb.db.api;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.github.whippetdb.db.api.Db;
import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;
import io.github.whippetdb.db.api.types.LongIO;
import io.github.whippetdb.util.Util;

/**
 * Basic usability test for journaling mode
 */
public class BasicJournalingDbTest {
   final String path = "tmp/" + getClass().getSimpleName();
   
   @Before
   public void before() throws Exception {
      new File("tmp").mkdir();
      new File(path).delete();
      new File(path + ".wal").delete();
   }
   
   @Test
   public void test() throws IOException {
      DbBuilder<CharSequence, Long> builder = new DbBuilder<>(new CharsIO(6,7), new LongIO());
      builder.journaling(true);
      builder.create(path);
      
      int N = 4; //100 //100_000;
      Map<CharSequence, Long> map = builder.asMap();
      for(long v = N; v --> 0;) {
         map.put("" + v, v);
      }
      Util.assertEquals(map.size(), N);
      
      // reference to open db
      Db db = builder.db();

      // Don't close the existing db; open a new db over  
      // the non-closed files and check the data.
      builder.open(path);
      

      map = builder.asMap();
      for(long v = N; v --> 0;) {
         Util.assertEquals(v, map.get("" + v));
      }
      Util.assertEquals(map.size(), N);
      
      int M = N/2;
      for(long v = M; v --> 0;) {
         Util.assertEquals(v, map.remove("" + v));
      }
      Util.assertEquals(map.size(), N-M);
      
      db.close();
      builder.db().close();
   }
}
