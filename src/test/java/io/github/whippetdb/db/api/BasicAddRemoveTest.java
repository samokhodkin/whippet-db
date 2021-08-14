package io.github.whippetdb.db.api;

import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

import io.github.whippetdb.db.api.types.CharsIO;

public class BasicAddRemoveTest {
   @Test
   public void test() throws Exception {
      Map<CharSequence,CharSequence> map = new DbBuilder<>(new CharsIO(), new CharsIO())
            .journaling(true)
            .autocommit(true)
            .synchronize(true)
            .create("tmp/BasicAddRemoveTest.db")
            .asMap();
      
      int N = 100;
      
      for(int n = 0; n < N; n++) {
         map.put("key" + n, "value" + n);
      }
      
      assertEquals(map.size(), N);
      
      for(int n = 0; n < N; n++) {
         map.remove("key" + n);
      }
      
      assertEquals(map.size(), 0);
   }
}
