package io.github.whippetdb.db.api;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import io.github.whippetdb.db.api.types.CharsIO;

public class BasicScanTest {
   @Test
   public void test() {
      Map<CharSequence,CharSequence> map = new DbBuilder<>(new CharsIO(3), new CharsIO(3))
            .journaling(true)
            .autocommit(true)
            .synchronize(true)
            .create("tmp/BasicScanTest.db")
            .asMap();
      
      int N = 100;
      
      for(int n = 0; n < N; n++) {
         map.put("k" + n, "v" + n);
      }
      
      map.entrySet().forEach(e -> {
         String key = e.getKey().toString();
         String value = e.getValue().toString();
         
         int nk = Integer.parseInt(key.substring(1));
         int nv = Integer.parseInt(value.substring(1));
         
         assertEquals(nk, nv);
         assertTrue(nk >= 0 && nk < N);
         assertTrue(nv >= 0 && nv < N);
      });
   }

}
