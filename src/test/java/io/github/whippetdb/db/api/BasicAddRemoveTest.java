package io.github.whippetdb.db.api;

import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

import io.github.whippetdb.db.api.types.BytesIO;
import io.github.whippetdb.db.api.types.CharsIO;

public class BasicAddRemoveTest {
   @Test
   public void testPutRemove() throws Exception {
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
   
   @Test
   public void testValueChange() {
      Map<CharSequence,byte[]> map = new DbBuilder<>(new CharsIO(), new BytesIO()).create().asMap();
      String key = "testkey";
      byte[] value1 = "LongTestString".getBytes();
      byte[] value2 = "STString".getBytes();
      byte[] v;
      
      map.put(key, value1);
      v = map.get(key);
      assertEquals(v.length, value1.length);
      assertArrayEquals(v, value1);
      
      map.put(key, value2);
      v = map.get(key);
      assertEquals(v.length, value2.length);
      assertArrayEquals(v, value2);
   }
}
