package io.github.whippetdb.memory.db;

import java.io.IOException;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemArray;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.db.CacheManager;
import io.github.whippetdb.memory.db.VarMap;
import io.github.whippetdb.util.Util;

public class VarMapTest {
   @Test
   public void test() throws IOException {
      MemDataSpace ms = new SimpleFileDataSpace("tmp", 0);
      //MemDataSpace ms = new SimpleHeapDataSpace();
      VarMap db = new VarMap(ms, 10, 20, 10);
      VarMap.Cursor cursor = db.new Cursor(ms);
      int N = 10;
      for(int i = 0; i < N; i++) {
         MemArray key = new SimpleHeapDataBuffer(String.valueOf(i).getBytes());
         Util.assertEquals(cursor.seek(key), false);
         Util.assertEquals(cursor.put(key), true);
         long v = (i<<16) | i;
         v = (v << 32) | v;
         cursor.value.fillLong(0, 20, v);
      }
      for(int i = 0; i < N; i++) {
         MemArray key = new SimpleHeapDataBuffer(String.valueOf(i).getBytes());
         Util.assertEquals(cursor.seek(key), true);
         Util.assertEquals(cursor.value.size(), 20L);
         Util.assertEquals(cursor.value.readShort(0), i);
         Util.assertEquals(cursor.value.readShort(2), i);
         Util.assertEquals(cursor.value.readShort(18), i);
      }
      cursor.scan(k->{cursor.delete(); return false;});
      cursor.scan(k->{Util.assertEquals(1, 0, "db must be empty"); return false;});
      Util.assertEquals(CacheManager.numCachedBlocks(ms, (Long)Util.getField(db, "keyDataCacheAddr")), 1L*N);
      Util.assertEquals(CacheManager.numCachedBlocks(ms, (Long)Util.getField(db, "valueDataCacheAddr")), 1L*N);
   }
}
