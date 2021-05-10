package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.*;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.memory.db.VarData;
import io.github.whippetdb.util.Util;

public class VarDataTest {
   @Test
   public void test() {
      for(int pageSize: new int[]{10,50,240}) {
         MemDataSpace ms = new SimpleHeapDataSpace();
         long cacheAddr = ms.allocate(8, true);
         long rootAddr1 = ms.allocate(8, true);
         long sizeAddr1 = ms.allocate(8, true);
         long rootAddr2 = ms.allocate(8, true);
         long sizeAddr2 = ms.allocate(8, true);
         
         int fillPages = 1<<3; //1<<10;
         int fillSize = pageSize*fillPages+3;
         VarData data = new VarData(ms, cacheAddr, pageSize);
         
         data.connect(sizeAddr1, rootAddr1);
         data.fillLong(0, fillSize, 0x0807060504030201L);
         
         for(long a = 0; a < (fillSize&~7); a += 8) Util.assertEquals(data.readLong(a), 0x0807060504030201L);
         Util.assertEquals(data.readBytes(fillSize&~7, fillSize&7), 0x0807060504030201L&~(-1<<((fillSize&7)<<3)));
         Util.assertEquals(data.size(), (long)fillSize);
         Util.assertNotEquals(ms.readLong(rootAddr1), 0L);
         
         data.connect(sizeAddr2, rootAddr2);
         data.fillLong(0, fillSize, 0x0807060504030201L);
         
         for(long a = 0; a < (fillSize&~7); a += 8) Util.assertEquals(data.readLong(a), 0x0807060504030201L);
         Util.assertEquals(data.readBytes(fillSize&~7, fillSize&7), 0x0807060504030201L&~(-1<<((fillSize&7)<<3)));
         Util.assertEquals(data.size(), (long)fillSize);
         Util.assertNotEquals(ms.readLong(rootAddr2), 0L);
         
         data.connect(sizeAddr1, rootAddr1);
         Util.assertEquals(VarData.equals(ms, rootAddr2, pageSize, data, 0, fillSize), true);
         data.connect(sizeAddr2, rootAddr2);
         Util.assertEquals(VarData.equals(ms, rootAddr1, pageSize, data, 0, fillSize), true);
         
         data.connect(sizeAddr1, rootAddr1);
         data.setSize(0);
         Util.assertEquals(ms.readLong(sizeAddr1), 0L);
         Util.assertEquals(ms.readLong(rootAddr1), 0L);
         
         data.connect(sizeAddr2, rootAddr2);
         data.setSize(0);
         Util.assertEquals(ms.readLong(sizeAddr2), 0L);
         Util.assertEquals(ms.readLong(rootAddr2), 0L);
         
         Util.assertEquals(numCachedBlocks(ms, cacheAddr), 2L*(fillPages+1)); // fillPages<+1> because of extra 3 bytes, see fillSize
      }
   }
}
