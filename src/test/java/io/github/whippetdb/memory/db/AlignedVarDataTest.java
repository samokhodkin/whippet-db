package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.numCachedBlocks;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.memory.db.AlignedVarData;
import io.github.whippetdb.util.Util;

public class AlignedVarDataTest {
   public void test() {
      for(int lgPageSize: new int[]{2,4,8}) {
         MemDataSpace ms = new SimpleHeapDataSpace();
         long cacheAddr = ms.allocate(8, true);
         long rootAddr1 = ms.allocate(8, true);
         long sizeAddr1 = ms.allocate(8, true);
         long rootAddr2 = ms.allocate(8, true);
         long sizeAddr2 = ms.allocate(8, true);
         
         int pageSize = 1<<lgPageSize;
         int fillPages = 1<<10;
         int fillSize = pageSize*fillPages; 
         AlignedVarData data = new AlignedVarData(ms, cacheAddr, lgPageSize);
         
         data.connect(sizeAddr1, rootAddr1);
         data.fillLong(0, fillSize, 0x0807060504030201L);
         for(long a = 0; a < (fillSize&~7); a += 8) Util.assertEquals(data.readLong(a), 0x0807060504030201L);
         Util.assertEquals(data.size(), (long)fillSize);
         Util.assertNotEquals(ms.readLong(rootAddr1), 0L);
         
         data.connect(sizeAddr2, rootAddr2);
         data.fillLong(0, fillSize, 0x0807060504030201L);
         for(long a = 0; a < (fillSize&~7); a += 8) Util.assertEquals(data.readLong(a), 0x0807060504030201L);
         Util.assertEquals(data.size(), (long)fillSize);
         Util.assertNotEquals(ms.readLong(rootAddr2), 0L);
         
         data.connect(sizeAddr1, rootAddr1);
         Util.assertEquals(AlignedVarData.equals(ms, rootAddr2, lgPageSize, data, 0, fillSize), true);
         data.connect(sizeAddr2, rootAddr2);
         Util.assertEquals(AlignedVarData.equals(ms, rootAddr1, lgPageSize, data, 0, fillSize), true);
         
         data.connect(sizeAddr1, rootAddr1);
         data.setSize(0);
         Util.assertEquals(ms.readLong(sizeAddr1), 0L);
         Util.assertEquals(ms.readLong(rootAddr1), 0L);
         
         data.connect(sizeAddr2, rootAddr2);
         data.setSize(0);
         Util.assertEquals(ms.readLong(sizeAddr2), 0L);
         Util.assertEquals(ms.readLong(rootAddr2), 0L);
         
         Util.assertEquals(numCachedBlocks(ms, cacheAddr), 2L*fillPages);
      }
   }
}
