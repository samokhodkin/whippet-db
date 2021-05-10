package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.ExternalCacheManager.*;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.memory.db.ExternalCacheManager;
import io.github.whippetdb.util.Util;

public class ExternalCacheManagerTest {
   @Test
   public void test() {
      MemDataSpace ms=new SimpleHeapDataSpace();
      long cacheAddr=ms.allocate(8,true);
      
      // push 1000 addresses
      for(int i=1000; i-->0;){
         releaseBlock(ms, cacheAddr, 111);
      }
      
      // pop all 
      int n=0; 
      //while(popAddr(ms, cacheAddr)!=NULL) n++;
      while(!Util.call(ExternalCacheManager.class, "popAddr", ms, cacheAddr).equals(NULL)) n++;
      Util.assertEquals(n, 1000);
      
      // push 1200
      for(int i=1200; i-->0;) releaseBlock(ms, cacheAddr, 111);
      
      // pop 700, remains 500
      //for(int i=700; i-->0;) popAddr(ms, cacheAddr);
      for(int i=700; i-->0;) Util.call(ExternalCacheManager.class, "popAddr", ms, cacheAddr);
      
      // push 800, remains 1300
      for(int i=800; i-->0;) releaseBlock(ms, cacheAddr, 111);
      
      int[] stat=new int[1];
      scanCache(ms, cacheAddr, addr -> {
         Util.assertEquals(addr, 111L);
         stat[0]++;
      });
      Util.assertEquals(stat[0], 1300);
      
      n=0; 
      //while(popAddr(ms, cacheAddr)!=NULL) n++;
      while(!Util.call(ExternalCacheManager.class, "popAddr", ms, cacheAddr).equals(NULL)) n++;
      Util.assertEquals(n, 1300);
      
      printCache(ms, cacheAddr);
   }
}
