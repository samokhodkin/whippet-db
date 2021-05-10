package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.*;

import java.util.Arrays;

import org.junit.Test;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.util.Util;

public class CacheManagerTest {
   @Test
   public void test() {
      MemDataSpace ms=new SimpleHeapDataSpace();
      long cacheAddr=ms.allocate(8,true);
      long[] pages = new long[1000];
      for(int i = 0; i < pages.length; i++) {
         pages[i] = ms.allocate(16, true);
      }
      
      // push 1000 addresses, last first
      for(int i=pages.length; i-->0;){
         releaseBlock(ms, cacheAddr, pages[i]);
      }
      Util.assertEquals(numCachedBlocks(ms, cacheAddr), (long)pages.length);
      
      // pop all; they come in rev order
      for(int i=0; i < pages.length; i++){
         Util.assertEquals(allocateBlock(ms, cacheAddr, 16, true), pages[i]);
      }
      Util.assertEquals(numCachedBlocks(ms, cacheAddr), 0L);
      
      // push all
      for(int i=pages.length; i-->0;){
         releaseBlock(ms, cacheAddr, pages[i]);
      }
      
      // pop first 700, remains 300
      for(int i=0; i < 700; i++){
         Util.assertEquals(allocateBlock(ms, cacheAddr, 16, true), pages[i]);
      }
      Util.assertEquals(numCachedBlocks(ms, cacheAddr), pages.length - 700L);
      
      
      // push first 700, cached 1000
      for(int i = 700; i --> 0;){
         releaseBlock(ms, cacheAddr, pages[i]);
      }
      Util.assertEquals(numCachedBlocks(ms, cacheAddr), (long)pages.length);
      Util.assertEquals(Arrays.toString(pages).replaceAll("\\s+", ""), listCache(ms, cacheAddr).toString());
   }
}
