package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.MemMap.TYPE_FREE;
import static io.github.whippetdb.memory.db.MemMap.TYPE_OFF;

import java.util.function.LongConsumer;

import io.github.whippetdb.memory.api.*;
import io.github.whippetdb.util.LongList;

/**
 * Manages cached pages. The list is implemented as a linked list,
 * where the entries are the cached pages themselves.
 */
public class CacheManager{
   public final static int ADDR_SIZE=8;
   public final static long NULL=0;
   
   public final static int NEXT_OFF = TYPE_OFF + 1;
   
   //take from cache or allocate
   public static long allocateBlock(MemDataSpace ms, long cacheAddr, int size, boolean clear){
      long cachedBlock=popAddr(ms, cacheAddr);
      if(cachedBlock==NULL) cachedBlock=ms.allocate(size,clear);
      else if(clear) ms.fillLong(cachedBlock, size, 0L);
      return cachedBlock;
   }
   
   public static void releaseBlock(MemDataSpace ms, long cacheAddr, long addr){
      long head=ms.readLong(cacheAddr);
      ms.writeByte(addr + TYPE_OFF, TYPE_FREE);
      ms.writeLong(addr + NEXT_OFF, head);
      ms.writeLong(cacheAddr, addr);
   }
   
   public static long numCachedBlocks(MemDataSpace ms, long cacheAddr) {
      long current=ms.readLong(cacheAddr);
      long num = 0;
      
      while(current != NULL) {
         num++;
         current = ms.readLong(current + NEXT_OFF);
      }
      
      return num;
   }
   
   public static void scanCache(MemDataSpace ms, long cacheAddr, LongConsumer consumer){
      long current=ms.readLong(cacheAddr);
      
      while(current!=NULL){
         consumer.accept(current);
         current=ms.readLong(current + NEXT_OFF);
      }
   }
   
   public static LongList listCache(MemDataSpace ms, long cacheAddr){
      LongList list = new LongList();
      scanCache(ms, cacheAddr, addr -> list.add(addr));
      return list;
   }
   
   private static long popAddr(MemDataSpace ms, long cacheAddr){
      long head = ms.readLong(cacheAddr);
      if(head != NULL) {
         ms.writeLong(cacheAddr, ms.readLong(head + NEXT_OFF));
      }
      return head;
   }
}