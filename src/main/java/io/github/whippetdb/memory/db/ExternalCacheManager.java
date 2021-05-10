package io.github.whippetdb.memory.db;

import java.util.function.LongConsumer;

import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemSpace;

/**
 * Same functionality as CacheManager, but implemented as a reference list in additionally
 * allocated space. It's use may be beneficial for performance in some cases, because of less scattered writes.
 * The cached pages may belong to a different memspace
 */
public class ExternalCacheManager {
   public final static int ADDR_SIZE=8;
   public final static long NULL=0;
   
   //[ nextAddr(8) prevAddr(8) numEntries(4)  ]
   public static final int MAX_NUM_ENTRIES=128;
   public static final int NEXT_ADDR_OFF=0;
   public static final int PREV_ADDR_OFF=NEXT_ADDR_OFF+8;
   public static final int NUM_ENTRIES_OFF=PREV_ADDR_OFF+8;
   public static final int DATA_OFF=NUM_ENTRIES_OFF+4;
   public static final int BLOCK_SIZE=DATA_OFF+MAX_NUM_ENTRIES*8;
   
   //take from cache or allocate
   public static long allocateBlock(MemDataSpace ms, long cacheAddr, int size, boolean clear){
      return allocateBlock(ms,ms,cacheAddr,size,clear);
   }
   
   public static long allocateBlock(MemDataSpace dbMs, MemSpace ms, long cacheAddr, int size, boolean clear){
      long cachedBlock=popAddr(dbMs, cacheAddr);
      if(cachedBlock==NULL) cachedBlock=ms.allocate(size,clear);
      else if(clear) ms.fillByte(cachedBlock, size, 0);
      return cachedBlock;
   }
   
   public static void releaseBlock(MemDataSpace dbMs, long cacheAddr, long addr){
      long currentBlock=dbMs.readLong(cacheAddr);
      
      if(currentBlock==NULL){
         currentBlock=dbMs.allocate(BLOCK_SIZE, true);
         dbMs.writeInt(currentBlock+NUM_ENTRIES_OFF, 1);
         dbMs.writeLong(currentBlock+DATA_OFF, addr);
         dbMs.writeLong(cacheAddr, currentBlock);
         return;
      }
      
      int numEntries=dbMs.readInt(currentBlock+NUM_ENTRIES_OFF);
      
      if(numEntries<MAX_NUM_ENTRIES){
         dbMs.writeInt(currentBlock+NUM_ENTRIES_OFF, numEntries+1);
         dbMs.writeLong(currentBlock+DATA_OFF+(numEntries<<3), addr);
         return;
      }
      
      long prevBlock=dbMs.readLong(currentBlock+PREV_ADDR_OFF);
      if(prevBlock==NULL){
         prevBlock=dbMs.allocate(BLOCK_SIZE, true);
         dbMs.writeLong(prevBlock+NEXT_ADDR_OFF, currentBlock);
         dbMs.writeLong(currentBlock+PREV_ADDR_OFF, prevBlock);
      }
      dbMs.writeInt(prevBlock+NUM_ENTRIES_OFF, 1);
      dbMs.writeLong(prevBlock+DATA_OFF, addr);
      dbMs.writeLong(cacheAddr, prevBlock);
   }
   
   public static long numCachedBlocks(MemDataSpace dbMs, long cacheAddr) {
      long current=dbMs.readLong(cacheAddr);
      long num = 0;
      
      while(current != NULL) {
         num += dbMs.readInt(current + NUM_ENTRIES_OFF);
         current = dbMs.readLong(current + NEXT_ADDR_OFF);
      }
      
      return num;
   }
   
   public static void scanCache(MemDataSpace dbMs, long cacheAddr, LongConsumer consumer){
      long current=dbMs.readLong(cacheAddr);
      
      while(current!=NULL){
         int n=dbMs.readInt(current+NUM_ENTRIES_OFF);
         for(int i=0; i<n; i++){
            consumer.accept(dbMs.readLong(current+DATA_OFF+i*ADDR_SIZE));
         }
         current=dbMs.readLong(current+NEXT_ADDR_OFF);
      }
   }
   
   public static void printCache(MemDataSpace dbMs, long cacheAddr){
      long anchor=dbMs.readLong(cacheAddr);
      if(anchor==NULL) return;
      
      long current=NULL, tmp=anchor;
      while(tmp!=NULL){
         current=tmp;
         tmp=dbMs.readLong(tmp+PREV_ADDR_OFF);
      }
      
      while(current!=NULL){
         int n=dbMs.readInt(current+NUM_ENTRIES_OFF);
         long next=dbMs.readLong(current+NEXT_ADDR_OFF);
         long prev=dbMs.readLong(current+PREV_ADDR_OFF);
         System.out.println((current==anchor? "* ": "  ")+current+": "+n+" entries, next="+next+", prev="+prev);
         current=next;
      }
   }
   
   private static long popAddr(MemDataSpace dbMs, long cacheAddr){
      long currentBlock=dbMs.readLong(cacheAddr);
      while(currentBlock!=NULL){
         int numEntries=dbMs.readInt(currentBlock+NUM_ENTRIES_OFF);
         if(numEntries-->0){
            dbMs.writeInt(currentBlock+NUM_ENTRIES_OFF, numEntries);
            return dbMs.readLong(currentBlock+DATA_OFF+(numEntries<<3));
         }
         currentBlock=dbMs.readLong(currentBlock+NEXT_ADDR_OFF);
         if(currentBlock!=NULL) dbMs.writeLong(cacheAddr, currentBlock);
      }
      return NULL;
   }
}
