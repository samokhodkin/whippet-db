package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.*;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.DynamicDataIO;
import io.github.whippetdb.util.LongList;
import io.github.whippetdb.util.Util;

/**
 * Same as VarData but with pages aligned at power of 2.
 * Is a bit faster, but may be very wasteful memory-wise, because actual data page size 
 * is 2^n+8, which means that the last 8 bytes will take the whole page in an aligned memspace. 
 * So it should be used only with non-aligned memspaces, which is quite limiting.
 */
public class AlignedVarData extends DynamicDataIO implements MemDataBuffer {
   private static long NULL = 0L;
   private static int MAX_PAGES_BUFFER = 1 << 10;
   
   private static int NEXT_ADDR_OFF = 0;
   private static int DATA_OFF = NEXT_ADDR_OFF + 8;
   
   private final MemDataSpace parent;
   private final long cacheAddr;
   private final int lgPageSize, pageSize, pageMask;
   private final int actualPageSize;
   
   private long sizeAddr = NULL;
   private long rootAddr = NULL;
   private long size = 0;
   private LongList pages;
   
   public AlignedVarData(MemDataSpace ms, long cacheAddr, int lgPageSize) {
      parent = ms;
      this.cacheAddr = cacheAddr;
      this.lgPageSize = lgPageSize;
      pageSize = 1 << lgPageSize;
      pageMask = pageSize - 1;
      actualPageSize = pageSize + DATA_OFF;
   }
   
   public void connect(long sizeAddr, long rootAddr) {
      if(pages == null) pages = new LongList();
      else pages.clip(0);
      
      this.sizeAddr = sizeAddr;
      this.rootAddr = rootAddr;
      size = parent.readLong(sizeAddr);
   }
   
   public void disconnect() {
      if(pages != null && pages.capacity() > MAX_PAGES_BUFFER) pages = null;
      this.sizeAddr = NULL;
      this.rootAddr = NULL;
      size = 0;
   }

   public long size() {
      return size;
   }
   
   @Override
   public void setSize(long v) {
      if(v == size) return;
      if(v < size) {
         int lastPageId = (int)((v - 1) >> lgPageSize);
         providePage(lastPageId);
         long nextPageAddr = lastPageId >= 0? pages.get(lastPageId) + NEXT_ADDR_OFF: rootAddr;
         long nextPage = parent.readLong(nextPageAddr);
         while(nextPage != NULL) {
            releaseBlock(parent, cacheAddr, nextPage);
            nextPage = parent.readLong(nextPage + NEXT_ADDR_OFF);
         }
         parent.writeLong(nextPageAddr, NULL);
         pages.clip(lastPageId + 1);
      }
      parent.writeLong(sizeAddr, size = v);
   }
   
   @Override
   protected void checkRead(long addr, int len) {
      Util.checkRead(addr, len, size);
   }
   
   @Override
   protected void checkWrite(long addr, int len) {
      Util.checkAppend(addr, size);
      if(addr + len > size) {
         parent.writeLong(sizeAddr, size = addr + len);
      }
   }
   
   @Override
   protected void arrangeRead(long addr) {
      int pageId = (int)(addr >> lgPageSize);
      if(!checkPage(pageId)) {
         buffer = ZEROS;
         bufferAddr = 0;
         bufferLen = ZEROS_SIZE;
         return;
      }
      buffer = parent;
      bufferAddr = pages.get(pageId) + DATA_OFF + (addr & pageMask);
      bufferLen = pageSize - (int)(addr & pageMask);
   }
   
   @Override
   protected void arrangeWrite(long addr) {
      int pageId = (int)(addr >> lgPageSize);
      providePage(pageId);
      buffer = parent;
      bufferAddr = pages.get(pageId) + DATA_OFF + (addr & pageMask);
      bufferLen = pageSize - (int)(addr & pageMask);
   }
   
   private boolean checkPage(int id) {
      int n = pages.size();
      if(n > id) return true;
      long nextPageAddr = n > 0? pages.peek() + NEXT_ADDR_OFF: rootAddr;
      while(pages.size() <= id) {
          long page = parent.readLong(nextPageAddr);
          if(page == NULL) return false;
          pages.add(page);
          nextPageAddr = page + NEXT_ADDR_OFF;
      }
      return true;
   }
   
   private void providePage(int id) {
      int n = pages.size();
      if(id < n) return;
      long nextPageAddr = n > 0? pages.peek() + NEXT_ADDR_OFF: rootAddr;
      while(pages.size() <= id) {
          long page = parent.readLong(nextPageAddr);
          if(page == NULL) {
             page = allocateBlock(parent, cacheAddr, actualPageSize, true);
             parent.writeLong(nextPageAddr, page);
          }
          pages.add(page);
          nextPageAddr = page + NEXT_ADDR_OFF;
      }
   }
   
   //
   // Fast create-compare-delete
   //
   
   static boolean equals(MemDataIO srcMs, long rootAddr, int lgPageSize, MemIO keyMs, long keyAddr, int keyLen) {
      final int pageSize = 1 << lgPageSize;
      int checked = 0;
      long page = srcMs.readLong(rootAddr);
      while(checked < keyLen) {
         if(page == NULL) return false;
         int chunk = Math.min(pageSize, keyLen - checked);
         if(!srcMs.equals(page + DATA_OFF, keyMs, keyAddr + checked, chunk)) return false;
         checked += chunk;
         page = srcMs.readLong(page + NEXT_ADDR_OFF);
      }
      return true;
   }

   static void create(MemDataSpace ms, long rootAddr, long cacheAddr, int lgPageSize, MemIO src, long srcAddr, int len) {
      final int pageSize = 1 << lgPageSize;
      int written = 0;
      while(written < len) {
         int chunk = Math.min(len - written, pageSize);
         long page = allocateBlock(ms, cacheAddr, pageSize + DATA_OFF, true);
         ms.write(page + DATA_OFF, src, srcAddr + written, chunk);
         ms.writeLong(rootAddr, page);
         rootAddr = page + NEXT_ADDR_OFF;
         written += chunk;
      }
   }
   
   static void delete(MemDataSpace ms, long rootAddr, long cacheAddr) {
      long page = ms.readLong(rootAddr);
      while(page != NULL) {
         releaseBlock(ms, cacheAddr, page);
         page = ms.readLong(page + NEXT_ADDR_OFF);
      }
      ms.writeLong(rootAddr, NULL);
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
}
