package io.github.whippetdb.memory.db;

import static io.github.whippetdb.memory.db.CacheManager.*;

import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.DynamicDataIO;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.util.LongList;
import io.github.whippetdb.util.Util;

/**
 * Var-length memory buffer that may created in a memspace at a certain address.
 * Data page size may be arbitrary. Uses regular division, so it is slower than AlignedVarData.
 */
public class VarData extends DynamicDataIO implements MemDataBuffer {
   private static final long NULL = 0L;
   private static final int MAX_PAGES_BUFFER = 1 << 10;
   
   static final int NEXT_ADDR_OFF = 0;
   static final int DATA_OFF = NEXT_ADDR_OFF + 8;
   
   private final MemDataSpace parent;
   private final long cacheAddr;
   private final int pageSize;
   
   private long sizeAddr = NULL;
   private long rootAddr = NULL;
   private long size = 0;
   private LongList pages;
   
   public VarData(MemDataSpace ms, long cacheAddr, int pageSize) {
      parent = ms;
      this.cacheAddr = cacheAddr;
      this.pageSize = pageSize;
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
         int lastPageId = pageId(v - 1);
         providePage(lastPageId);
         long nextPageAddr = lastPageId >= 0? pages.get(lastPageId) + NEXT_ADDR_OFF: rootAddr;
         long nextPage = parent.readLong(nextPageAddr);
         while(nextPage != NULL) {
            long tmp = parent.readLong(nextPage + NEXT_ADDR_OFF);
            releaseBlock(parent, cacheAddr, nextPage);
            nextPage = tmp;
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
      int pageId = pageId(addr);
      if(!checkPage(pageId)) {
         buffer = ZEROS;
         bufferAddr = 0;
         bufferLen = ZEROS_SIZE;
         return;
      }
      buffer = parent;
      bufferAddr = pages.get(pageId) + DATA_OFF + pageOff(addr, pageId);
      bufferLen = pageSize - pageOff(addr, pageId);
   }
   
   @Override
   protected void arrangeWrite(long addr) {
      int pageId = pageId(addr);
      providePage(pageId);
      buffer = parent;
      bufferAddr = pages.get(pageId) + DATA_OFF + pageOff(addr, pageId);
      bufferLen = pageSize - pageOff(addr, pageId);
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
             page = allocateBlock(parent, cacheAddr, pageSize + 8, true);
             parent.writeLong(nextPageAddr, page);
          }
          pages.add(page);
          nextPageAddr = page + NEXT_ADDR_OFF;
      }
   }
   
   private final int pageId(long addr) {
      return addr < 0? -1: (int) (addr/pageSize);
   }
   
   private final int pageOff(long addr, int pageId) {
      return (int) (addr - pageId*pageSize);
   }
   
   //
   // Fast create-compare-delete
   //
   
   static boolean equals(MemDataIO srcMs, long rootAddr, int pageSize, MemIO keyMs, long keyAddr, int keyLen) {
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

   static void create(MemDataSpace ms, long rootAddr, long cacheAddr, int pageSize, MemIO src, long srcAddr, int len) {
      int written = 0;
      while(written < len) {
         int chunk = Math.min(len - written, pageSize);
         long page = allocateBlock(ms, cacheAddr, pageSize + 8, true);
         ms.write(page + DATA_OFF, src, srcAddr + written, chunk);
         ms.writeLong(rootAddr, page);
         rootAddr = page + NEXT_ADDR_OFF;
         written += chunk;
      }
   }
   
   static void put(MemDataSpace ms, long rootAddr, long cacheAddr, int pageSize, MemIO src, long srcAddr, int len) {
      int written = 0;
      long page;
      while(written < len) {
         int chunk = Math.min(len - written, pageSize);
         page = ms.readLong(rootAddr);
         if(page == NULL) {
            page = allocateBlock(ms, cacheAddr, pageSize + 8, true);
            ms.writeLong(rootAddr, page);
         }
         ms.write(page + DATA_OFF, src, srcAddr + written, chunk);
         rootAddr = page + NEXT_ADDR_OFF;
         written += chunk;
      }
      page = ms.readLong(rootAddr);
      while(page != NULL) {
         long tmp = ms.readLong(page + NEXT_ADDR_OFF);
         releaseBlock(ms, cacheAddr, page);
         page = tmp;
      }
   }

   static int read(MemDataSpace ms, long rootAddr, int pageSize, MemIO dst, long dstOff, int len) {
      int read = 0;
      long page = ms.readLong(rootAddr);
      while(read < len) {
         if(page == NULL) return read;
         int chunk = Math.min(pageSize, len - read);
         ms.read(page + DATA_OFF, dst, dstOff + read, chunk);
         read += chunk;
         page = ms.readLong(page + NEXT_ADDR_OFF);
      }
      return read;
   }
   
   static void delete(MemDataSpace ms, long rootAddr, long cacheAddr) {
      long page = ms.readLong(rootAddr);
      while(page != NULL) {
         long tmp = ms.readLong(page + NEXT_ADDR_OFF);
         releaseBlock(ms, cacheAddr, page);
         page = tmp;
      }
      ms.writeLong(rootAddr, NULL);
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
   
   public static void main(String[] args) {
      MemDataSpace ms = new SimpleHeapDataSpace();
      long cacheAddr = ms.allocate(8, true);
      long rootAddr = ms.allocate(8, true);
      long sizeAddr = ms.allocate(8, true);
      
      int pageSize = 24;
      MemDataBuffer src = new SimpleDirectDataBuffer();
      src.fillLong(0, 2*pageSize + 10, 0x0801010101010100L);
      put(ms, rootAddr, cacheAddr, pageSize, src, 0, (int)src.size());
      
      MemDataBuffer dst = new SimpleDirectDataBuffer();
      read(ms, rootAddr, pageSize, dst, 0, (int) src.size()); // dst == src
      Util.assertEquals(src.size(), dst.size());
      Util.assertEquals(src.equals(0, dst, 0, (int)dst.size()), true);
      
      read(ms, rootAddr, pageSize, dst, 0, 1000); // dst size = 3*pageSize
      Util.assertEquals(dst.size(), 3L*pageSize);
      Util.assertEquals(src.equals(0, dst, 0, (int)src.size()), true);
      
      Util.assertEquals(CacheManager.numCachedBlocks(ms, cacheAddr), 0L);
      
      src.setSize(0);
      src.fillLong(0, pageSize + 10, 0x0802020202020200L);
      put(ms, rootAddr, cacheAddr, pageSize, src, 0, (int) src.size());
      Util.assertEquals(CacheManager.numCachedBlocks(ms, cacheAddr), 1L);
      
      VarData mem = new VarData(ms, cacheAddr, pageSize);
      mem.connect(sizeAddr, rootAddr);
      Util.assertEquals(mem.size(), 0L);
      mem.write(0, "abcdef");
      Util.assertEquals(mem.size(), 12L);
      Util.assertEquals(mem.readString(0).toString(), "abcdef");
   }
}
