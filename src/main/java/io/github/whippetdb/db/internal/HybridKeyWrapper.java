package io.github.whippetdb.db.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.util.Util;

/**
 * Represents arbitrary-size key as a fixed-size array, with the following structure:
 * 
 * [<1-byte key length> <key data> <zeros>]
 * 
 * As a result, the following limitations apply:
 * key size + 1 <= array size
 * key size <= 255
 */

public class HybridKeyWrapper implements MemDataArray {
   private MemIO data;
   private long size;
   private long dataOff;
   private int dataSize;
   
   /**
    * @param size - total size
    * @param data - key data
    * @param dataOff - key data off
    * @param dataSize - key data size
    */
   void setData(long size, MemIO data, long dataOff, int dataSize) {
      if(dataSize < 0) throw new IllegalArgumentException("negative dataSize");
      if(dataSize > 255) throw new IllegalArgumentException("dataSize exceeds 255: " + dataSize);
      if(dataSize >= size) throw new IllegalArgumentException("dataSize exceeds avalable space: " + dataSize + " + 1 > " + size);
      this.size = size;
      this.data = data;
      this.dataOff = dataOff;
      this.dataSize = dataSize;
   }
   
   @Override
   public int readByte(long addr) {
      Util.checkRead(addr, 1, size);
      return addr == 0? dataSize: addr > dataSize? 0: data.readByte(dataOff + addr - 1);
   }
   
   @Override
   public void read(long addr, byte[] dst, int dstOff, int len) {
      Util.checkRead(addr, len, size);
      
      if(addr == 0 && len > 0) {
         dst[dstOff++] = (byte)dataSize;
         addr++;
         len--;
      }
      
      if(addr <= dataSize && len > 0) {
         int chunk = (int) Math.min(len, dataSize + 1 - addr);
         data.read(dataOff + addr - 1, dst, dstOff, chunk);
         addr += chunk;
         dstOff += chunk;
         len -= chunk;
      }
      
      Arrays.fill(dst, dstOff, dstOff + len, (byte)0);
   }
   
   @Override
   public void read(long addr, ByteBuffer dst, int dstOff, int len) {
      Util.checkRead(addr, len, size);
      
      if(addr == 0 && len > 0) {
         dst.put(dstOff++, (byte)dataSize);
         addr++;
         len--;
      }
      
      if(addr <= dataSize && len > 0) {
         int chunk = (int) Math.min(len, dataSize + 1 - addr);
         data.read(dataOff + addr - 1, dst, dstOff, chunk);
         addr += chunk;
         dstOff += chunk;
         len -= chunk;
      }
      
      Util.fillByte(dst, dstOff, len, 0);
   }
   
   @Override
   public void read(long addr, MemIO dst, long dstAddr, int len) {
      Util.checkRead(addr, len, size);
      
      if(addr == 0 && len > 0) {
         dst.writeByte(dstAddr++, dataSize);
         addr++;
         len--;
      }
      
      if(addr <= dataSize && len > 0) {
         int chunk = (int) Math.min(len, dataSize + 1 - addr);
         data.read(dataOff + addr - 1, dst, dstAddr, chunk);
         addr += chunk;
         dstAddr += chunk;
         len -= chunk;
      }
      
      dst.fillByte(dstAddr, len, 0);
   }

   @Override
   public void writeByte(long addr, int b) {
      throw new UnsupportedOperationException();
   }

   @Override
   public long size() {
      return size;
   }
   
   @Override
   public String toString() {
      return toString(0, size);
   }
}
