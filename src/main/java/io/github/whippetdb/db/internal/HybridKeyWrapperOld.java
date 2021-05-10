package io.github.whippetdb.db.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.util.Util;

public class HybridKeyWrapperOld implements MemDataArray {
   private MemIO data;
   private long size;
   private long dataOff;
   private int dataSize;
   
   void setData(long size, MemIO data, long dataOff, int dataSize) {
      if(dataSize >= 256) throw new IllegalArgumentException("dataSize exceeds 255: " + dataSize);
      Util.checkWrite(1, dataSize, size);
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
      if(addr == 0) {
         dst[dstOff++] = (byte)dataSize;
         addr++;
         len--;
      }
      if(addr <= dataSize) {
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
      if(addr == 0) {
         dst.put(dstOff++, (byte)dataSize);
         addr++;
         len--;
      }
      if(addr <= dataSize) {
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
      if(addr == 0) {
         dst.writeByte(dstAddr++, dataSize);
         addr++;
         len--;
      }
      if(addr <= dataSize) {
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
