package io.github.whippetdb.memory.basic;

import java.nio.ByteBuffer;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemIO;

public abstract class DynamicDataIO implements MemDataIO {
   protected static final int ZEROS_SIZE = 512;
   protected static final MemDataIO ZEROS = new SimpleDirectDataBuffer(ZEROS_SIZE){
      @Override
      public void setSize(long v) {
         throw new UnsupportedOperationException();
      }
      @Override
      protected void checkWrite(long addr, int len) {
         throw new UnsupportedOperationException();
      }
   };
   
   protected MemDataIO buffer;
   protected long bufferAddr;
   protected int bufferLen;
   
   protected abstract void checkRead(long addr, int len);
   protected abstract void checkWrite(long addr, int len);
   
   protected abstract void arrangeRead(long addr);
   protected abstract void arrangeWrite(long addr);
   
   @Override
   public void read(long addr, byte[] dst, int dstOff, int len) {
      checkRead(addr, len);
      int done = 0;
      while(done < len) {
         arrangeRead(addr + done);
         int bytes = Math.min(len-done, bufferLen);
         buffer.read(bufferAddr, dst, dstOff + done, bytes);
         done += bytes;
      }
   }
   
   @Override
   public void read(long addr, ByteBuffer dst, int dstOff, int len) {
      checkRead(addr, len);
      int done = 0;
      while(done < len) {
         arrangeRead(addr + done);
         int bytes = Math.min(len-done, bufferLen);
         buffer.read(bufferAddr, dst, dstOff + done, bytes);
         done += bytes;
      }
   }
   
   @Override
   public void read(long addr, MemIO dst, long dstAddr, int len) {
      checkRead(addr, len);
      int done = 0;
      while(done < len) {
         arrangeRead(addr + done);
         int bytes = Math.min(len-done, bufferLen);
         buffer.read(bufferAddr, dst, dstAddr + done, bytes);
         done += bytes;
      }
   }
   
   @Override
   public int readByte(long addr) {
      checkRead(addr, 1);
      arrangeRead(addr);
      return buffer.readByte(bufferAddr);
   }
   
   @Override
   public int readShort(long addr) {
      return (int)readBytes(addr, 2);
   }
   
   @Override
   public int readInt(long addr) {
      return (int)readBytes(addr, 4);
   }
   
   @Override
   public long readLong(long addr) {
      return readBytes(addr, 8);
   }
   
   @Override
   public long readBytes(long addr, int len) {
      return readBytes(addr, len, 0, len);
   }
   
   @Override
   public long readBytes(long addr, int len, int from, int to) {
      checkRead(addr, to - from);
      long v = 0;
      while(from < to) {
         arrangeRead(addr);
         int bytes = Math.min(bufferLen, to - from);
         v |= buffer.readBytes(bufferAddr, len, from, from + bytes);
         from += bytes;
         addr += bytes;
      }
      return v;
   }
   
   @Override
   public void writeByte(long addr, int b) {
      checkWrite(addr, 1);
      arrangeWrite(addr);
      buffer.writeByte(bufferAddr, b);
   }
   
   @Override
   public void write(long addr, byte[] src, int srcOff, int len) {
      checkWrite(addr, len);
      int written = 0;
      while(written < len) {
         arrangeWrite(addr + written);
         int bytes = Math.min(len-written, bufferLen);
         buffer.write(bufferAddr, src, srcOff + written, bytes);
         written += bytes;
      }
   }
   
   @Override
   public void write(long addr, ByteBuffer src, int srcOff, int len) {
      checkWrite(addr, len);
      int written = 0;
      while(written < len) {
         arrangeWrite(addr + written);
         int bytes = Math.min(len-written, bufferLen);
         buffer.write(bufferAddr, src, srcOff + written, bytes);
         written += bytes;
      }
   }
   
   @Override
   public void write(long addr, MemIO src, long srcAddr, int len) {
      checkWrite(addr, len);
      int written = 0;
      while(written < len) {
         arrangeWrite(addr + written);
         int bytes = Math.min(len-written, bufferLen);
         buffer.write(bufferAddr, src, srcAddr + written, bytes);
         written += bytes;
      }
   }
   
   @Override
   public void writeShort(long addr, int v) {
      writeBytes(addr, v, 2);
   }
   
   @Override
   public void writeInt(long addr, int v) {
      writeBytes(addr, v, 4);
   }
   
   @Override
   public void writeLong(long addr, long v) {
      writeBytes(addr, v, 8);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len) {
      writeBytes(addr, v, len, 0, len);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len, int from, int to) {
      checkWrite(addr, to - from);
      while(from < to) {
         arrangeWrite(addr);
         int bytes = Math.min(bufferLen, to-from);
         buffer.writeBytes(bufferAddr, v, len, from, from + bytes);
         from += bytes;
         addr += bytes;
      }
   }
}
