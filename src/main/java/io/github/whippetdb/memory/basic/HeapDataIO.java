package io.github.whippetdb.memory.basic;

import java.nio.ByteBuffer;
import java.util.Arrays;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemIO;

public abstract class HeapDataIO implements MemDataIO {
   protected byte[] data;
   
   protected abstract void checkWrite(long addr, int len);
   protected abstract void checkRead(long addr, int len);
   
   @Override
   public int readByte(long addr) {
      checkRead(addr, 1);
      return data[(int)addr]&0xff;
   }
   
   @Override
   public void read(long addr, byte[] dst, int dstOff, int len) {
      checkRead(addr, len);
      System.arraycopy(data, (int)addr, dst, dstOff, len);
   }
   
   @Override
   public void read(long addr, MemIO dst, long dstAddr, int len) {
      checkRead(addr, len);
      dst.write(dstAddr, data, (int)addr, len);
   }
   
   @Override
   public void read(long addr, ByteBuffer dst, int dstOff, int len) {
      checkRead(addr, len);
      (dst = dst.duplicate()).clear().position(dstOff);
      dst.put(data, (int)addr, len);
   }
   
   @Override
   public int readShort(long addr) {
      checkRead(addr, 2);
      return Bytes.readShort(data, (int)addr);
   }
   
   @Override
   public int readInt(long addr) {
      checkRead(addr, 4);
      return Bytes.readInt(data, (int)addr);
   }
   
   @Override
   public long readLong(long addr) {
      checkRead(addr, 8);
      return Bytes.readLong(data, (int)addr);
   }
   
   @Override
   public void writeByte(long addr, int b) {
      checkWrite(addr, 1);
      data[(int)addr] = (byte)b;
   }
   
   @Override
   public void write(long addr, byte[] src, int srcOff, int len) {
      checkWrite(addr, len);
      System.arraycopy(src, srcOff, data, (int)addr, len);
   }
   
   @Override
   public void write(long addr, ByteBuffer src, int srcOff, int len) {
      checkWrite(addr, len);
      (src = src.duplicate()).clear().position(srcOff);
      src.put(data, (int)addr, len);
   }
   
   @Override
   public void write(long addr, MemIO src, long srcAddr, int len) {
      checkWrite(addr, len);
      src.read(srcAddr, data, (int)addr, len);
   }
   
   @Override
   public void writeShort(long addr, int v) {
      checkWrite(addr, 2);
      Bytes.writeShort(data, (int)addr, v);
   }
   
   @Override
   public void writeInt(long addr, int v) {
      checkWrite(addr, 4);
      Bytes.writeInt(data, (int)addr, v);
   }
   
   @Override
   public void writeLong(long addr, long v) {
      checkWrite(addr, 8);
      Bytes.writeLong(data, (int)addr, v);
   }
   
   @Override
   public long readBytes(long addr, int len) {
      checkRead(addr, len);
      return Bytes.readBytes(data, (int)addr, len);
   }
   
   @Override
   public long readBytes(long addr, int len, int from, int to) {
      checkRead(addr, to-from);
      return Bytes.readBytes(data, (int)addr, len, from, to);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len) {
      checkWrite(addr, len);
      Bytes.writeBytes(data, (int)addr, v, len);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len, int from, int to) {
      checkWrite(addr, to-from);
      Bytes.writeBytes(data, (int)addr, v, len, from, to);
   }
   
   @Override
   public void fillByte(long addr, int len, int b) {
      checkWrite(addr, len);
      int off = (int)addr;
      Arrays.fill(data, off, off+len, (byte)b);
   }
   
   @Override
   public void fillLong(long addr, int len, long v) {
      checkWrite(addr, len);
      int off = (int)addr;
      if(len < 8) {
         Bytes.writeBytes(data, off, v, len);
         return;
      }
      Bytes.writeLong(data, off, v);
      int written = 8;
      while(len-written > written) {
         System.arraycopy(data, off, data, off + written, written);
         written <<= 1;
      }
      System.arraycopy(data, off, data, off + written, len-written);
   }
}
