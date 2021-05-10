package io.github.whippetdb.memory.basic;

import java.nio.ByteBuffer;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemIO;

public abstract class ProxyDataIO implements MemDataIO {
   protected MemDataIO parent;
   
   protected abstract long offset();
   protected abstract void checkRead(long addr, int len);
   protected abstract void checkWrite(long addr, int len);

   @Override
   public int readByte(long addr) {
      checkRead(addr, 1);
      return parent.readByte(offset()+addr);
   }

   @Override
   public void read(long addr, byte[] dst, int dstOff, int len) {
      checkRead(addr, len);
      parent.read(offset() + addr, dst, dstOff, len);
   }
   
   @Override
   public void read(long addr, ByteBuffer dst, int dstOff, int len) {
      checkRead(addr, len);
      parent.read(offset() + addr, dst, dstOff, len);
   }

   @Override
   public void read(long addr, MemIO dst, long dstAddr, int len) {
      checkRead(addr, len);
      parent.read(offset() + addr, dst, dstAddr, len);
   }

   @Override
   public void writeByte(long addr, int b) {
      checkWrite(addr, 1);
      parent.writeByte(offset() + addr, b);
   }

   @Override
   public void write(long addr, byte[] src, int srcOff, int len) {
      checkWrite(addr, len);
      parent.write(offset() + addr, src, srcOff, len);
   }
   
   @Override
   public void write(long addr, ByteBuffer src, int srcOff, int len) {
      checkWrite(addr, len);
      parent.write(offset() + addr, src, srcOff, len);
   }

   @Override
   public void write(long addr, MemIO src, long srcAddr, int len) {
      checkWrite(addr, len);
      parent.write(offset() + addr, src, srcAddr, len);
   }
   
   @Override
   public long readBytes(long addr, int len) {
      checkRead(addr, len);
      return parent.readBytes(offset() + addr, len);
   }
   
   @Override
   public long readLong(long addr) {
      checkRead(addr, 8);
      return parent.readLong(offset() + addr);
   }
   
   @Override
   public int readInt(long addr) {
      checkRead(addr, 4);
      return parent.readInt(offset() + addr);
   }
   
   @Override
   public int readShort(long addr) {
      checkRead(addr, 2);
      return parent.readShort(offset() + addr);
   }
   
   @Override
   public CharSequence readString(long addr, int strLen) {
      checkRead(addr, strLen<<1);
      return parent.readString(offset() + addr, strLen);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len) {
      checkWrite(addr, len);
      parent.writeBytes(offset() + addr, v, len);
   }
   
   @Override
   public void writeLong(long addr, long v) {
      checkWrite(addr, 8);
      parent.writeLong(offset() + addr, v);
   }
   
   @Override
   public void writeInt(long addr, int v) {
      checkWrite(addr, 4);
      parent.writeInt(offset() + addr, v);
   }
   
   @Override
   public void writeShort(long addr, int v) {
      checkWrite(addr, 2);
      parent.writeShort(offset() + addr, v);
   }
   
   @Override
   public void write(long addr, CharSequence cs, int off, int len) {
      checkWrite(addr, len<<1);
      parent.write(offset() + addr, cs, off, len);
   }
   
   @Override
   public void writeBytes(long addr, long v, int len, int from, int to) {
      checkWrite(addr, to-from);
      parent.writeBytes(addr, v, len, from, to);
   }
}
