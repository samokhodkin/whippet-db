package io.github.whippetdb.memory.basic;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.util.Util;

public abstract class DirectDataIO implements MemDataIO {
   protected ByteBuffer data;
   
   protected abstract void checkRead(long addr, int len);
   protected abstract void checkWrite(long addr, int len);
   
   @Override
   public int readByte(long addr) {
      checkRead(addr, 1);
      return data.get((int)addr) & 0xff;
   }
   
   @Override
   public void read(long addr, byte[] dst, int dstOff, int len) {
      checkRead(addr, len);
      ByteBuffer src = data.duplicate();
      src.clear().position((int)addr);
      src.get(dst, dstOff, len);
   }
   
   @Override
   public void read(long addr, ByteBuffer dst, int dstOff, int len) {
      checkRead(addr, len);
      Util.copy(data, (int)addr, dst, dstOff, len);
   }
   
   @Override
   public void read(long addr, MemIO dst, long dstAddr, int len) {
      checkRead(addr, len);
      dst.write(dstAddr, data, (int)addr, len);
   }
   
   @Override
   public int readShort(long addr) {
      checkRead(addr, 2);
      return data.getShort((int)addr) & 0xffff;
   }
   
   @Override
   public int readInt(long addr) {
      checkRead(addr, 4);
      return data.getInt((int)addr);
   }
   
   @Override
   public long readLong(long addr) {
      checkRead(addr, 8);
      return data.getLong((int)addr);
   }
   
   @Override
   public void writeByte(long addr, int b) {
      checkWrite(addr, 1);
      data.put((int)addr, (byte)b);
   }
   
   @Override
   public void write(long addr, byte[] src, int srcOff, int len) {
      checkWrite(addr, len);
      ByteBuffer dst = data.duplicate();
      dst.clear().position((int)addr);
      dst.put(src, srcOff, len);
   }
   
   @Override
   public void write(long addr, MemIO src, long srcAddr, int len) {
      checkWrite(addr, len);
      src.read(srcAddr, data, (int)addr, len);
   }
   
   @Override
   public void write(long addr, ByteBuffer src, int srcOff, int len) {
      checkWrite(addr, len);
      Util.copy(src, srcOff, data, (int)addr, len);
   }
   
   @Override
   public void writeShort(long addr, int v) {
      checkWrite(addr, 2);
      data.putShort((int)addr, (short)v);
   }
   
   @Override
   public void writeInt(long addr, int v) {
      checkWrite(addr, 4);
      data.putInt((int)addr, v);
   }
   
   @Override
   public void writeLong(long addr, long v) {
      checkWrite(addr, 8);
      data.putLong((int)addr, v);
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
      fillLong(addr, len, 0x0101010101010101L*b);
   }
   
   @Override
   public void fillLong(long addr, int len, long v) {
      checkWrite(addr, len);
      int off = (int)addr;
      int frac = len & 0x7;
      int round = len - frac;
      for(int i = 0; i < round; i += 8) {
         data.putLong(off+i, v);
      }
      Bytes.writeBytes(data, off + round, v, frac);
   }
   
   @Override
   public CharSequence readString(long addr, int strLen) {
      checkRead(addr, strLen<<1);
      int off = (int)addr;
      ByteBuffer dup = data.duplicate().order(Bytes.BYTE_ORDER);
      dup.position(off).limit(off + (strLen<<1));
      char[] buf = new char[strLen];
      dup.asCharBuffer().get(buf);
      return CharBuffer.wrap(buf);
   }
   
   @Override
   public void write(long addr, CharSequence cs, int strOff, int strLen) {
      checkWrite(addr, strLen<<1);
      int off = (int)addr;
      ByteBuffer dup = data.duplicate().order(Bytes.BYTE_ORDER);
      dup.clear().position(off);
      dup.asCharBuffer().append(cs, strOff, strOff + strLen);
   }
}
