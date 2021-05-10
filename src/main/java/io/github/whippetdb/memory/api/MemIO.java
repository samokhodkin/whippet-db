package io.github.whippetdb.memory.api;

import java.nio.ByteBuffer;

public interface MemIO {
   public int readByte(long addr); // read *unsigned* byte;
   public void writeByte(long addr, int b);
   
   default void read(long addr, byte[] dst, int dstOff, int len) {
      for(int i = len; i --> 0;) dst[dstOff + i] = (byte)readByte(addr + i); 
   }
   default void read(long addr, ByteBuffer dst, int dstOff, int len) {
      for(int i = len; i --> 0;) dst.put(dstOff + i, (byte)readByte(addr + i)); 
   }
   default void read(long addr, MemIO dst, long dstAddr, int len) {
      for(int i = len; i --> 0;) dst.writeByte(dstAddr + i, readByte(addr + i));
   }
   default CharSequence readString(long addr, int strLen) {
      byte[] buf = new byte[strLen<<1];
      read(addr, buf, 0, strLen<<1);
      return ByteBuffer.wrap(buf).order(Bytes.BYTE_ORDER).asCharBuffer();
   }
   
   default void write(long addr, byte[] src, int srcOff, int len) {
      for(int i = len; i --> 0;) writeByte(addr + i, src[srcOff + i]);
   }
   default void write(long addr, ByteBuffer src, int srcOff, int len) {
      for(int i = len; i --> 0;) writeByte(addr + i, src.get(srcOff + i));
   }
   default void write(long addr, MemIO src, long srcAddr, int len) {
      for(int i = len; i --> 0;) writeByte(addr + i, src.readByte(srcAddr + i));
   }
   default void write(long addr, CharSequence src, int srcOff, int srcLen) {
      byte[] data = new byte[srcLen<<1];
      for(int i = 0; i < srcLen; i++) {
         Bytes.writeShort(data, i<<1, src.charAt(i));
      }
      write(addr, data, 0, srcLen<<1);
   }
   default void write(long addr, CharSequence cs) {
      write(addr, cs, 0, cs.length());
   }
   
   default void fillByte(long addr, int len, int b) {
      int written = 0;
      while(written < 4 && written < len)  writeByte(addr+written++, b);
      while(written < len-written) {
         write(addr+written, this, addr, written);
         written <<= 1;
      }
      write(addr+written, this, addr, len-written);
   }
   
   default int compareTo(long addr, MemIO other, long otherAddr, int len) {
      int diff;
      for(int i = 0; i < len; i++) {
         if((diff = readByte(addr+i) - other.readByte(otherAddr+i)) == 0) continue;
         return diff > 0? 1: -1;
      }
      return 0;
   }
   
   default boolean equals(long addr, MemIO other, long otherAddr, int len) {
      for(int i = 0; i < len; i++) {
         if(readByte(addr+i) != other.readByte(otherAddr+i)) return false;
      }
      return true;
   }
   
   default String toString(long addr, long len) {
      StringBuilder sb = new StringBuilder("[");
      if(len > 0) sb.append(readByte(addr));
      for(int i = 1; i < len; i++) sb.append(",").append(readByte(addr+i));
      return sb.append("]").toString();
   }
}
