package io.github.whippetdb.memory.api;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class BytesLE {
   protected BytesLE(){}
   
   public final static ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
   
   //
   // byte[]
   //
   
   public static long readLong(byte[] src, int off) {
      return
            (src[off]&0xffL) |
            ((src[off+1]&0xffL) << 8) |
            ((src[off+2]&0xffL) << 16) |
            ((src[off+3]&0xffL) << 24) |
            ((src[off+4]&0xffL) << 32) |
            ((src[off+5]&0xffL) << 40) |
            ((src[off+6]&0xffL) << 48) |
            (((long)src[off+7]) << 56)
      ;
   }
   
   public static int readInt(byte[] src, int off) {
      return
         src[off]&0xff | 
         ((src[off+1]&0xff) << 8) | 
         ((src[off+2]&0xff) << 16) | 
         (src[off+3] << 24)
      ;
   }
   
   public static int readShort(byte[] src, int off) {
      return src[off]&0xff | ((src[off+1]&0xff) << 8);
   }
   
   public static void writeLong(byte[] dst, int off, long v) {
      dst[off] = (byte)v;
      dst[off+1] = (byte)(v>>8);
      dst[off+2] = (byte)(v>>16);
      dst[off+3] = (byte)(v>>24);
      dst[off+4] = (byte)(v>>32);
      dst[off+5] = (byte)(v>>40);
      dst[off+6] = (byte)(v>>48);
      dst[off+7] = (byte)(v>>56);
   }
   
   public static void writeInt(byte[] dst, int off, int v) {
      dst[off] = (byte)v;
      dst[off+1] = (byte)(v>>8);
      dst[off+2] = (byte)(v>>16);
      dst[off+3] = (byte)(v>>24);
   }
   
   public static void writeShort(byte[] dst, int off, int v) {
      dst[off] = (byte)v;
      dst[off+1] = (byte)(v>>8);
   }
   
   public static long readBytes(byte[] src, int off, int len) {
      if(len >= 8) return readLong(src, off);
      if(len >= 4) return readInt(src, off) & 0xffffffffL | readBytes(src, off+4, len-4) << 32;
      if(len >= 2) return readShort(src, off) | readBytes(src, off+2, len-2) << 16;
      if(len >= 1) return src[off] & 0xff;
      return 0L;
   }
   
   public static long readBytes(byte[] src, int off, int len, int from, int to) {
      return readBytes(src, off, to-from) << (from<<3);
   }
   
   public static void writeBytes(byte[] dst, int off, long v, int len) {
      if(len>=8) {writeLong(dst, off, v); return;}
      if(len>=4) {writeInt(dst, off, (int)v); writeBytes(dst, off+4, v>>>32, len-4); return;}
      if(len>=2) {writeShort(dst, off, (int)v); writeBytes(dst, off+2, v>>>16, len-2); return;}
      if(len>=1) dst[off] = (byte)v;
   }
   
   public static void writeBytes(byte[] dst, int off, long v, int len, int from, int to) {
      writeBytes(dst, off, v>>>(from<<3), to-from);
   }
   
   
   //
   // ByteBuffer 
   //
   
   public static long readBytes(ByteBuffer src, int off, int len) {
      if(len >= 8) return src.getLong(off);
      if(len >= 4) return src.getInt(off) & 0xffffffffL | readBytes(src, off+4, len-4) << 32;
      if(len >= 2) return src.getShort(off) & 0xffff | readBytes(src, off+2, len-2) << 16;
      if(len >= 1) return src.get(off) & 0xff;
      return 0L;
   }
   
   public static long readBytes(ByteBuffer src, int off, int len, int from, int to) {
      return readBytes(src, off, to-from) << (from<<3);
   }
   
   public static void writeBytes(ByteBuffer dst, int off, long v, int len) {
      if(len>=8) {dst.putLong(off, v); return;}
      if(len>=4) {dst.putInt(off, (int)v); writeBytes(dst, off+4, v>>>32, len-4); return;}
      if(len>=2) {dst.putShort(off, (short)v); writeBytes(dst, off+2, v>>>16, len-2); return;}
      if(len>=1) dst.put(off, (byte)v);
   }
   
   public static void writeBytes(ByteBuffer dst, int off, long v, int len, int from, int to) {
      writeBytes(dst, off, v>>>(from<<3), to-from);
   }
   
   //
   // MemIO 
   //
   
   public static long readBytes(MemIO src, long addr, int len) {
      if(len >= 8) return readLong(src, addr);
      if(len >= 4) return readInt(src, addr) & 0xffffffffL | readBytes(src, addr+4, len-4) << 32;
      if(len >= 2) return readShort(src, addr) | readBytes(src, addr+2, len-2) << 16;
      if(len >= 1) return src.readByte(addr);
      return 0L;
   }
   
   public static long readLong(MemIO src, long addr) {
      return
         src.readByte(addr) |
         src.readByte(addr+1) <<8 |
         src.readByte(addr+2) <<16 |
         src.readByte(addr+3) <<24 |
         ((long)src.readByte(addr+4)) <<32 |
         ((long)src.readByte(addr+5)) <<40 |
         ((long)src.readByte(addr+6)) <<48 |
         ((long)src.readByte(addr+7)) <<56
      ;
   }
   
   public static int readInt(MemIO src, long addr) {
      return
         src.readByte(addr) |
         src.readByte(addr+1)<<8 |
         src.readByte(addr+2)<<16 |
         src.readByte(addr+3)<<24
      ;
   }
   
   public static int readShort(MemIO src, long addr) {
      return
         src.readByte(addr) |
         src.readByte(addr+1)<<8
      ;
   }
   
   public static void writeLong(MemIO dst, long addr, long v) {
      dst.writeByte(addr, (int)v);
      dst.writeByte(addr+1, (int)(v>>8));
      dst.writeByte(addr+2, (int)(v>>16));
      dst.writeByte(addr+3, (int)(v>>24));
      dst.writeByte(addr+4, (int)(v>>32));
      dst.writeByte(addr+5, (int)(v>>40));
      dst.writeByte(addr+6, (int)(v>>48));
      dst.writeByte(addr+7, (int)(v>>56));
   }
   
   public static void writeInt(MemIO dst, long addr, int v) {
      dst.writeByte(addr, (int)v);
      dst.writeByte(addr+1, (int)(v>>8));
      dst.writeByte(addr+2, (int)(v>>16));
      dst.writeByte(addr+3, (int)(v>>24));
   }
   
   public static void writeShort(MemIO dst, long addr, int v) {
      dst.writeByte(addr, (int)v);
      dst.writeByte(addr+1, (int)(v>>8));
   }
   
   
   //
   // MemDataIO 
   //
   
   public static long readBytes(MemDataIO src, long addr, int len) {
      if(len >= 8) return src.readLong(addr);
      if(len >= 4) return src.readInt(addr) & 0xffffffffL | readBytes(src, addr+4, len-4) << 32;
      if(len >= 2) return src.readShort(addr) | readBytes(src, addr+2, len-2) << 16;
      if(len >= 1) return src.readByte(addr);
      return 0L;
   }
   
   public static void writeBytes(MemDataIO dst, long dstAddr, long v, int len) {
      if(len>=8) {dst.writeLong(dstAddr, v); return;}
      if(len>=4) {dst.writeInt(dstAddr, (int)v); writeBytes(dst, dstAddr+4, v>>>32, len-4); return;}
      if(len>=2) {dst.writeShort(dstAddr, (int)v); writeBytes(dst, dstAddr+2, v>>>16, len-2); return;}
      if(len>=1) dst.writeByte(dstAddr, (int)v);
   }
   
   public static long readBytes(MemDataIO src, long srcAddr, int len, int from, int to) {
      return src.readBytes(srcAddr, to-from) << (from<<3);
   }
   
   public static void writeBytes(MemDataIO dst, long dstAddr, long v, int len, int from, int to) {
      dst.writeBytes(dstAddr, v>>>(from<<3), to-from);
   }
   
   public static int numBytes(long v) {
      int n = 0;
      if((v&0xffffffff00000000L)!=0) {
         n += 4;
         v >>>= 32;
      }
      if((v&0xffff0000L)!=0) {
         n += 2;
         v >>>= 16;
      }
      if((v&0xff00L)!=0) {
         n += 1;
         v >>>= 8;
      }
      return v == 0? n: n + 1;
   }
}
