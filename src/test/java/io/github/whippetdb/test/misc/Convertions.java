package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Convertions {
   static final ByteBuffer byteBuf = ByteBuffer.allocateDirect(8);
   
   static void writeLong(long v) {
      byteBuf.putLong(0, v);
   }
   
   static long readLong() {
      return byteBuf.getLong(0);
   }
   
   static void writeLong1(long v, byte[] buf, int off) {
      buf[off]=(byte)v;
      buf[off+1]=(byte)(v>>8);
      buf[off+2]=(byte)(v>>16);
      buf[off+3]=(byte)(v>>24);
      buf[off+4]=(byte)(v>>32);
      buf[off+5]=(byte)(v>>40);
      buf[off+6]=(byte)(v>>48);
      buf[off+7]=(byte)(v>>56);
   }
   
   static void writeLong2(long v, byte[] buf, int off) {
      byteBuf.putLong(0, v);
      byteBuf.position(0);
      byteBuf.get(buf, 0, 8);
   }
   
   static long readLong1(byte[] buf, int off) {
      return
            (buf[off]&0xffL) |
            ((buf[off+1]&0xffL)<<8) |
            ((buf[off+2]&0xffL)<<16) |
            ((buf[off+3]&0xffL)<<24) |
            ((buf[off+4]&0xffL)<<32) |
            ((buf[off+5]&0xffL)<<40) |
            ((buf[off+6]&0xffL)<<48) |
            ((buf[off+7]&0xffL)<<56)
      ;
   }
   
   static long readLong2(byte[] buf, int off) {
      byteBuf.position(0);
      byteBuf.put(buf, 0, 8);
      return byteBuf.getLong(0);
   }
   
   public static void main(String[] args) {
      int N=100_000_000;
      byte[] buf = new byte[8];
      long t0, t1;
      
      writeLong1(27, buf, 0);
      System.out.println(readLong1(buf, 0));
      writeLong2(27, buf, 0);
      System.out.println(readLong2(buf, 0));

      for(int i=N/3; i-->0; )  writeLong(readLong());
      for(int i=N/3; i-->0; )  writeLong1(readLong1(buf, 0), buf, 0);
      for(int i=N/3; i-->0; )  writeLong2(readLong2(buf, 0), buf, 0);
      
      t0 = System.currentTimeMillis();
      writeLong(0);
      for(int i=N; i-->0; ) {
         writeLong(readLong()+1);
      }
      t1 = System.currentTimeMillis();
      System.out.println("direct buffer: " + (t1-t0) + " mls, v -> " + readLong() + ", buf  -> " + byteBuf);

      t0 = System.currentTimeMillis();
      writeLong1(0, buf, 0);
      for(int i=N; i-->0; ) {
         writeLong1(readLong1(buf, 0)+1, buf, 0);
      }
      t1 = System.currentTimeMillis();
      System.out.println("array: " + (t1-t0) + " mls, v -> " + readLong1(buf, 0) + ", buf -> " + Arrays.toString(buf));
      
      t0 = System.currentTimeMillis();
      writeLong1(0, buf, 0);
      for(int i=N; i-->0; ) {
         writeLong2(readLong2(buf, 0)+1, buf, 0);
      }
      t1 = System.currentTimeMillis();
      System.out.println("array + direct buffer: " + (t1-t0) + " mls, v -> " + readLong2(buf, 0) + ", buf  -> " + Arrays.toString(buf));
   }
}
