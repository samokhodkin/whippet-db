package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;

public class BbString {
   public static void main(String[] args) {
      ByteBuffer bb = ByteBuffer.allocateDirect(100).order(ByteOrder.LITTLE_ENDIAN);
      
      write(bb, 0, "abc");
      write(bb, 6, "def");
      write(bb, 12, "...");
      write(bb, 18, "xyz");
      
      System.out.println(read(bb,6,6));
      System.out.println(read1(bb,6,6));
      
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         read(bb,0,12);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         read1(bb,0,12);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         read2(bb,0,12);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
//      bb.asCharBuffer().append("ABCDEFGHIJKLMNOP");
//      System.out.println(bb);
//      ByteBuffer dup = bb.duplicate().order(ByteOrder.LITTLE_ENDIAN);
//      System.out.println(dup);
//      dup.clear().position(4);
//      System.out.println(dup.asCharBuffer());
   }
   
   static CharSequence read(ByteBuffer src, int off, int strLen) {
      ByteBuffer dup = src.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      dup.position(off).limit(off + (strLen<<1));
      return dup.asCharBuffer().toString();
   }
   
   static CharSequence read1(ByteBuffer src, int off, int strLen) {
      ByteBuffer dup = src.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      dup.position(off).limit(off + (strLen<<1));
      char[] buf = new char[strLen];
      dup.asCharBuffer().get(buf);
      return CharBuffer.wrap(buf);
   }
   
   static CharSequence read2(ByteBuffer src, int off, int strLen) {
      ByteBuffer dup = src.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      dup.position(off).limit(off + (strLen<<1));
      ByteBuffer buf = ByteBuffer.allocate(strLen<<1);
      buf.put(dup);
      return buf.asCharBuffer();
   }
   
   static void write(ByteBuffer dst, int off, CharSequence s) {
      ByteBuffer dup = dst.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      dup.clear().position(off);
      dup.asCharBuffer().append(s);
   }
}
