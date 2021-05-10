package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;

public class MemString {
   public static void main(String[] args) {
      MemDataBuffer mem = new SimpleDirectDataBuffer(100);
      
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         write(mem, 0, "0123456789");
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         write1(mem, 0, "0123456789");
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
      
      t0 = System.currentTimeMillis();
      for(int i = 1<<27; i --> 0;) {
         write2(mem, 0, "0123456789");
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(dt);
   }
   
   static void write(MemDataBuffer mem, long addr, CharSequence str) {
      mem.write(addr, str);
   }
   
   static void write1(MemDataBuffer mem, long addr, CharSequence str) {
      int len = str.length();
      ByteBuffer bb = ByteBuffer.allocate(len<<1);
      bb.asCharBuffer().append(str);
      mem.write(addr, bb, 0, len<<1);
   }
   
   static void write2(MemDataBuffer mem, long addr, CharSequence str) {
      int len = str.length();
      byte[] data = new byte[len<<1];
      for(int i = 0; i < len; i++) {
         Bytes.writeShort(data, i<<1, str.charAt(i));
      }
      mem.write(addr, data, 0, len<<1);
   }
}
