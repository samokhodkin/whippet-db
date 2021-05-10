package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;

public class BBSpeed {
   public static void main(String[] args) {
      int L = 33;
      ByteBuffer bb1 = ByteBuffer.allocateDirect(L);
      ByteBuffer bb2 = ByteBuffer.allocateDirect(L);
      
      for(int i = 0; i < bb1.capacity(); i++ ) bb1.put(i, (byte)i);
      
      int N = 500_000_000;
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int i = N; i-->0; )  copy1(bb1, bb2);
      dt = System.currentTimeMillis() - t0;
      System.out.println("copy1: " + dt);
      
      t0 = System.currentTimeMillis();
      for(int i = N; i-->0; )  copy2(bb1, bb2);
      dt = System.currentTimeMillis() - t0;
      System.out.println("copy2: " + dt);
      
      t0 = System.currentTimeMillis();
      for(int i = N; i-->0; )  copy3(bb1, bb2);
      dt = System.currentTimeMillis() - t0;
      System.out.println("copy3: " + dt);
      
   }
   
   private static void copy1(ByteBuffer bb1, ByteBuffer bb2) {
      bb1.clear();
      bb2.clear();
      bb2.put(bb1);
   }
   
   private static void copy2(ByteBuffer bb1, ByteBuffer bb2) {
      int len = Math.min(bb1.capacity(), bb2.capacity());
      int off1 = 0;
      int off2 = 0;
      
      int frac = len & 0x7;
      int whole = len - frac;
      for(int i = 0; i < whole; i += 8) {
         bb2.putLong(off2+i, bb1.getLong(off1+i));
      }
      if((frac&4) != 0) {
         bb2.putInt(off2 + whole, bb1.get(off1 + whole));
         whole += 4;
      }
      if((frac&2) != 0) {
         bb2.putShort(off2 + whole, bb1.getShort(off1 + whole));
         whole += 2;
      }
      if((frac&1) != 0) {
         bb2.put(off2 + whole, bb1.get(off1 + whole));
      }
   }
   
   private static void copy3(ByteBuffer bb1, ByteBuffer bb2) {
      (bb1 = bb1.duplicate()).clear();
      (bb2 = bb2.duplicate()).clear();
      bb2.put(bb1);
   }
}
