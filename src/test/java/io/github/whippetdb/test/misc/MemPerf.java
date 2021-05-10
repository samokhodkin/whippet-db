package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.DirectDataIO;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;

public class MemPerf {
   public static void main(String[] args) {
      int n = 1<<8;
      int mask = n-1;
      int N = 1000_000_000;
      long t0, dt;
      
      MemDataSpace ms=new SimpleHeapDataSpace();
      long addr = ms.allocate(n<<3, false);
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         ms.writeLong(addr + ((i+1)&mask), ms.readLong(addr + (i&mask)));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("heap: " + dt);
      
      MemDataIO dms = new DirectDataIO() {{
            data = ByteBuffer.allocateDirect(n<<3);
         }
         protected void checkWrite(long addr, int len) {}
         protected void checkRead(long addr, int len) {}
      };
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         dms.writeLong(((i+1)&mask), dms.readLong((i&mask)));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("direct: " + dt);
      
      ByteBuffer bb = ByteBuffer.allocateDirect(n<<3);
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         bb.putLong((i+1)&mask, bb.getLong(i&mask));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("direct ByteBuffer: " + dt);
      
      bb = ByteBuffer.allocate(n<<3);
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         bb.putLong((i+1)&mask, bb.getLong(i&mask));
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println("heap ByteBuffer: " + dt);
   }
}
