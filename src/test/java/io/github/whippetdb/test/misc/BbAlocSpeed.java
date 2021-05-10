package io.github.whippetdb.test.misc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class BbAlocSpeed {
   public static void main(String[] args) throws InterruptedException {
      final int N = 10_000_000;
      final int maxSize = 10_000;
      final int bbCapacity = 256;
      List<ByteBuffer> heapBbList = new ArrayList<>();
      List<ByteBuffer> directBbList = new ArrayList<>();
      
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         heapBbList.add(ByteBuffer.allocate(bbCapacity));
         if(heapBbList.size() > maxSize) heapBbList.clear();
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " heap buffers: " + dt);
      
      Thread.sleep(1000);
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         heapBbList.add(ByteBuffer.allocateDirect(bbCapacity));
         if(heapBbList.size() > maxSize) heapBbList.clear();
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " direct buffers: " + dt);
   }
}
