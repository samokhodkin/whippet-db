package io.github.whippetdb.memory.journaling.simple;

import java.io.File;
import java.io.IOException;

import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingDataIO;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingMemSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;
import io.github.whippetdb.util.Util;

@SuppressWarnings("unused")
public class JournalingIoPerf {
   private static long key;
   private static long t0 = System.currentTimeMillis();
   
   static long t() {
      return System.currentTimeMillis() - t0;
   }
   
   public static void main(String[] args) throws IOException {
      File dataFile = new File("tmp/SimpleJournalingDataIO.test");
      File logFile = new File("tmp/SimpleJournalingDataIO.test.wal");
      dataFile.delete();
      logFile.delete();
      
      final int lgPageSize = 6;
      final int pageSize = 1<<lgPageSize;
      
      SimpleFileDataSpace parent = new SimpleFileDataSpace(dataFile.getPath(), pageSize, lgPageSize);
      
      SimpleJournalingMemSpace ms = new SimpleJournalingMemSpace(parent, parent::flush, lgPageSize, 200_000, 2000_000){
         {
            log = new SimpleLog(logFile.getPath(), true){
               @Override
               public void start() {
//                  System.out.println("log.start(), key=" + key + ", t=" + t());
                  super.start();
               }
            };
            log.start();
         }
         @Override
         protected void flushPages() {
//            PageMap uniqueNativePages = new PageMap();
//            pageMap.scan(e -> uniqueNativePages.put((e.key<<lgPageSize)>>12));
//            System.out.println(
//               "SJMS.flushPages(), key=" + key + ", t=" + t() + ", dirty=" + log.dirtySize() + 
//               ", pages=" + pageMap.size() + ", size=" + ((SimpleFileDataSpace)parent).allocatedSize() + 
//               ", native pages=" + uniqueNativePages.size()
//            );
            super.flushPages();
         }
      };
      
      System.out.println("=== writing data ===");
      
      long t0, dt;
      int N = 20_000_000;
      int printMask = Util.ceil2(N/100)-1;
      
      t0 = System.currentTimeMillis();
      for(key = 1; key <= N; key++) {
         long a = ms.allocate(pageSize, true);
         ms.writeByte(a, 1);
         ms.writeShort(a + 1, 2);
         ms.writeInt(a + 3, 4);
         ms.writeLong(a + 8, 8);
         ms.writeLong(a + 16, 8);
         ms.writeLong(a + 24, 8);
         ms.writeLong(a + 32, 8);
         ms.commit();
         
         if((key & printMask) == 0) {
            System.out.printf("%d commits, %d bytes, %d mls\n", key, parent.allocatedSize(), System.currentTimeMillis() - t0);
         }
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println(N + " commits: " + dt + ", " + (N*1000f/dt) + " op/sec");
   }
}
