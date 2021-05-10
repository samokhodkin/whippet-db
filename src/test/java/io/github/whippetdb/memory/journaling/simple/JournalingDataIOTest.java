package io.github.whippetdb.memory.journaling.simple;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingDataIO;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;
import io.github.whippetdb.util.Util;

public class JournalingDataIOTest {
   @Test
   public void test() throws IOException {
      File dataFile = new File("tmp/SimpleJournalingDataIO.test");
      File logFile = new File("tmp/SimpleJournalingDataIO.test.wal");
      dataFile.delete();
      logFile.delete();
      
      final int logPageSize = 6;
      final int pageSize = 1<<logPageSize;
      
      SimpleFileDataSpace parent = new SimpleFileDataSpace(dataFile.getPath(), pageSize, logPageSize);
      
      final int frameSize = SimpleLog.FRAME_DATA_OFF + SimpleLog.RECORD_DATA_OFF + pageSize;
      final int maxDirtyFrames = 100;
      final int maxFrames = 1000;
      final int maxDirtySize = frameSize*maxDirtyFrames;
      final int maxLogSize = frameSize*maxFrames;
      
      SimpleJournalingDataIO ms = new SimpleJournalingDataIO(parent, parent::flush, logPageSize, maxDirtySize, maxLogSize){{
         log = new SimpleLog(logFile.getPath());
      }};
      
      ms.log.scan(null, data->Util.assertEquals(0, 1, "must be empty"), null, null);
      ms.log.start();
      
      for(int i = 30; i --> 0;) {
         ms.writeLong(parent.allocate(pageSize, true), 111);
         ms.commit();
      }
      Util.assertEquals(ms.log.dirtySize(), 30*frameSize*1L);
      Util.assertEquals(ms.log.currentFrame(), 30*frameSize*1L);
      Util.assertEquals(ms.pageMap.size(), 30);
      
      for(int i = maxFrames; i --> 0;) {
         ms.writeLong(parent.allocate(pageSize, true), 222);
         ms.commit();
      }
      Util.assertEquals(ms.log.dirtySize(), 30*frameSize*1L);
      Util.assertEquals(ms.log.currentFrame(), 30*frameSize*1L);
      Util.assertEquals(ms.pageMap.size(), 30); // (30 + maxDirtyFrames) % maxDirtyFrames
      
      // TODO: other checks, including replay
//      
//      LongList addrList = new LongList();
//      for(int i = maxFrames; i --> 0;) {
//         long addr = parent.allocate(pageSize, true);
//         addrList.add(addr);
//         ms.writeLong(addr, 333);
//         ms.commit();
//      }
//      Util.assertEquals(ms.log.dirtySize(), 30*frameSize*1L);
//      Util.assertEquals(ms.log.currentFrame(), 30*frameSize*1L);
//      Util.assertEquals(ms.pageMap.size(), 30);
//      for(int i = addrList.size()-30; i < addrList.size(); i++) {
//         Util.assertNotEquals(parent.readLong(addrList.get(i)), 333L); // last 30 writes must be in the log, not in the parent
//      }
//      
//      long allocated = parent.allocatedSize();
//      
//      parent = new SimpleFileDataSpace(dataFile.getPath(), allocated, logPageSize);
//      
//      ms = new SimpleJournalingDataIO(parent, parent::flush, logPageSize, frameSize*maxDirtyFrames, frameSize*maxFrames) {{
//         log = new SimpleLog(logFile.getPath());
//         log.replay(parent);
//         log.start();
//      }};
//      
//      for(int i = addrList.size()-30; i < addrList.size(); i++) {
//         Util.assertEquals(ms.readLong(addrList.get(i)), 333L); // now everything must be in the parent
//      }
   }
}
