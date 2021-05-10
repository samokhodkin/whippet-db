package io.github.whippetdb.db.internal;

import java.io.IOException;

import io.github.whippetdb.memory.journaling.simple.SimpleJournalingMemSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;

class JournalingDbMemSpace extends SimpleJournalingMemSpace {
   private final DbFile dbf;
   
   public JournalingDbMemSpace(
         DbFile dbf, int lgPageSize, int maxDirtySpace, long maxLogSize
   ) throws IOException {
      this(dbf, null, lgPageSize, maxDirtySpace, maxLogSize);
   }
      
   public JournalingDbMemSpace(
      DbFile dbf, String logPath, int lgPageSize, int maxDirtySpace, long maxLogSize
   ) throws IOException {
      super(dbf, dbf::flush, lgPageSize, maxDirtySpace, maxLogSize);
      this.dbf = dbf;
      log = new SimpleLog(logPath != null? logPath: dbf.defaultLogPath(), dbf.persistent);
      log.start();
   }
      
   @Override
   public long allocate(int size, boolean clear) {
      long addr = super.allocate(size, clear);
      log.writeLong(log.alloc(dbf.recordedSizeAddr(), 8), dbf.allocatedSize());
      return addr;
   }
}
