package io.github.whippetdb.test.misc;

import java.io.File;
import java.io.IOException;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingMemSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

/**
 * Unexpected error.
 * When we create new journaling DB over the existing file, 
 * the old bytes get into the db, causing random errors.
 * 
 * UPD. The cause is explained in SimpleJournalingMemSpace.
 */
public class AlignmentError {
   static MemDataIO key = new SimpleHeapDataBuffer(8);
   static long t0 = System.currentTimeMillis();
   static MemMap.Cursor db;
   static SimpleFileDataSpace parent;
   static SimpleLog log;
   static SimpleJournalingMemSpace ms;
   
   static long t() {
      return System.currentTimeMillis() - t0;
   }
   
   public static void main(String[] args) throws IOException {
      int lgPageSize = 6;
      
      String path = "tmp/AlignmentError.data";
      // generate random data file
      if(!new File(path).exists()) {
         int size = 800_000;
         parent = new SimpleFileDataSpace(path, size, 0);
         for(int i = 0; i < size; i+=8) {
            parent.writeLong(i, FastHash.hash64(i));
         }
         parent.close();
         parent = null;
         System.gc();
      }
      
      // if we delete the file, error doesn't happen
      //new File("tmp/JournalingMemMapSpeed.data").delete();
      
      // create new db over the existing file
      
      // this causes error
      parent = new SimpleFileDataSpace(path, 1<<lgPageSize, 0){
         // allow last page IO through size boundary
         @Override
         protected void ensureCapacity(long v) {
            super.ensureCapacity(v + (1<<lgPageSize));
         }
         @Override
         protected void checkRead(long addr, int len) {
            Util.checkRead(addr, len, size + (1<<lgPageSize));
         }
         @Override
         protected void checkWrite(long addr, int len) {
            Util.checkWrite(addr, len, size + (1<<lgPageSize));
         }
      };
      
      // this doesn't cause error
      // parent = new SimpleFileDataSpace(path, 1<<logPageSize, logPageSize);
      
      open(lgPageSize);
      
      // see error in db;
      // this shouldn't happen, because new allocations are cleared (see SimpleFileDataSpace.allocate())
      runWrites(10_000_000);
   }
   
   static void open(int logPageSize) throws IOException {
      if(parent == null) throw new IllegalArgumentException();
      ms = new SimpleJournalingMemSpace(parent, parent::flush, logPageSize, 200_000, 64_000_000){{
         log = AlignmentError.log = new SimpleLog("tmp/AlignmentError.wal");
         log.start();
      }};
      db = new MemMap(ms, 8, 8, 10).new Cursor(ms);
   }
   
   static void close() {
      if(parent != null) parent.close();
      if(log != null) log.close();
      ms = null;
   }

   static MemIO key(long v) {
      key.writeLong(0, v);
      return key;
   }
   
   static void runWrites(int numKeys) {
      for(int i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         if(!db.put(key(i), 0)) {
            System.out.println("Collision: " + db.value.readLong(0) + ", " + i + " -> " + k);
         }
         db.value.writeLong(0, k);
         ms.commit();
         
         if((i & 0xffff)==0) System.out.printf("%16d  %16d  %16d\n", i, ms.allocatedSize(), System.currentTimeMillis()-t0);
      }
   }
   
   static void runReads(int numKeys) {
      long t0 = System.currentTimeMillis();
      for(int i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         if(!db.seek(key(i), 0)) {
            System.out.println("Warning: missing key: " + k);
            break;
         }
         Util.assertEquals(db.value.readLong(0), k);
         if((i & 0xfffff)==0) System.out.printf("%16d  %16d\n", i, System.currentTimeMillis()-t0);
      }
      long dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
   }
}
