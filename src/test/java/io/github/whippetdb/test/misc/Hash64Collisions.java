package io.github.whippetdb.test.misc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.SimpleHeapDataSpace;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.db.VarMap;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

@SuppressWarnings("unused")
public class Hash64Collisions {
   public static void main(String[] args) throws IOException {
      //runHashMap(30_000_000L);
      runMemMap(50_000_000L);
      //runVarMap(20_000_000L);
   }
   
   static void runMemMap(long numKeys) throws IOException {
      //MemDataSpace ms = new SimpleFileDataSpace("tmp/Hash64Collisions.test", 0);
      //MemDataSpace ms = new SimpleFileDataSpace("tmp/Hash64Collisions.test", 0);
      MemDataSpace ms = new SimpleDirectDataSpace();
      
      //MemDataSpace ms = new SimpleHeapDataSpace());
      long t0;
      MemMap.Cursor db = new MemMap(ms, 8, 8, 10).new Cursor(ms);
      MemDataIO key = new SimpleHeapDataBuffer(8);
      t0 = System.currentTimeMillis();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         key.writeLong(0, k);
         if(!db.put(key, 0)) {
            System.out.println("collision: " + db.value.readLong(0) + ", " + i + " -> " + k);
         }
         db.value.writeLong(0, i);
         if((i&0x3fffff)==0) System.out.printf("%16d  %16d  %16d\n", i, ms.allocatedSize(), System.currentTimeMillis()-t0);
      }
      long dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
      System.out.println((ms.allocatedSize()/numKeys) + " bytes/key");
      
      System.out.println("Reading");
      t0 = System.currentTimeMillis();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         key.writeLong(0, k);
         if(!db.seek(key, 0)) {
            System.out.println("Warning: missing key: " + k);
         }
         Util.assertEquals(db.value.readLong(0) == i, true);
         if((i&0x3fffff)==0) System.out.printf("%16d  %16d\n", i, System.currentTimeMillis()-t0);
      }
      dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
   }
   
   static void runHashMap(long numKeys) {
      long t0, dt;
      Map<Long,Long> db = new HashMap<>();
      
      t0 = System.currentTimeMillis();
      Runtime runtime = Runtime.getRuntime();
      long usedMemory0 = runtime.totalMemory()-runtime.freeMemory();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         if(db.put(k, i) != null) {
            System.out.println("collision: " + db.get(k) + ", " + i + " -> " + k);
         }
         if((i&0xfffff)==0) System.out.printf("%16d  %16d  %16d\n", i, runtime.totalMemory()-runtime.freeMemory()-usedMemory0, System.currentTimeMillis()-t0);
      }
      dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
      
      System.out.println("Reading");
      t0 = System.currentTimeMillis();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         Long v = db.get(k);
         if(v == null) {
            System.out.println("Warning: missing key: " + k);
         }
         else Util.assertEquals(v.longValue() == i, true);
         if((i&0xfffff)==0) System.out.printf("%16d  %16d\n", i, System.currentTimeMillis()-t0);
      }
      dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
   }
   
   static void runVarMap(long numKeys) throws IOException {
      long t0, dt;
      SimpleFileDataSpace ms = new SimpleFileDataSpace("tmp", 0);
      //SimpleHeapDataSpace ms = new SimpleHeapDataSpace();
      VarMap.Cursor db = new VarMap(ms, 8, 8, 10).new Cursor(ms);
      MemDataIO key = new SimpleHeapDataBuffer(8);
      //MemDataIO key = new SimpleDirectDataBuffer(8);
      System.out.println("Writing");
      t0 = System.currentTimeMillis();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         key.writeLong(0, k);
         if(!db.put(key, 0, 8)) {
            System.out.println("Collision: " + db.value.readLong(0) + ", " + i + " -> " + k);
         }
         db.value.writeLong(0, i);
         if((i&0xfffff)==0) System.out.printf("%16d  %16d  %16d\n", i, ms.allocatedSize(), System.currentTimeMillis()-t0);
      }
      dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
      
      System.out.println("Reading");
      t0 = System.currentTimeMillis();
      for(long i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         key.writeLong(0, k);
         if(!db.seek(key, 0, 8)) {
            System.out.println("Warning: missing key: " + k);
         }
         Util.assertEquals(db.value.readLong(0) == i, true);
         if((i&0xfffff)==0) System.out.printf("%16d  %16d\n", i, System.currentTimeMillis()-t0);
      }
      dt = System.currentTimeMillis()-t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
   }
}
