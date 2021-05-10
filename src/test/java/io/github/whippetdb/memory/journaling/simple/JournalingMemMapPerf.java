package io.github.whippetdb.memory.journaling.simple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.journaling.simple.SimpleJournalingMemSpace;
import io.github.whippetdb.memory.journaling.simple.SimpleLog;
import io.github.whippetdb.util.AbstractLongMap;
import io.github.whippetdb.util.FastHash;
import io.github.whippetdb.util.Util;

@SuppressWarnings("unused")
public class JournalingMemMapPerf {
   static MemDataIO key = new SimpleHeapDataBuffer(8);
   static long t0 = System.currentTimeMillis();
   static MemMap.Cursor db;
   static SimpleJournalingMemSpace ms;
   
   static long t() {
      return System.currentTimeMillis() - t0;
   }
   
   public static void main(String[] args) throws IOException {
      int lgPageSize = 6;
      SimpleFileDataSpace parent = new SimpleFileDataSpace("tmp/JournalingMemMapSpeed.data", 1<<lgPageSize, lgPageSize);
      
      ms = new SimpleJournalingMemSpace(parent, parent::flush, lgPageSize, 200_000, 240_000_000){
         {
            log = new SimpleLog("tmp/JournalingMemMapSpeed.wal"){
               public void start() {
//                  System.out.println("log.start(), key=" + key.readLong(0) + ", t=" + t());
                  super.start();
               }
            };
            log.start();
         }
         
         @Override
         protected void flushPages() {
//            System.out.println(
//               key.readLong(0) + " " + t() + " " + log().dirtySize() + " " + pageMap.size() + " " + 
//               log().currentFrame() + " " + ((SimpleFileDataSpace)parent).allocatedSize()
//            );
            super.flushPages();
         }
      };
      
      db = new MemMap(ms, 8, 8, 10).new Cursor(ms);
      
      runWrites(10_000_000);
      //runReads(10_000_000);
   }

   static MemIO key(long v) {
      key.writeLong(0, v);
      return key;
   }

   static MemIO str(String s) {
      return new StringWrapper(s);
   }
   
   static MemIO bytes(int... v) {
      byte[] data = new byte[v.length];
      for(int i = 0; i < v.length; i++) data[i] = (byte)v[i];
      return new SimpleHeapDataBuffer(data);
   }
   
   static MemIO longs(long... v) {
      SimpleHeapDataBuffer buf = new SimpleHeapDataBuffer(v.length*8);
      for(int i = 0; i < v.length; i++) buf.writeLong(i*8, v[i]);
      return buf;
   }
   
   static void runWrites(int numKeys) {
      long t0 = System.currentTimeMillis();
      for(int i = 1; i <= numKeys; i++){
         long k = FastHash.hash64(i);
         if(!db.put(key(i), 0)) {
            System.out.println("Collision: " + db.value.readLong(0) + ", " + i + " -> " + k);
         }
         db.value.writeLong(0, k);
         ms.commit();
         
         if((i & 0xffff)==0) System.out.printf("%16d  %16d  %16d\n", i, ms.allocatedSize(), System.currentTimeMillis()-t0);
      }
      long dt = System.currentTimeMillis() - t0;
      System.out.println((numKeys*1000f/dt) + " op/sec");
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
   
   static class PageCountMap extends AbstractLongMap<PageCountMap.Entry> {
      class Entry extends AbstractLongMap.Entry implements Comparable<Entry> {
         int count;
         public int compareTo(Entry other) {
            return Integer.compare(other.count, count); //decreasing order
         }
         @Override
         public String toString() {
            return ""+count;
         }
      }
      protected Entry alloc() {
         return new Entry();
      }
   }
}
