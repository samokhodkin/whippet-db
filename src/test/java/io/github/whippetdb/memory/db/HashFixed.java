package io.github.whippetdb.memory.db;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.FastHash;

public class HashFixed {
   static long fnv(String s) {
      return fnv(new StringWrapper(s), 0, s.length()<<1);
   }
   static long fnv(MemDataIO data, long off, int len) {
      long end = len;
      long h = (0xcbf29ce484222325L ^ end) * 1099511628211L;
      int step = 5;
      while(end >= 8) {
         h += data.readLong(end-8);
         h *= 1099511628211L;
         end -= step;
      }
      if(end > 0) {
         h += data.readBytes(0, (int) end);
         h *= 1099511628211L;
      }
      return h;
   }
   
   static long fnv(long v) {
      return ((v*0xcbf29ce484222325L)+(Long.rotateRight(v,40)))*1099511628211L;
   }
   
   static long md(MessageDigest md, long v) {
      byte[] data = new byte[8];
      Bytes.writeLong(data, 0, v);
      byte[] hash = md.digest(data);
      return Bytes.readLong(hash, 0);
   }
   
   static long md(MessageDigest md, String s) {
      ByteBuffer bb = ByteBuffer.allocate(s.length()<<1);
      bb.asCharBuffer().append(s);
      byte[] hash = md.digest(bb.array());
      return Bytes.readLong(hash, 0);
   }
   
   static int N = 50_000_000;
   static long[] keys;// = new long[N];
   
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      //MessageDigest md = MessageDigest.getInstance("MD5");
      
      long t0, dt;
      Random random = new Random();
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         //keys[i] = md(md, i);
         //keys[i] = md(md, hash64(i));
         //keys[i] = md(md, "" + i);
         //keys[i] = md(md, "" + hash64(i));
         
         //keys[i] = hash64(i);
         //keys[i] = hash64(i*i);
         //keys[i] = hash64(hash64(i));
         //keys[i] = hash64(i*hash64(i));
         //keys[i] = hash64(i*hash64(i*i));
         //keys[i] = hash64(new StringWrapper("" + i));
         //keys[i] = hash64(new StringWrapper("" + hash64(i)));
         
         //keys[i] = fnv(i);
         //keys[i] = fnv(hash64(i));
         //keys[i] = fnv("" + i);
         //keys[i] = fnv("" + hash64(i));
         
         
         //keys[i] = fnv("" + fnv(i));
         //keys[i] = FastHash.hash64(new StringWrapper("" + i));
         //keys[i] = FastHash.hash64(i);
         
         //keys[i] = random.nextLong();
      }
      long keysDt = System.currentTimeMillis() - t0;
      
      MemDataSpace ms = new SimpleDirectDataSpace();
      //MemDataSpace ms = new SimpleFileDataSpace("tmp", 0);
      
      //MemMap.Cursor db = new MemMap(ms, 8, 8, 20).new Cursor(ms);
      MemMap.Cursor db = new MemMap(ms, 8, 8, 4).new Cursor(ms);
      
      MemDataBuffer mmKey = new SimpleDirectDataBuffer(8);
      
      Map<Long,Long> map = new Map<Long,Long>() {
         private MemDataBuffer mmKey = new SimpleDirectDataBuffer(8);
         private MemMap.Cursor db = new MemMap(ms, 8, 8, 11).new Cursor(ms);
         
         public int size() {
            return db.size();
         }

         @Override
         public boolean isEmpty() {
            boolean[] empty = new boolean[]{true};
            db.scan(()->{
               empty[0]=false;
               return true;
            });
            return empty[0];
         }

         @Override
         public boolean containsKey(Object key) {
            if(!(key instanceof Long)) return false;
            mmKey.writeLong(0, ((Long)key).longValue());
            return db.seek(mmKey, 0);
         }

         @Override
         public boolean containsValue(Object value) {
            if(!(value instanceof Long)) return false;
            long v = ((Long)value).longValue();
            boolean[] contains = new boolean[]{false};
            db.scan(()->{
               if(db.key.readLong(0) == v) {
                  return contains[0] = true;
               }
               return false;
            });
            return contains[0];
         }

         @Override
         public Long get(Object key) {
            long k = ((Long)key).longValue();
            mmKey.writeLong(0, k);
            return db.seek(mmKey, 0)? db.value.readLong(0): null;
         }

         @Override
         public Long put(Long key, Long value) {
            mmKey.writeLong(0, key);
            Long old = null;
            if(!db.put(mmKey, 0))  old = db.value.readLong(0);
            db.value.writeLong(0, value);
            return old;
         }

         @Override
         public Long remove(Object key) {
            long k = ((Long)key).longValue();
            mmKey.writeLong(0, k);
            if(!db.seek(mmKey, 0)) return null;
            Long old = db.value.readLong(0);
            db.delete();
            return old;
         }

         @Override
         public void putAll(Map<? extends Long, ? extends Long> m) {
            // TODO Auto-generated method stub
            
         }

         @Override
         public void clear() {
            // TODO Auto-generated method stub
            
         }

         @Override
         public Set<Long> keySet() {
            // TODO Auto-generated method stub
            return null;
         }

         @Override
         public Collection<Long> values() {
            // TODO Auto-generated method stub
            return null;
         }

         @Override
         public Set<java.util.Map.Entry<Long, Long>> entrySet() {
            // TODO Auto-generated method stub
            return null;
         }
      };
      
      System.out.println("Writing..");
      t0 = System.currentTimeMillis();
      for(int i = 0; i < N; i++) {
         Long key = key(i);
         Long value = value(i);
         
         mmKey.writeLong(0, key);
         if(!db.put(mmKey, 0)) throw new Error(i + ":" + key + ", clash with i=" + db.value.readLong(0));
         db.value.writeLong(0, new Long(i));
         // num keys, allocated, actually used, time
         if((i & 0x1fffff)==0) System.out.printf("%16d  %16d  %16d  %16d\n", i, ms.allocatedSize(), ms.allocatedSize(), System.currentTimeMillis()-t0);
         
//         if(map.put(key, value) != null) throw new Error(i + ": key=" + key);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/(dt + keysDt)) +  " op/sec total");
      System.out.println((N*1000f/dt) +  " op/sec without key generation");
      System.out.println((N*1000f/keysDt) +  " op/sec key generation alone");
//      LongList stat = db.stat();
//      System.out.println("stat=" + stat);
//      System.out.println("avg " + (stat.get(2)*1f/stat.get(1)) + " records in list");
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
      System.exit(0);
      
      
      System.out.println("Reading..");
      t0 = System.currentTimeMillis();
      for(int i = 0; i < N; i++) {
         long k = keys[i];
//         String s = "" + hash64(i);
//         key.setSize(0);
//         key.write(0, s);
//         long k = hash64(key);

         mmKey.writeLong(0, k);
         if(!db.seek(mmKey, 0)) throw new Error(i + ":" + k);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/(dt + keysDt)) +  " op/sec total");
      System.out.println((N*1000f/dt) +  " op/sec without key generation");
   }
   
   static MemDataBuffer keybuf = new SimpleDirectDataBuffer();
   static Random randon = new Random();
   
   static Long key(long i) {
//      return FastHash.hash64(i); // less random keys
      return FastHash.fnb64(i*FastHash.fnb64(i*i)); // highly random keys
//      String s = "" + FastHash.hash64(i);
//      keybuf.write(0, s);
//      return FastHash.hash64(keybuf, 0, s.length()<<1);
//      return randon.nextLong();
   }
   
   static Long value(long i) {
      return i;
   }
}
