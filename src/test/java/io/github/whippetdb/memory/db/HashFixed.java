package io.github.whippetdb.memory.db;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataArray;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.StringWrapper;
import static io.github.whippetdb.util.FastHash.*;

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
   
   static int N = 5_000_000;
   static long[] keys = new long[N];
   
   @SuppressWarnings("unused")
   public static void main(String[] args) throws Exception {
      //MessageDigest md = MessageDigest.getInstance("MD5");
      
      MemDataSpace ms = new SimpleDirectDataSpace();
      //MemDataSpace ms = new SimpleFileDataSpace("tmp", 0);
      MemMap.Cursor db = new MemMap(ms, 8, 32, 11).new Cursor(ms);
      MemDataBuffer mmKey = new SimpleDirectDataBuffer(8);
      long t0, dt;
      
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         //keys[i] = md(md, i);
         //keys[i] = md(md, hash64(i));
         //keys[i] = md(md, "" + i);
         //keys[i] = md(md, "" + hash64(i));
         
         //keys[i] = hash64(i);
         //keys[i] = hash64(hash64(i));
         //keys[i] = hash64(new StringWrapper("" + i));
         keys[i] = hash64(new StringWrapper("" + hash64(i)));
         
         //keys[i] = fnv(i);
         //keys[i] = fnv(hash64(i));
         //keys[i] = fnv("" + i);
         //keys[i] = fnv("" + hash64(i));
         
         
         //keys[i] = fnv("" + fnv(i));
         //keys[i] = FastHash.hash64(new StringWrapper("" + i));
         //keys[i] = FastHash.hash64(i);
         //keys[i] = fnv(i);
      }
      long keysDt = System.currentTimeMillis() - t0;
      
      System.out.println("Writing..");
      long keyCacheAddr = ms.allocate(8, true);
      long valueCacheAddr = ms.allocate(8, true);
      VarData value = new VarData(ms, valueCacheAddr, 20);
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         mmKey.writeLong(0, keys[i]);
         db.put(mmKey, 0);
         
         // mimic VarMap
         MemDataArray key = new StringWrapper("01234567890123456789");
         long mmValueAddr = db.currentAddr + MemMap.KEY_OFF + 8;
         VarData.put(ms, mmValueAddr, keyCacheAddr, 40, key, 0, (int)key.size());
//         ms.writeLong(mmValueAddr + 8, key.size());
//         value.connect(mmValueAddr + 16, mmValueAddr + 24);
//         value.write(0, "" + i);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/(dt + keysDt)) +  " op/sec total");
      System.out.println((N*1000f/dt) +  " op/sec without key generation");
      System.out.println((N*1000f/keysDt) +  " op/sec key generation alone");
      System.out.println("stat=" + db.stat());
      System.out.println("avg " + (db.stat().get(2)*1f/db.stat().get(1)) + " records in list");
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
   }
}
