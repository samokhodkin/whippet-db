package io.github.whippetdb.memory.db;

import static io.github.whippetdb.util.FastHash.hash64;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;

import io.github.whippetdb.memory.api.Bytes;
import io.github.whippetdb.memory.api.MemDataBuffer;
import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.basic.SimpleDirectDataBuffer;
import io.github.whippetdb.memory.basic.SimpleDirectDataSpace;
import io.github.whippetdb.memory.basic.StringWrapper;
import io.github.whippetdb.util.LongList;

public class MimicVarMap {
   static long fnb(String s) {
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
   
   static long fnb(long v) {
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
   
   static int N = 12_000_000;
   static long[] keys = new long[N];
   
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
         //keys[i] = fnb(i*fnb(1L*i*i));
         keys[i] = hash64(new StringWrapper("" + i));
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
      
      //MemMap.Cursor db = new MemMap(ms, 8, 8, 10).new Cursor(ms);
      //MemMap4.Cursor db = new MemMap4(ms, 8, 8, 10).new Cursor(ms);
      //for VarMap
      MemMap.Cursor db = new MemMap(ms, 8, 32, 4).new Cursor(ms);
      //try new design
      //MemMap.Cursor db = new MemMap(ms, 8, 16, 10).new Cursor(ms);
      
      MemDataBuffer mmKey = new SimpleDirectDataBuffer(8);
      
      System.out.println("Writing..");
      long keyCacheAddr = ms.allocate(8, true);
      long valueCacheAddr = ms.allocate(8, true);
      
      VarData valueData = new VarData(ms, valueCacheAddr, 20);
      // new design: key + value in one vardata
      //VarData valueData = new VarData(ms, valueCacheAddr, 40 + 20);
      
      t0 = System.currentTimeMillis();
      MemDataBuffer key = new SimpleDirectDataBuffer();
      key.write(0, "01234567890123456789");
      for(int i = N; i --> 0;) {
         long k = keys[i];
//         String s = "" + hash64(i);
//         //key.setSize(0);
//         key.write(0, s);
//         key.setSize(s.length()<<1);
//         long k = hash64(key);
         
         mmKey.writeLong(0, k);
         if(!db.put(mmKey, 0)) throw new Error(i + ":" + k + ", clash with i=" + db.value.readLong(0));
         db.value.writeLong(0, i);
         
         // mimic VarMap (this part alone is ~ 4.5M op/s, very slow)
         long mmValueAddr = db.currentAddr + MemMap.KEY_OFF + 8;
         ms.fillLong(mmValueAddr + 0, 32, 0);
         VarData.create(ms, mmValueAddr, keyCacheAddr, 40, key, 0, (int)key.size());
         ms.writeLong(mmValueAddr + 8, key.size());
         valueData.connect(mmValueAddr + 16, mmValueAddr + 24);
         valueData.write(0, "" + i);
         
         // new VarMap design
//         long mmValueAddr = db.currentAddr + MemMap.KEY_OFF + 8;
//         ms.fillLong(mmValueAddr + 0, 16, 0);
//         valueData.connect(mmValueAddr, mmValueAddr + 8);
//         valueData.write(0, key, 0, (int)key.size());
//         String v = "" + i;
//         valueData.write((int)key.size(), v);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/(dt + keysDt)) +  " op/sec total");
      System.out.println((N*1000f/dt) +  " op/sec without key generation");
      System.out.println((N*1000f/keysDt) +  " op/sec key generation alone");
      LongList stat = db.stat();
      System.out.println("stat=" + stat);
      System.out.println("avg " + (stat.get(2)*1f/stat.get(1)) + " records in list");
      System.out.println((ms.allocatedSize() / N) + " bytes/key");
      
      
      System.out.println("Reading..");
      t0 = System.currentTimeMillis();
      for(int i = N; i --> 0;) {
         long k = keys[i];
//         String s = "" + hash64(i);
//         key.setSize(0);
//         key.write(0, s);
//         long k = hash64(key);

         mmKey.writeLong(0, k);
         if(!db.seek(mmKey, 0)) throw new Error(i + ":" + k);
         
         // mimic VarMap
         long mmValueAddr = db.currentAddr + MemMap.KEY_OFF + 8;
         if(!VarData.equals(ms, mmValueAddr, 40, key, 0, (int)key.size())) throw new Error("bad stored key at "+i);
         //?? connect value
         //if(!valueData.readString(0).toString().equals("" + i)) throw new Error("bad value at " + i);
         //new design
//         if(!VarData.equals(ms, mmValueAddr + 8, 40+20, key, 0, (int)key.size())) throw new Error("bad stored key at "+i);
         //?? connect value
         //if(!valueData.readString(0).toString().equals("" + i)) throw new Error("bad value at " + i);
      }
      dt = System.currentTimeMillis() - t0;
      System.out.println((N*1000f/(dt + keysDt)) +  " op/sec total");
      System.out.println((N*1000f/dt) +  " op/sec without key generation");
   }
}
