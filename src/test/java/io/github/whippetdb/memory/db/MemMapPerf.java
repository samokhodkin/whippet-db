package io.github.whippetdb.memory.db;

import static io.github.whippetdb.util.FastHash.hash64;

import java.io.IOException;

import io.github.whippetdb.memory.api.MemDataIO;
import io.github.whippetdb.memory.api.MemDataSpace;
import io.github.whippetdb.memory.api.MemIO;
import io.github.whippetdb.memory.basic.SimpleFileDataSpace;
import io.github.whippetdb.memory.basic.SimpleHeapDataBuffer;
import io.github.whippetdb.memory.db.MemMap;
import io.github.whippetdb.memory.db.MemMap.Cursor;
import io.github.whippetdb.util.Util;

/**
 * mixed functional and speed tests; TODO: split and automate
 */

public class MemMapPerf {
   public static void main(String[] args) throws IOException {
      int keySize=5;
      int valueSize=8;
      int L=5; //list size
      int N=2*L+1;
//      MemDataSpace ms=new SimpleHeapDataSpace();
      MemDataSpace ms=new SimpleFileDataSpace("tmp", 0);
      MemMap map=new MemMap(ms,keySize,valueSize,L);
      Cursor c=map.new Cursor(ms);
      
      for(int i=0;i<N;i++){
         System.out.println(i+": "+c.put(bytes(1,2,3,4,i),0));
      }
      System.out.println();
      System.out.println(c.toString_d(true, true));
      
      for(int i=0;i<N;i++){
         if(c.seek(bytes(1,2,3,4,i),0)){
            System.out.println(i+": found, deleting");
            c.delete();
         }
         else{
            System.out.println(i+": not found, error!");
         }
      }
      System.out.println("after deletion: ");
      System.out.println("map: "+c.toString());
      System.out.println("sructure: "+c.toString_d(true, true));
      
      System.out.println("testing multiple values");
      for(int i=0;i<L;i++){
         MemIO key=bytes(1,2,3,4,i);
         Util.assertEquals(c.put(key,0), true);
         c.value.writeLong(0,0x0100010001000100L);
         c.addValue();
         c.value.writeLong(0,0x0200020002000200L);
      }
      System.out.println("put "+L+"*2 ok");
      System.out.println("Map -> ");
      System.out.println("ms -> " + ms);
      System.out.println(c.toString());
      
      System.out.println("deleting first values:");
      for(int i=0;i<L;i++){
         MemIO key=bytes(1,2,3,4,i);
         Util.assertEquals(c.seek(key,0), true, "seek(" + key + ")");
         c.delete();
      }
      System.out.println("ok");
      System.out.println("Map -> " + c.toString());
      
      System.out.println("deleting second values:");
      for(int i=0;i<L;i++){
         MemIO key=bytes(1,2,3,4,i);
         Util.assertEquals(c.seek(key,0), true, "seek(" + key + ")");
         Util.assertEquals(c.value.readLong(0), 0x0200020002000200L, "value");
         c.delete();
      }
      System.out.println("ok");
      System.out.println("Map -> " + c.toString());
      
      System.out.println("Map struct: ");
      System.out.println(c.toString_d(true, true));
      System.out.println("multiple values ok");
      
      System.out.println();
      System.out.println("Testing speed");
      long t0,dt;
      int n=100_000;
      N=40_000_000/n;
      long[] keys=new long[n];
      for(int i=n; i-->0;) keys[i]=hash64(i+1);
      MemMap.Cursor db=new MemMap(ms,8,8,5).new Cursor(ms);
      MemDataIO key=new SimpleHeapDataBuffer(8);
      
      t0=System.currentTimeMillis();
      for(int i=N; i-->0;) for(int j=n; j-->0;){
         key.writeLong(0, keys[j]);
         db.put(key, 0);
         db.value.writeLong(0,~keys[j]);
      }
      dt=System.currentTimeMillis()-t0;
      System.out.println((n*N)+" puts took "+dt+" mls, "+(n*N*1000f/dt)+" op/sec");
      
      for(int i=N; i-->0;) for(int j=n; j-->0;){
         key.writeLong(0, keys[j]);
         if(db.seek(key, 0)) db.value.readLong(0);
      }
      t0=System.currentTimeMillis();
      for(int i=N; i-->0;) for(int j=n; j-->0;){
         key.writeLong(0, keys[j]);
         if(db.seek(key, 0)) db.value.readLong(0);
      }
      dt=System.currentTimeMillis()-t0;
      System.out.println((n*N)+" gets took "+dt+" mls, "+(n*N*1000f/dt)+" op/sec");
      
      System.out.println("checking");
      for(int j=n; j-->0;){
         key.writeLong(0, keys[j]);
         Util.assertEquals(db.seek(key,0), true, "seek");
         Util.assertEquals(db.value.readLong(0), ~keys[j], "value");
      }
      System.out.println("ok");
   }
   
   static MemIO bytes(int... data){
      MemIO buf=new SimpleHeapDataBuffer(data.length);
      for(int i=data.length;i-->0;) buf.writeByte(i,data[i]);
      return buf;
   }
}
