package io.github.whippetdb.util;

import java.util.*;
import java.util.function.Consumer;

//base class for specific maps
public abstract class AbstractLongMap<E extends AbstractLongMap.Entry>{
   public static class Entry{
      public long key;
      public int hash;
      int slot;
      Entry prev, next;
      
      public String toString(){
         return key+"->[]";
      }
      
      String toString_d(){
         return "{"+this+", p="+k2s(prev)+", n="+k2s(next)+"}";
      }
      
      private String k2s(Entry e){
         return e==null? "<>": String.valueOf(e.key);
      }
   }
   
   private Entry[] table=new Entry[128];
   protected int size=0;
   
   protected AbstractLongMap(){
      table=new Entry[128];
   }
   
   protected AbstractLongMap(int initialTableSize){
      table=new Entry[Util.ceil2(initialTableSize)];
   }
   
   @SuppressWarnings("unchecked")
   public E get(long key){
      Entry[] tab; Entry e;
      if((e=(tab=table)[hash(key)&(tab.length-1)])!=null){
         do if(e.key==key) return (E)e; while((e=e.next)!=null);
      }
      return null;
  }
   
   //find or create, never return null
   @SuppressWarnings("unchecked")
   public E put(long key){
      Entry[] tab; Entry e; int h;
      if((e=(tab=table)[(h=hash(key))&(tab.length-1)])!=null){
         do if(e.key==key) return (E)e; while((e=e.next)!=null);
      }
      return add(key, h);
   }
   
   public void remove(long key){
      E e;
      if((e=get(key))!=null) remove(e);
   }
   
   public void remove(E e){
      Entry prev;
      Entry next=e.next;
      if((prev=e.prev)!=null) prev.next=next;
      else table[e.slot]=next;
      if(next!=null) next.prev=prev;
      size--;
   }
   
   public int size(){
      return size;
   }
   
   public void clear(){
      Arrays.fill(table,null);
      size=0;
   }
   
   protected abstract E alloc();
   
   protected E alloc(long key, int hash){
      E e=alloc();
      e.key=key;
      e.hash=hash;
      return e;
   }
   
   protected E add(long key, int hash){
      E e; Entry[] tab;
      if(size>(tab=table).length) table=tab=rehash(tab, tab.length<<3);
      add(tab, e=alloc(key,hash));
      size++;
      return e;
   }
   
   private static Entry[] rehash(Entry[] oldTable, int newLength){
      Entry[] newTable=(Entry[])new Entry[newLength];
      for(int i=oldTable.length; i-->0;){
         Entry e=oldTable[i];
         while(e!=null){
            Entry next=e.next;
            add(newTable, e);
            e=next;
         }
      }
      return newTable;
   }
   
   private static void add(Entry[] table, Entry e){
      int slot; Entry e0;
      if((e0=table[e.slot=slot=e.hash&(table.length-1)])!=null) e0.prev=e;
      e.next=e0;
      e.prev=null;
      table[slot]=e;
   }
   
   private static int hash(long v){
      int h=(int)(v^(v>>>32));
      h^=h>>>16;
      h^=h>>>8;
      return h;
   }
   
   @SuppressWarnings("unchecked")
   public void scan(Consumer<E> feed){
      for(int i=0; i<table.length; i++){
         Entry e=table[i];
         while(e!=null){
            feed.accept((E)e);
            e=e.next;
         }
      }
   }
   
   public String toString(){
      StringBuilder sb=new StringBuilder("[");
      for(int i=0; i<table.length; i++){
         Entry e=table[i];
         while(e!=null){
            if(sb.length()>1) sb.append(", ");
            sb.append(e);
            e=e.next;
         }
      }
      return sb.append("]").toString();
   }
   
   public String toString_d(){
      StringBuilder sb=new StringBuilder("[");
      for(int i=0; i<table.length; i++){
         sb.append('\n').append(i).append(": ");
         Entry e=table[i];
         while(e!=null){
            sb.append(e.toString_d()).append(", ");
            e=e.next;
         }
      }
      return sb.append("\n]").toString();
   }
   
   public void check(){
      int c=0;
      for(int i=0; i<table.length; i++){
         c+=check(null, table[i]);
      }
      Util.assertEquals(c, size, "size vs # actual entries");
   }
   static int check(Entry e1, Entry e2){
      if(e1!=null) Util.assertEquals(e1.next, e2, "next");
      if(e2!=null) Util.assertEquals(e1, e2.prev, "prev");
      if(e2==null) return 0;
      return 1+check(e2, e2.next);
   }
   
   public static void main(String[] args){
      class TestLongMap extends AbstractLongMap<Entry>{
         protected Entry alloc(){
            return new Entry();
         }
      };
      TestLongMap map=new TestLongMap();
      
      int N=40000;
      for(int i=0; i<N; i++) map.put(i*143);
      Util.assertEquals(map.size(), N);
      
      map.check();
      System.out.println("check ok");
      
      for(int i=N; i<2*N; i++) Util.assertEquals(map.get(i*143), null);
      
      for(int i=0; i<N; i++){
         long key=i*143;
         Entry e=map.get((int)key);
         Util.assertNotEquals(e,null,"key="+key+" at i="+i);
         Util.assertEquals(e.key,key,"key="+key+" at i="+i);
         map.remove(e);
      }
      map.check();
      Util.assertEquals(map.size(), 0, "map.size()");
      
      
      int M=40_000_000;
      int m=10;
      map=new TestLongMap();
      
      long key=143;
      map.put(key);
      
      long t0, dt;
      
      int sum=0;
      
      System.out.println("timing LOM.get");
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.get(key);
      t0=System.currentTimeMillis();
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.get(key);
      dt=System.currentTimeMillis()-t0;
      System.out.println("LOM get: "+(M*m*1000f/dt)+" op/s");
      
      System.out.println("timing LOM.put");
      t0=System.currentTimeMillis();
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.put(key);
      dt=System.currentTimeMillis()-t0;
      System.out.println("LOM put: "+(M*m*1000f/dt)+" op/s");
      System.out.println(sum);
   }
}
