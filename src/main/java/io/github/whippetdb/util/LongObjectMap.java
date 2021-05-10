package io.github.whippetdb.util;

public class LongObjectMap<V> extends AbstractLongMap<LongObjectMap.Entry<V>>{
   public static class Entry<T> extends AbstractLongMap.Entry{
      public T value;
   }
   
   public V getValue(long key){
      Entry<V> e=super.get(key);
      return e==null? null: e.value;
   }
   
   protected Entry<V> alloc(){
      return new Entry<V>();
   }
   
   public static void main(String[] args){
      LongObjectMap<Integer> map=new LongObjectMap<>();
      int N=40000;
      for(int i=0; i<N; i++) map.put(i*143).value=i;
      Util.assertEquals(map.size(), N);
      
      map.check();
      System.out.println("check ok");
      
      for(int i=N; i<2*N; i++) Util.assertEquals(map.get(i*143), null);
      
      for(int i=0; i<N; i++){
         long key=i*143;
         Entry<Integer> e=map.get((int)key);
         Util.assertNotEquals(e,null,"key="+key+" at i="+i);
         Util.assertEquals(e.key,key,"key="+key+" at i="+i);
         map.remove(e);
      }
      Util.assertEquals(map.size(), 0, "map.size()");
      
      int M=40_000_000;
      int m=10;
      map=new LongObjectMap<>();
      //MyHashMap<Integer, Integer> map1=new MyHashMap<>();
      
      long key=143;
      Integer value=10;
      map.put(key).value=value;
      
      long t0, dt;
      
      int sum=0;
      
      System.out.println("timing LOM.get");
      for(int i=M; i-->0;) for(int k=m; k-->0;) sum+=map.get(key).value;
      for(int i=M; i-->0;) for(int k=m; k-->0;) sum+=map.get(key).value;
      t0=System.currentTimeMillis();
      for(int i=M; i-->0;) for(int k=m; k-->0;) sum+=map.get(key).value;
      dt=System.currentTimeMillis()-t0;
      System.out.println("LOM get: "+(M*m*1000f/dt)+" op/s");
      
      System.out.println("timing LOM.put");
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.put(key).value=value;
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.put(key).value=value;
      t0=System.currentTimeMillis();
      for(int i=M; i-->0;) for(int k=m; k-->0;) map.put(key).value=value;
      dt=System.currentTimeMillis()-t0;
      System.out.println("LOM put: "+(M*m*1000f/dt)+" op/s");
      System.out.println(sum);
   }
}