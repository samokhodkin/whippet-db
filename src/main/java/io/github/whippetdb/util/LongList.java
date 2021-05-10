package io.github.whippetdb.util;

import java.util.Arrays;
import java.util.function.LongConsumer;

public class LongList{
   private long[] data;
   private int size;
   
   public LongList(){}
   
   public LongList(int size){
      ensureCapacity(size);
      this.size = size;
   }
   
   public LongList(long... data){
      this.data=data;
      size=data.length;
   }
   
   public static LongList wrap(long[] data){
      LongList list=new LongList();
      list.data=data;
      list.size=data.length;
      return list;
   }
   
   public final int add(long v){
      int s=size;
      ensureCapacity(s+1);
      data[s]=v;
      return size=s+1;
   }
   
   public final int add(LongList list){
      int s=size, ls=list.size;
      ensureCapacity(s+ls);
      System.arraycopy(list.data, 0, data, s, ls);
      return size=s+ls;
   }
   
   public final long pop(){
      return data[--size];
   }
   
   public final long peek(){
      return data[size-1];
   }
   
   public final long get(int ind){
      if(ind>=size) throw new IndexOutOfBoundsException(ind+">="+size);
      return data[ind];
   }
   
   public final void set(int ind, long v){
      if(ind>=size) throw new IndexOutOfBoundsException(ind+">="+size);
      data[ind]=v;
   }
   
   public final void clip(int newSize){
      size=Math.min(size, newSize);
   }
   
   public void insert(int ind, long v){
      if(ind>size) throw new IndexOutOfBoundsException(ind+">"+size);
      ensureCapacity(size+1);
      if(ind==size){
         data[size++]=v;
         return;
      }
      System.arraycopy(data, ind, data, ind+1, size-ind);
      data[ind]=v;
      size++;
   }
   
   public void remove(int ind, int len){
      if(ind+len>size) throw new IndexOutOfBoundsException((ind+len)+">"+size);
      if(ind+len==size){
         size=ind;
         return;
      }
      System.arraycopy(data, ind+len, data, ind, size-ind-len);
      size-=len;
   }
   
   public void replace(int ind, int len, long v){
      if(ind+len>size) throw new IndexOutOfBoundsException((ind+len)+">"+size);
      ensureCapacity(ind+1);
      if(len==0){
         insert(ind,v);
         return;
      }
      if(ind+len==size){
         data[ind]=v;
         size=ind+1;
         return;
      }
      System.arraycopy(data, ind+len, data, ind+1, size-len-ind);
      data[ind]=v;
      size-=len-1;
   }
   
   public void sort(){
      if(size > 0) Arrays.sort(data, 0, size);
   }
   
   // must be sorted
   public void unique(){
      if(size <= 1) return;
      int i1=0;
      for(int i2=1; i2<size; i2++){
         long v=data[i2];
         if(v==data[i1]) continue;
         data[++i1]=v;
      }
      size=i1+1;
   }
   
   // must be sorted
   public int binarySearch(long key){
      if(data==null) return -1;
      return Arrays.binarySearch(data, 0, size, key);
   }
   
   public void forEach(LongConsumer action) {
      for(int i = 0; i < size; i++) {
         action.accept(data[i]);
      }
   }
   
   public final int size(){
      return size;
   }
   
   public final int capacity(){
      return data == null? 0: data.length;
   }
   
   public final long[] data(){
      return data;
   }
   
   private final void ensureCapacity(int s){
      if(data==null) data=new long[s+10];
      else if(data.length<s){
         long[] newdata=new long[s+data.length/2+1];
         System.arraycopy(data,0,newdata,0,size);
         data=newdata;
      }
   }
   
   public String toString(){
      return size==0? "[]" : Util.toString(data, 0, size);
   }
   
   public static void main(String[] args){
      
   }
}
