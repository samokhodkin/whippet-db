package io.github.whippetdb.util;

import static io.github.whippetdb.util.FastHash.hash64;

import java.io.IOException;

public class FastHashPerf {
   public static void main(String[] args) throws IOException{
      long v=1;
      long t0, dt;
      int M=90_0000_000;
      
      for(int i=0;i<M/2;i++) v=hash64(v);
      
      t0=System.currentTimeMillis();
      for(int i=M;i-->0;) v=hash64(v);
      dt=System.currentTimeMillis()-t0;
      System.out.println("hash64(long): "+dt+" mls, "+(M*1000f/dt)+" op/s");
   }
}
