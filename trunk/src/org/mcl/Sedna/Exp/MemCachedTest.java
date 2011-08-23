/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Exp;

import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.LocalStorage.LocalMemCached;

/**
 *
 * @author daidong
 */
public class MemCachedTest {
    
    LocalMemCached lmc = null;
    
    public MemCachedTest(){
        Configuration conf = new Configuration();
        lmc = new LocalMemCached(conf);
    }
    
    public String genValue(long size){
        StringBuilder sb = new StringBuilder();
        long times = size/10;
        for (long i = 0; i < times; i++){
            sb.append("abcdefghi");
        }
        return sb.toString();
    }
    
    public void run_test(int times){
        long MAX = 1*1024*1024;
        System.out.println("MAX: " + MAX);
        long vs = (MAX/times);
        System.out.println("vs: " + vs);
        String v = genValue(vs);
        long st = System.currentTimeMillis();
        
        
        for (int i = 0; i < times*1024; i++){
            lmc.set("key"+i, v);
        }
        
        long et = System.currentTimeMillis();
        System.out.println("Write " + times*1024 + " Costs " + (et - st));
        
        st = System.currentTimeMillis();
        
        for (int i = 0; i < times*1024 ; i++){
            lmc.get("key"+i);
        }
        
        et = System.currentTimeMillis();
        System.out.println("Read " + times*1024 + " Costs " + (et - st));

    }
    
    public static void main(String[] args){
        MemCachedTest mct = new MemCachedTest();
        int i = 60;
        mct.run_test(i);
    }
    
}
