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
    
    public void run_test(int times){
        long st = System.currentTimeMillis();
        
        for (int i = 0; i < times; i++){
            lmc.set("key"+i, "key"+i);
        }
        
        long et = System.currentTimeMillis();
        System.out.println("Write " + times + " Costs " + (et - st));
        
        st = System.currentTimeMillis();
        
        for (int i = 0; i < times; i++){
            lmc.get("key"+i);
        }
        
        et = System.currentTimeMillis();
        System.out.println("Read " + times + " Costs " + (et - st));

    }
    
    public static void main(String[] args){
        MemCachedTest mct = new MemCachedTest();
        int i = 30000;
        mct.run_test(i);
    }
    
}
