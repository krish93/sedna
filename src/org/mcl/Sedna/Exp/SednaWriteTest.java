/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Exp;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import org.mcl.Sedna.Client.FileSystem;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class SednaWriteTest {
    FileSystem fs = null;
    
    public SednaWriteTest(){
        Configuration conf = new Configuration();
        fs = new FileSystem(conf);
    }
    
    public String genValue(long size){
        StringBuilder sb = new StringBuilder();
        long times = size/10;
        for (long i = 0; i < times; i++){
            sb.append("abcdefghi");
        }
        return sb.toString();
    }
    
    public long run_test(int amount, int nid){
        long MAX = 1*1024;
        long vs = MAX/amount;
        String v = genValue(vs);
        
        long st = System.currentTimeMillis();
        
        for (int index = 0; index < amount * 1024; index++){
            String key = "test-" + nid + "-" + String.valueOf(index);

            fs.write(key, v);
        }
        
        long et = System.currentTimeMillis();
        return (et - st);
    }
}
