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
    
    public long run_test(int amount){

        long st = System.currentTimeMillis();
        
        for (int index = 0; index < amount; index++){
            String key = "test-" + String.valueOf(index);
            String value = String.valueOf(index);

            fs.write(key, value);
        }
        
        long et = System.currentTimeMillis();
        return (et - st);
    }
}
