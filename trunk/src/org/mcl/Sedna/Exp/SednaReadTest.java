/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Exp;

import java.io.FileWriter;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import org.mcl.Sedna.Client.FileSystem;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class SednaReadTest {
    FileSystem fs = null;
    
    public SednaReadTest(){
        Configuration conf = new Configuration();
        fs = new FileSystem(conf); 
    }
    
    public long run_test(int amount){
           
        long st = System.currentTimeMillis();
        for (int index = 0; index < amount; index++){
            String key = "test-" + String.valueOf(index);
            String v = fs.read_latest(key);
        }
        
        long et = System.currentTimeMillis();
        return (et - st);
    }
    
    public static void main(String[] args) throws IOException{
        PropertyConfigurator.configure("conf/log4j.properties");
        
        FileWriter fw = new FileWriter("logs/sedna_write_plot.txt");
        FileWriter fr = new FileWriter("logs/sedna_read_plot.txt");
        
        int startNum = 100;
        int stopNum = 5000;
        int step = 100;
        int i = 30000;
        
        //for (i = startNum; i <= stopNum; i = i + step){
            SednaWriteTest srt = new SednaWriteTest();
            SednaReadTest srr = new SednaReadTest();
            
            System.out.println("**************** Read Test number: " + i + " ****************");

            
            long wt = srt.run_test(i);
            System.out.println("*** Write Time: " + wt);
            fw.write(i + "\t" + wt + "\n");
            
            long rt = srr.run_test(i);
            System.out.println("*** Read  Time: " + rt);
            fr.write(i + "\t" + rt + "\n");
            
            
        //}
        fw.flush();
        fw.close();
        fr.flush();
        fr.close();
    }
}
