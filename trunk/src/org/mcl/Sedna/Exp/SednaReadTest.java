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
    
    public long run_test(int amount, int nodeId){
        
        long st = System.currentTimeMillis();
        for (int index = 0; index < amount*1024; index++){
            String key = "test-" + nodeId + "-" + String.valueOf(index);
            String v = fs.read_latest(key);
        }
        
        long et = System.currentTimeMillis();
        return (et - st);
    }
    
    public static void main(String[] args) throws IOException{
        PropertyConfigurator.configure("conf/log4j.properties");
        System.out.println("Sedna Test Suit, args length: " + args.length);
        String nodeId = args[0];
        System.out.println("args are: " + nodeId);
        
        int nid = 0;
        if (nodeId != null)
            nid = Integer.parseInt(nodeId);
        
        FileWriter fw = new FileWriter("logs/sedna_write_plot.txt");
        FileWriter fr = new FileWriter("logs/sedna_read_plot.txt");
        

        int i = 10;
        

        SednaWriteTest srt = new SednaWriteTest();
        SednaReadTest srr = new SednaReadTest();

        System.out.println("**************** Read Test number: " + i + " ****************");


        long wt = srt.run_test(i, nid);
        System.out.println("*** Write Time: " + wt);
        fw.write(i + "\t" + wt + "\n");
        fw.flush();
        fw.close();

        
        long rt = srr.run_test(i, nid);
        System.out.println("*** Read  Time: " + rt);
        fr.write(i + "\t" + rt + "\n");
        fr.flush();
        fr.close();
    }
}
