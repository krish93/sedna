/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Client;

import java.util.Random;
import org.apache.log4j.PropertyConfigurator;
import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class ClientMain {
    
    public static void main(String[] args){
        
        PropertyConfigurator.configure("conf/log4j.properties");
        
        Configuration conf = new Configuration();
        FileSystem fs = new FileSystem(conf);
        Random R = new Random(System.currentTimeMillis());
        String key = "Sedna" + R.nextInt();
        String value = "Hello World!";
        //fs.Set(key, value);
        //System.out.println("Get From Sedna: " + fs.Get(key));
        
        System.out.println("Write Begin");
        fs.write(key, value);
        System.out.println("Write OK");
        System.out.println("Get From Sedna: " + fs.read_latest(key));
    }
}
