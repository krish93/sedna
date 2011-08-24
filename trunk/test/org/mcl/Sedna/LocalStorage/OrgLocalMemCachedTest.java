/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.LocalStorage;

import org.mcl.Sedna.Configuration.Configuration;

/**
 *
 * @author daidong
 */
public class OrgLocalMemCachedTest {

    public static void main(String[] args){
        Configuration conf = new Configuration();
        OrgLocalMemCached olmc = new OrgLocalMemCached(conf);
        /*
        for (int i = 0; i < 1000; i++){
            olmc.set("test"+i, String.valueOf(i));
        }
        
        for (int v = 0; v < 100; v++){
            String virt = String.format("%08d", v);
            olmc.duplicate(virt, "192.168.1.10:11211");
        }
        */
        String value = (String) olmc.get("notest");
        if (value == null){
            System.out.println("Null return null");
            return;
        }
        if (value.equals("null")){
            System.out.println("Null return String null");
            return;
        }
    }
}
