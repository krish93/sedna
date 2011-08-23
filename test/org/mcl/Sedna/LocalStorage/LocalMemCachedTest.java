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
public class LocalMemCachedTest {
    
    public LocalMemCachedTest(){
        
    }
    
    public static void main(String[] args){
        LocalMemCached lmc = new LocalMemCached(new Configuration());
        lmc.duplicate("97", "192.168.1.15");
    }
}
