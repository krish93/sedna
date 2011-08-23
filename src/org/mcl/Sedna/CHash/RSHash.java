/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.CHash;

/**
 *
 * @author daidong
 */
public class RSHash implements HashAlgorithm{

    private int vNums = 0;
    
    public RSHash(int vnodes){
        vNums = vnodes;
    }
    
    public int hash(String key) {
        int b = 378551;
        int a = 63689;
        int hash = 0;
        
        byte[] bw = key.getBytes();
        for (int i = 0; i < bw.length; i++){
            hash = hash * a + bw[i];
            a *= b;
        }
        return (hash & 0x7FFFFFF);
    }

    public int virt(String key) {
        int hashId = hash(key);
        //int average = Integer.MAX_VALUE / vNums;
        int average = vNums;
        int index = hashId % average;
        return index;
    }
    
}
