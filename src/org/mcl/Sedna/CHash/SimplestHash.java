/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.CHash;

/**
 *
 * @author daidong
 */
public class SimplestHash implements HashAlgorithm{

    private int vNums = 0;

    public SimplestHash(int vnodes){
        vNums = vnodes;
    }
    public int hash(String key) {
        return key.hashCode();
    }

    public int virt(String key) {
        int hashId = hash(key);
        int average = Integer.MAX_VALUE / vNums;
        int index = hashId / average;
        return index;
    }

}
