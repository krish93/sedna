/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.CHash;

/**
 *
 * @author daidong
 */
public interface HashAlgorithm {

    public int hash(String key);
    public int virt(String key);
}
