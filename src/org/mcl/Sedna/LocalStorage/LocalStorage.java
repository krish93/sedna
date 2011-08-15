/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.LocalStorage;

/**
 *
 * @author daidong
 */
public interface LocalStorage {

    public boolean set(String key, Object value);
    public Object get(String key);
    public String stats();
    public boolean transfer(String vnode, String host);
    public boolean duplicate(String vnode, String host);
    public boolean exist(String key);
    
}
