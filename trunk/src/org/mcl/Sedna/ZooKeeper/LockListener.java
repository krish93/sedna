/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.ZooKeeper;

/**
 *
 * @author daidong
 */
public interface LockListener {

    public void lockAcquire();
    
    public void lockRelease();
}
