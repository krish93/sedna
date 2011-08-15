/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.StateMachine;

import java.io.IOException;
import java.nio.BufferOverflowException;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public interface SednaState {

    public boolean get(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public boolean set(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public Object cget(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public boolean cset(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public void dataMoveOut(String rnode, String vnode, int type, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public void dataMoveIn(String vnode, String rnodeSub, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public void datadupIn(String vnode, INonBlockingConnection conn) throws IOException, BufferOverflowException;
    public void start();
}
