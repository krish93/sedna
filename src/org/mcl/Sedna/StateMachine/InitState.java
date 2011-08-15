/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.StateMachine;

import java.io.IOException;
import java.nio.BufferOverflowException;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Communication.SednaServer;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.LocalStorage.LocalMemCached;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class InitState implements SednaState{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(InitState.class);
    }
    private Sedna sed = null;

    public InitState(Sedna s){
        this.sed = s;
    }

    public boolean get(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return true;
    }

    public boolean set(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return true;
    }

    public Object cget(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return true;
    }

    public boolean cset(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return true;
    }

    public void dataMoveOut(String rnode, String vnode, int type, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return;
    }

    public void dataMoveIn(String vnode, String rnodeSub, INonBlockingConnection conn){
        LOG.error("In Init State, There should not any get operation yet");
        return;
    }
    
    public void datadupIn(String vnode, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Init State, There should not any get operation yet");
        return;
    }

    public void start() {
        LOG.debug("Init State Start...");

        Configuration conf = sed.getConf();

        if (sed.getServer() != null){
            sed.getServer().close();
        }

        sed.setLocalStorage(new LocalMemCached(conf));
        sed.setServer(new SednaServer(conf, sed));
        sed.getServer().start();
        
        LOG.debug("Init State Finish...");
        sed.setState(sed.getRegState());
    }

}
