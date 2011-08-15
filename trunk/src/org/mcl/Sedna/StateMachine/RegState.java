/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.StateMachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import org.apache.log4j.Logger;
import org.mcl.Sedna.CHash.RSHash;
import org.mcl.Sedna.CHash.SimplestHash;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.Tasks.RemoveIChangeVNodeTask;
import org.mcl.Sedna.Tasks.UpdateLocalVNodeTable;
import org.xsocket.connection.INonBlockingConnection;

/**
 *
 * @author daidong
 */
public class RegState implements SednaState{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(InitState.class);
    }
    
    private Sedna sed = null;

    public RegState(Sedna s) {
        this.sed = s;
    }

    public boolean get(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any get operation yet");
        return true;
    }

    public boolean set(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any set operation yet");
        return true;
    }

    public Object cget(String Key, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any get operation yet");
        return true;
    }

    public boolean cset(String Key, String Value, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any set operation yet");
        return true;
    }


    public void dataMoveOut(String rnode, String vnode, int type, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any get operation yet");
        return;
    }

    public void dataMoveIn(String vnode, String rnodeSub, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any get operation yet");
        return;
    }
    
    public void datadupIn(String vnode, INonBlockingConnection conn) throws IOException, BufferOverflowException{
        LOG.error("In Reg State, There should not any get operation yet");
        return;
    }

    public void start() {
        LOG.debug("Reg State Start...");

        //sed.setCHash(new SimplestHash(sed.getCluster().getVirtNodeNum()));
        sed.setCHash(new RSHash(sed.getCluster().getVirtNodeNum()));
        
        LOG.debug("ZooKeeper Init Start...");
        
        sed.getZooKeeperService().debugInitNodes();
        sed.getZooKeeperService().initVNodes();
        sed.getZooKeeperService().waitVNodes();
        
        LOG.debug("ZooKeeper Init Finish...");
        
        UpdateLocalVNodeTable ulvt = new UpdateLocalVNodeTable(sed);
        ulvt.start();
        
        RemoveIChangeVNodeTask ricvt = new RemoveIChangeVNodeTask(sed);
        ricvt.start();
        
        LOG.debug("Reg State Finish...");
        sed.setState(sed.getReadyState());
    }

}
