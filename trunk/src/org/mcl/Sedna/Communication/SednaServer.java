/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Communication;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.StateMachine.Sedna;
import org.xsocket.connection.IServer;
import org.xsocket.connection.Server;

/**
 * Each Sedna instance needs a SednaServer running up while system is starting.
 * Typically, we call this server a coordinator. Its responsibility includes several parts:
 * 1, Receive Library's get/set requests and generate appropriate result according state machine
 * 2, Request to other nodes for read/write operation.
 * 3, Receive other nodes' requests and reply
 *
 * @author daidong
 */
public class SednaServer {

    private static Logger LOG;

    static {
        LOG = Logger.getLogger(SednaServer.class);
    }
    
    private Configuration conf = null;
    private IServer localSrv = null;
    private Sedna sed = null;


    public SednaServer(Configuration conf, Sedna instance){
        this.conf = conf;
        this.sed = instance;

        String sp = conf.getValue("tcp_server_port");
        if ("".equalsIgnoreCase(sp))
            sp = "11212";
        int serverPort = Integer.parseInt(sp);

        try {
            localSrv = new Server(serverPort, new SednaHandler(conf, instance));
        } catch (UnknownHostException ex) {
            LOG.error("Senda Server Error: " + "UnknownHostException");
        } catch (IOException ex) {
            LOG.error("Senda Server Error: " + "IOException");
        }
        
        sp = conf.getValue("tcp_idle_timeout");
        if ("".equalsIgnoreCase(sp))
            sp = "60000";
        long idleTimeOut = Long.parseLong(sp);
        localSrv.setIdleTimeoutMillis(idleTimeOut);

        
    }
    
    public void start(){
        try {
            localSrv.start();
            LOG.debug("localSrv start");
        } catch (IOException ex) {
            LOG.error("Senda Server Start Error: " + "IOException");
        }
    }

    public void close(){
        try {
            localSrv.close();
        } catch (IOException ex) {
            LOG.error("Senda Server Close Error: " + "IOException");
        }
    }
}
