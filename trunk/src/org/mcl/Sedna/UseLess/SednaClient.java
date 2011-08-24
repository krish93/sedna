/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.UseLess;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mcl.Sedna.Configuration.Configuration;
import org.xsocket.connection.IHandler;
import org.xsocket.connection.INonBlockingConnection;
import org.xsocket.connection.NonBlockingConnection;

/**
 *
 * @author daidong
 */
public class SednaClient {

    private Configuration conf = null;
    private String host;
    private int port;
    private INonBlockingConnection nbc = null;
    private IHandler handler = null;


    public SednaClient(Configuration conf){
        this.conf = conf;
    }
    public SednaClient(String host, int port, Configuration conf){
        try {
            this.host = host;
            this.port = port;
            this.conf = conf;
            this.handler = (IHandler) new SednaClient(conf);
            nbc = new NonBlockingConnection(this.host, this.port, this.handler);

        } catch (IOException ex) {
            Logger.getLogger(SednaClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void write(){

    }

}
