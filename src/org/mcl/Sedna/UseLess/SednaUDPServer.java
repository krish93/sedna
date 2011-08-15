/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.UseLess;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mcl.Sedna.Configuration.Configuration;
import org.xsocket.datagram.Endpoint;
import org.xsocket.datagram.IEndpoint;
import org.xsocket.datagram.UserDatagram;

/**
 *
 * @author daidong
 */
public class SednaUDPServer {


    private static final int PACKET_SIZE = 8096;
    
    private Configuration conf = null;
    private IEndpoint UDPServer = null;

    public SednaUDPServer(Configuration conf){
        try {
            this.conf = conf;
            String sp = conf.getValue("udp_server_port");
            if ("".equalsIgnoreCase(sp)) {
                sp = "11213";
            }
            int serverPort = Integer.parseInt(sp);
            InetAddress ia = new InetSocketAddress("127.0.0.1", serverPort).getAddress();
            UDPServer = new Endpoint(PACKET_SIZE, null, ia, serverPort);

        } catch (IOException ex) {
            Logger.getLogger(SednaUDPServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String[] args){
        try {
            InetSocketAddress isa = new InetSocketAddress("127.0.0.1", 11212);
            InetAddress ia = isa.getAddress();
            IEndpoint endpoint = new Endpoint(PACKET_SIZE, null, ia, 11212);
            UserDatagram waitFor = endpoint.receive(10000);
            System.out.println("waitFor: "+waitFor.readString());

            UserDatagram reply = new UserDatagram(waitFor.getRemoteSocketAddress(), PACKET_SIZE);
            reply.write("hello, dude");
            endpoint.send(reply);

            endpoint.close();

        } catch (IOException ex) {
            Logger.getLogger(SednaUDPServer.class.getName()).log(Level.SEVERE, null, ex);
        }


    }
}
