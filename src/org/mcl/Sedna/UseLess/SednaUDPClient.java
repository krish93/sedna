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
import org.xsocket.datagram.Endpoint;
import org.xsocket.datagram.IEndpoint;
import org.xsocket.datagram.UserDatagram;

/**
 *
 * @author daidong
 */
public class SednaUDPClient {

    private static int PACKET_SIZE = 8096;

    public static void main(String[] args){
        try {
            InetSocketAddress isa = new InetSocketAddress("127.0.0.1", 11213);
            InetAddress ia = isa.getAddress();
            IEndpoint endpoint = new Endpoint(PACKET_SIZE, null, ia, 11213);
            
            UserDatagram send = new UserDatagram(ia, 11212, PACKET_SIZE);
            send.write("hello, dude server");
            endpoint.send(send);


            UserDatagram waitFor = endpoint.receive(10000);
            System.out.println("Response: "+waitFor.readString());

            endpoint.close();

        } catch (IOException ex) {
            Logger.getLogger(SednaUDPServer.class.getName()).log(Level.SEVERE, null, ex);
        }


    }
}
