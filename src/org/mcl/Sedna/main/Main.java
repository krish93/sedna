/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.main;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mcl.Sedna.StateMachine.Sedna;

/**
 *
 * @author daidong
 */
public class Main {

    private static Logger LOG;

    static {
        LOG = Logger.getLogger(Main.class);
    }
    
    public static void main(String[] args){
        PropertyConfigurator.configure("conf/log4j.properties");
        LOG.error("Sedna Start Begin at: " + System.currentTimeMillis());
        Sedna instance = new Sedna();
        instance.start();
    }
}
