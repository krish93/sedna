/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Tasks;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Communication.BlockSender;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;

/**
 *
 * @author daidong
 */
public class DataDupMaintainTask extends Thread{
    
    private String vnode = null;
    private Sedna sed = null;
    private ZooKeeperService zks = null;
    
    
    public DataDupMaintainTask(Sedna sed, String vnode){
        this.sed = sed;
        this.vnode = vnode;
        this.zks = sed.getZooKeeperService();
    }
    
    public void run(){
        Cluster cluster = sed.getCluster();
        String myName = sed.getMyRealName();

        String rs = zks.sync(ZooKeeperService.vnodeDir + "/" + vnode);
        String[] rnodes = rs.split(",");

        if (rnodes.length == 3) {
            return;
        }

        int size = rnodes.length;

        zks.updateRealNodes();
        String rnode = cluster.getRnode(rnodes);
        if (rnode == null)
            return;
        if (myName.equals(rnode))
            return;
        
        String raddr = rnode.split(":")[0];
        int rp = Integer.parseInt(rnode.split(":")[1]);

        BlockSender bs = new BlockSender(raddr, rp);
        bs.send(SednaProtocol.formCommand("dupin", vnode));

        if (SednaProtocol.deCompReply(bs.getConnection()).equals("OK")) 
            return;

    }
}
