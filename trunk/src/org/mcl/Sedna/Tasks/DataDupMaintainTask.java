/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Tasks;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Random;
import org.apache.log4j.Logger;
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
    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(DataDupMaintainTask.class);
    }
    
    private String vnode = null;
    private Sedna sed = null;
    private ZooKeeperService zks = null;
    private String lockDup = "dup-" + vnode;
    
    
    public DataDupMaintainTask(Sedna sed, String vnode){
        this.sed = sed;
        this.vnode = vnode;
        this.zks = sed.getZooKeeperService();
    }
    
    public void run(){
        
        Cluster cluster = sed.getCluster();
        ZooKeeperService zks = sed.getZooKeeperService();
        String myName = sed.getMyRealName();
        
        if (cluster.getVnodeItem(vnode).contains(myName)){
            return;
        }
        
        try {
            
            zks.lock(vnode);

            zks.syncVnode(vnode);

            String[] rnodes = cluster.getVnodeItems(vnode);
            
            if (rnodes.length == 3) {
                zks.unlock(vnode);
                return;
            }
            
            String rnwc = cluster.getVnodeItem(vnode);
            if (rnwc.contains(myName)){
                zks.unlock(vnode);
                return;
            } else {
                if (!rnwc.equals("")){
                    rnwc = rnwc + "," + myName;
                } else {
                    rnwc = myName;
                }
            }
            zks.setVNodeValue(vnode, rnwc);
            cluster.addVNodeForMe(vnode);
            zks.unlock(vnode);

            Random rand = new Random(System.currentTimeMillis());
            int i = Math.abs(rand.nextInt()) % rnodes.length;
            String rnode = rnodes[i];
            if (rnode.split(":").length != 2)
                return;
            
            String ip = rnode.split(":")[0];
            int port = Integer.parseInt(rnode.split(":")[1]);
            
            BlockSender bs = new BlockSender(ip, port);
            bs.send(SednaProtocol.formCommand("dupin", vnode));

            if (SednaProtocol.deCompReply(bs.getConnection()).equals("ok"))
                return;
            
        } catch (KeeperException ex) {
            LOG.error("data move in error KeeperException");
        } catch (InterruptedException ex) {
            LOG.error("data move in error InterruptedException");
        } catch (BufferOverflowException ex) {
            LOG.error("data move in error KeeperException");
        } finally {
            //zks.unlock(vnode);
        }
    }
    public void runOld() {
        
        if (!zks.dup_thread_start(vnode))
            return;
        
        Cluster cluster = sed.getCluster();

        boolean needFix = true;
        
        String[] rnodes = cluster.getVnodeItems(vnode);
        
        int times = (3 - rnodes.length);
        
        while(needFix && times-- > 0){
            
            zks.syncVnode(vnode);
            rnodes = cluster.getVnodeItems(vnode);

            if (rnodes.length == 3) {
                needFix = false;
                continue;
            }

            zks.updateRealNodes();
            String rnode = cluster.getRnode(rnodes);
            
            if (rnode == null){
                needFix = false;
                continue;
            }
            
            String raddr = rnode.split(":")[0];
            int rp = Integer.parseInt(rnode.split(":")[1]);
            
            BlockSender bs = new BlockSender(raddr, rp);
            bs.send(SednaProtocol.formCommand("dupin", vnode));
            
            String reply = SednaProtocol.deCompReply(bs.getConnection());
        }
        zks.dup_thread_stop(vnode);
    }
}
