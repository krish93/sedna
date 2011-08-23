/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Tasks;

import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.Communication.BlockSender;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.Protocol.SednaProtocol;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;

/**
 *
 * @author daidong
 */
public class DataMoveInTask extends Thread {

    private static Logger LOG;

    static {
        LOG = Logger.getLogger(DataMoveInTask.class);
    }
    private Cluster cluster = null;
    private Configuration conf = null;
    private ZooKeeperService zks = null;
    private Sedna sed = null;

    public DataMoveInTask(Sedna sed) {
        this.sed = sed;
        this.cluster = sed.getCluster();
        this.conf = sed.getConf();
        this.zks = sed.getZooKeeperService();
    }

    /*
     * @TODO: Modify this code from using Lock to using just zks.mov_thread_start.
     */
    @Override
    public void run() {
        
        LOG.debug("DataMoveInTask starts");
        
        Random rand = new Random(System.currentTimeMillis());
        int DataMoveInThreadInterval = Integer.parseInt(conf.getValue("data_movein_thread_interval"));
        String vnode = "";
        
        LOG.debug("DataMoveInTask Stage 1" + this);
        
        while (true) {
            LOG.debug("DataMoveInTask Stage 2" + this);
            
            zks.updateRealNodes();
            int realNodeNums = cluster.getRealNodeNum();
            realNodeNums = (realNodeNums == 0)?1:realNodeNums;
            if (cluster.vnodeNumStored() < cluster.getVirtNodeNum() / realNodeNums * 0.8) {

                try {

                    boolean exists = true;
                    boolean datamove = false;
                    boolean datadup = false;

                    int vnodeInt = -1;
                    int retried = 0;
                    LOG.debug("DataMoveInTask Stage 2.5 " + this);
                    while (exists && (retried++ < 5)) {

                        vnodeInt = Math.abs(rand.nextInt() % cluster.getVirtNodeNum());
                        String vnodeStr = String.format("%8d", vnodeInt);
                        exists = cluster.vnodeStored().contains(vnodeStr);
                        
                    }
                    if (exists){
                        LOG.debug("DataMoveInTask, still can not random a correct node, return");
                        return;
                    }
                    vnode = String.format("%08d", vnodeInt);
                    
                    zks.lock(vnode);
                    
                    zks.syncVnode(vnode);

                    String[] rnodes = cluster.getVnodeItems(vnode);
                    
                    String rnode = null;
                    String localName = sed.getMyRealName();

                    if (rnodes == null || rnodes.length == 0) {
                        //this means this vnode hasn't been assigned to any real node
                        //just assign and return is fine, do not need data move
                        zks.setVNodeValue(vnode, localName);
                        cluster.addVNodeForMe(vnode);
                        datamove = false;
                        datadup = false;

                    } else if (rnodes.length == 1) {
                        //this means this vnode have been assigned to one real node
                        //just assign to this new node is fine, if two real nodes
                        //are equal, just return. data movement is needed.
                        if ( rnodes[0].equals("") || rnodes[0].equals(" ")){
                            zks.setVNodeValue(vnode, localName);
                            cluster.addVNodeForMe(vnode);
                            datamove = false;
                            datadup = false;
                        } else if (rnodes[0].equals(localName)) {
                            LOG.debug("DataMoveInTask Stage 3(2)...rnodes length = 1, and equal to localname" + this);
                            datamove = false;
                            datadup = false;
                        } else {
                            LOG.debug("DataMoveInTask Stage 3(3)...rnodes length = 1, add new real node" + this);
                            String newRealNodes = rnodes[0] + "," + localName;
                            zks.setVNodeValue(vnode, newRealNodes);
                            cluster.addVNodeForMe(vnode);
                            rnode = rnodes[0];
                            datamove = false;
                            datadup = true;
                        }

                    } else if (rnodes.length == 2) {
                        //this means this vnode has been assigned to two real nodes
                        //just assign to this new node is fine, if two real nodes
                        //are equal, just return. data movement is needed.
                        if (rnodes[0].equals(localName) || rnodes[1].equals(localName)) {
                            datamove = false;
                            datadup = false;
                        } else {
                            String newRealNodes = rnodes[0] + "," + rnodes[1] + "," + localName;
                            zks.setVNodeValue(vnode, newRealNodes);
                            cluster.addVNodeForMe(vnode);
                            int index = Math.abs(rand.nextInt()) % 2;
                            rnode = rnodes[index];
                            datamove = false;
                            datadup = true;
                        }
                    } else if (rnodes.length == 3) {
                        //this means this vnode has been assigned to three real nodes
                        //need replace one real node, before doing this, if two real nodes
                        //are equal, just return. data movement is needed.
                        if (rnodes[0].equals(localName) || rnodes[1].equals(localName)
                                || rnodes[2].equals(localName)) {

                            datamove = false;
                            datadup = false;
                        } else {
                            /*
                            String newRealNodes = rnodes[0] + "," + rnodes[1]
                                    + "," + rnodes[2] + "," + localName;
                            zks.setVNodeValue(vnode, newRealNodes);
                            int index = Math.abs(rand.nextInt()) % 3;
                            rnode = rnodes[index];
                            */
                            datamove = false;
                            datadup = false;
                        }
                    }
                    /*
                     * In current design, datamove never happens in DataMoveInTask
                     *
                    if (datamove) {
                        LOG.debug("DataMoveInTask, need datamove...This should not happen in stand alone mode");
                        String ip = rnode.split(":")[0];
                        int port = Integer.parseInt(rnode.split(":")[1]);
                        BlockSender bs = new BlockSender(ip, port);
                        bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "0"));

                        int retries = 2;

                        while (!SednaProtocol.deCompReply(bs.getConnection()).equals("ok")
                                && --retries > 0) {
                            bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "0"));
                        }

                        bs.close();

                        if (retries > 0) {
                            //Data Transfor Request, BLOCKING EXECUTING
                        }
                    } else*/
                    if (datadup) {
                        String ip_addr = rnode.split(":")[0];
                        int ip_port = Integer.parseInt(rnode.split(":")[1]);
                        BlockSender bs = new BlockSender(ip_addr, ip_port);
                        bs.send(SednaProtocol.formCommand("dupin", vnode));
                        
                        SednaProtocol.deCompReply(bs.getConnection());
                        /*
                        sed.getLocalStorage().duplicate(vnode, rnode);
                        LOG.error("DataMoveInTask Duplicate Command Execute");
                        */
                        /*
                        String ip = rnode.split(":")[0];
                        int port = Integer.parseInt(rnode.split(":")[1]);
                        BlockSender bs = new BlockSender(ip, port);
                        
                        bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "1"));

                        int retries = 2;

                        while (!SednaProtocol.deCompReply(bs.getConnection()).equals("ok")
                                && --retries > 0) {
                            bs.send(SednaProtocol.formCommand("moveout", vnode, sed.getMyRealName(), "1"));
                        }

                        bs.close();

                        if (retries > 0) {
                            //Data Transfor Request, BLOCKING EXECUTING
                        }
                        */
                    }
                    //cluster.addVNodeForMe(vnode);
                    zks.unlock(vnode);
                    try {
                        sleep(DataMoveInThreadInterval);
                    } catch (Exception ex) {
                        LOG.error("Data Move In Task Error " + "Exception");
                    }

                } catch (KeeperException ex) {
                    LOG.error("Data Move In Task Error KeeperException: " + vnode);
                } catch (InterruptedException ex) {
                    LOG.error("Data Move In Task Error InterruptedException");
                }

            } else {
                try {
                    sleep(DataMoveInThreadInterval);
                } catch (Exception ex) {
                    LOG.error("Data Move In Task Error " + "Exception");
                }
            }
        }
    }
}
