/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Tasks;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;

/**
 *
 * @author daidong
 */
public class UpdateLocalVNodeTable extends Thread{

    private static Logger LOG;

    static{
        LOG = Logger.getLogger(RemoveIChangeVNodeTask.class);
    }
    private Sedna sed = null;
    private ZooKeeperService zks = null;

    public UpdateLocalVNodeTable(Sedna sed){
        this.sed = sed;
        zks = sed.getZooKeeperService();
    }
    
    /**
     * 更新的设计比较简单。
     * 更新所有的更改的节点即可。
     * 更新的频率通过leaseTime来控制。由于节点的加入不会进行播，因此并不知新节点加入
     * 所以leaseTime可以作废了。
     */
    @Override
    public void run(){
        while (true){
            try {
                zks.syncAllRNode();
                zks.syncAllChangeVNode();
                sleep(sed.getLeaseTime());
            } catch (Exception ex){
               LOG.error("UpdateLocalVNodeTable Exception"); 
            }
        }
        
        
    }
}
