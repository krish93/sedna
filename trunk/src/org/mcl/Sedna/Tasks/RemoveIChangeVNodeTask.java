/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.Tasks;

import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Configuration.Configuration;
import org.mcl.Sedna.StateMachine.Sedna;
import org.mcl.Sedna.ZooKeeper.ZooKeeperService;

/**
 *
 * @author daidong
 */
public class RemoveIChangeVNodeTask extends Thread{
    
    private static Logger LOG;

    static{
        LOG = Logger.getLogger(RemoveIChangeVNodeTask.class);
    }

    private ZooKeeperService zks = null;
    private long interval = 0;
    
    public RemoveIChangeVNodeTask(Sedna sed){
        this.zks = sed.getZooKeeperService();
        Configuration conf = sed.getConf();
        interval = Long.parseLong(conf.getValue("remove_i_change_vnode_interval"));
    }

    /**
     * 每隔interval的时间执行一次，执行过程将所有于现在时间间隔超过interval的老node删除掉
     */
    @Override
    public void run(){
        
        do {
            zks.removeIChangeVNodes(interval);
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ex) {
                LOG.error("RemoveIChangeVNodeTask Sleep Exception");
            }
        } while (true);
    }
}
