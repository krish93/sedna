/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.ZooKeeper;

/**
 *
 * @author daidong
 */
public class ChangeVNode implements Comparable<ChangeVNode>{
    
    public String vnode;
    public long time;
    
    public ChangeVNode(String v, long t){
        vnode = v;
        time = t;
    }
    
    public String toString(){
        return vnode+"-"+time;
    }
    
    public boolean equals(Object o){
        if (this == o)
            return true;
        ChangeVNode comp = (ChangeVNode) o;
        if (this.vnode == comp.vnode && this.time == comp.time)
            return true;
        return false;
    }

    public int compareTo(ChangeVNode t) {
        if (this.time < t.time)
            return -1;
        if (this.time == t.time)
            return 0;
        return 1;
    }
}
