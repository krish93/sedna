/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.webface;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.StateMachine.Sedna;

/**
 *
 * @author daidong
 */
public class VnodesList extends HttpServlet {

    private Sedna sed = null;

    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        response.setContentType("text/html");
        response.setHeader("Cache-Control", "private");
        response.setHeader("Pragma", "no-cache");

        PrintWriter writer = response.getWriter();
        sed = (Sedna) this.getServletContext().getAttribute("this.sed");
        Cluster cluster = sed.getCluster();
        CopyOnWriteArrayList<String> vnodes = cluster.vnodeStored();
        String html = "<html><head>"+vnodes.size()+"</head><body>";
        for (String vnode:vnodes){
            html += ("<p>" + vnode + "</p>");
        }
        html += "</html></body>";
        
        writer.print(html);
        writer.flush();
    }

    public void doPost(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        doGet(request, response);
    }
}
