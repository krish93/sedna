/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.webface;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mcl.Sedna.Cluster.Cluster;
import org.mcl.Sedna.LocalStorage.LocalStorage;
import org.mcl.Sedna.StateMachine.Sedna;

/**
 *
 * @author daidong
 */
public class DataList extends HttpServlet{
    
    private Sedna sed = null;
    
    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException{

        response.setContentType("text/html");
        response.setHeader("Cache-Control", "private");
        response.setHeader("Pragma", "no-cache");

        sed = (Sedna) this.getServletContext().getAttribute("this.sed");
        Cluster cluster = sed.getCluster();
        LocalStorage ls = sed.getLocalStorage();
        String output = ls.stats();
        
        PrintWriter writer = response.getWriter();
        writer.print("<HTML><head>Data List Page</head><BODY><P>"+output+"</P></BODY></HTML>");
        writer.flush();
        
    }
     
     public void doPost(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException{
         
         doGet(request, response);
     }
}
