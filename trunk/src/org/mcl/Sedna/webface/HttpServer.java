/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mcl.Sedna.webface;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.mcl.Sedna.Configuration.Configuration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

/**
 *
 * @author daidong
 * Copy from Hadoop Project
 */
public class HttpServer{

    private static final Logger LOG;

    static {
        LOG = Logger.getLogger(HttpServer.class);
    }
    
    protected final Server webServer;
    protected final Connector listener;
    protected final WebAppContext webAppContext;
    protected final Map<Context, Boolean> defaultContexts =
            new HashMap<Context, Boolean>();
    protected final List<String> filterNames = new ArrayList<String>();
    private static final int MAX_RETRIES = 10;
    private Configuration conf = null;

    /**
     * Create a status server on the given port.
     * The jsp scripts are taken from src/webapps/<name>.
     * @param name The name of the server
     * @param port The port to use on the server
     * @param findPort whether the server should start at the given port and 
     *        increment by 1 until it finds a free port.
     * @param conf Configuration 
     */
    public HttpServer(String name, String bindAddress, int port, Configuration conf) 
            throws IOException {
        this.conf = conf;
        webServer = new Server();

        listener = createBaseListener(conf);
        listener.setHost(bindAddress);
        listener.setPort(port);
        webServer.addConnector(listener);

        webServer.setThreadPool(new QueuedThreadPool());

        final String appDir = getWebAppsPath();
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        webServer.setHandler(contexts);
        
        webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(appDir + "/" + name);
        webServer.addHandler(webAppContext);

        addDefaultApps(contexts, appDir);
    }

    /**
     * Create a required listener for the Jetty instance listening on the port
     * provided. This wrapper and all subclasses must create at least one
     * listener.
     */
    protected Connector createBaseListener(Configuration conf)
            throws IOException {
        SelectChannelConnector ret = new SelectChannelConnector();
        ret.setLowResourceMaxIdleTime(10000);
        ret.setAcceptQueueSize(128);
        ret.setResolveNames(false);
        ret.setUseDirectBuffers(false);
        return ret;
    }

    /**
     * Add default apps.
     * @param appDir The application directory
     * @throws IOException
     */
    protected void addDefaultApps(ContextHandlerCollection parent,
            final String appDir) throws IOException {
        String logDir = conf.getValue("sedna_log_dir");
        if (logDir != null) {
            Context logContext = new Context(parent, "/logs");
            logContext.setResourceBase(logDir);
            logContext.addServlet(DefaultServlet.class, "/");
            defaultContexts.put(logContext, true);
        }
        // set up the context for "/static/*"
        Context staticContext = new Context(parent, "/static");
        staticContext.setResourceBase(appDir + "/static");
        staticContext.addServlet(DefaultServlet.class, "/*");
        defaultContexts.put(staticContext, true);
    }

    public void addContext(Context ctxt, boolean isFiltered)
            throws IOException {
        webServer.addHandler(ctxt);
        defaultContexts.put(ctxt, isFiltered);
    }

    /**
     * Add a context 
     * @param pathSpec The path spec for the context
     * @param dir The directory containing the context
     * @param isFiltered if true, the servlet is added to the filter path mapping 
     * @throws IOException
     */
    protected void addContext(String pathSpec, String dir, boolean isFiltered) throws IOException {
        if (0 == webServer.getHandlers().length) {
            throw new RuntimeException("Couldn't find handler");
        }
        WebAppContext webAppCtx = new WebAppContext();
        webAppCtx.setContextPath(pathSpec);
        webAppCtx.setWar(dir);
        addContext(webAppCtx, true);
    }

    /**
     * Set a value in the webapp context. These values are available to the jsp
     * pages as "application.getAttribute(name)".
     * @param name The name of the attribute
     * @param value The value of the attribute
     */
    public void setAttribute(String name, Object value) {
        webAppContext.setAttribute(name, value);
    }

    /**
     * Add a servlet in the server.
     * @param name The name of the servlet (can be passed as null)
     * @param pathSpec The path spec for the servlet
     * @param clazz The servlet class
     */
    public void addServlet(String name, String pathSpec,
            Class<? extends HttpServlet> clazz) {
        addInternalServlet(name, pathSpec, clazz);
        //addFilterPathMapping(pathSpec, webAppContext);
    }

    /**
     * Add an internal servlet in the server.
     * @param name The name of the servlet (can be passed as null)
     * @param pathSpec The path spec for the servlet
     * @param clazz The servlet class
     * @deprecated this is a temporary method
     */
    @Deprecated
    public void addInternalServlet(String name, String pathSpec,
            Class<? extends HttpServlet> clazz) {
        ServletHolder holder = new ServletHolder(clazz);
        if (name != null) {
            holder.setName(name);
        }
        webAppContext.addServlet(holder, pathSpec);
    }
    /**
     * Get the value in the webapp context.
     * @param name The name of the attribute
     * @return The value of the attribute
     */
    public Object getAttribute(String name) {
        return webAppContext.getAttribute(name);
    }

    /**
     * Get the pathname to the webapps files.
     * @return the pathname as a URL
     * @throws IOException if 'webapps' directory cannot be found on CLASSPATH.
     */
    protected String getWebAppsPath() throws IOException {
        URL url = getClass().getClassLoader().getResource("webapps");
        if (url == null) {
            throw new IOException("webapps not found in CLASSPATH");
        }
        return url.toString();
    }

    /**
     * Get the port that the server is on
     * @return the port
     */
    public int getPort() {
        return webServer.getConnectors()[0].getLocalPort();
    }

    /**
     * Set the min, max number of worker threads (simultaneous connections).
     */
    public void setThreads(int min, int max) {
        QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
        pool.setMinThreads(min);
        pool.setMaxThreads(max);
    }

    /**
     * Start the server. Does not wait for the server to start.
     */
    public void start() throws IOException {
        try {
            int port = 0;
            int oriPort = listener.getPort(); // The original requested port
            while (true) {
                try {
                    port = webServer.getConnectors()[0].getLocalPort();
                    LOG.info("Port returned by webServer.getConnectors()[0]."
                            + "getLocalPort() before open() is " + port
                            + ". Opening the listener on " + oriPort);
                    listener.open();
                    port = listener.getLocalPort();
                    LOG.info("listener.getLocalPort() returned " + listener.getLocalPort()
                            + " webServer.getConnectors()[0].getLocalPort() returned "
                            + webServer.getConnectors()[0].getLocalPort());
                    //Workaround to handle the problem reported in HADOOP-4744
                    if (port < 0) {
                        Thread.sleep(100);
                        int numRetries = 1;
                        while (port < 0) {
                            LOG.warn("listener.getLocalPort returned " + port);
                            if (numRetries++ > MAX_RETRIES) {
                                throw new Exception(" listener.getLocalPort is returning "
                                        + "less than 0 even after " + numRetries + " resets");
                            }
                            for (int i = 0; i < 2; i++) {
                                LOG.info("Retrying listener.getLocalPort()");
                                port = listener.getLocalPort();
                                if (port > 0) {
                                    break;
                                }
                                Thread.sleep(200);
                            }
                            if (port > 0) {
                                break;
                            }
                            LOG.info("Bouncing the listener");
                            listener.close();
                            Thread.sleep(1000);
                            listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
                            listener.open();
                            Thread.sleep(100);
                            port = listener.getLocalPort();
                        }
                    } //Workaround end
                    LOG.info("Jetty bound to port " + port);
                    webServer.start();
                    // Workaround for HADOOP-6386
                    port = listener.getLocalPort();
                    if (port < 0) {
                        LOG.warn("Bounds port is " + port + " after webserver start");
                        for (int i = 0; i < MAX_RETRIES / 2; i++) {
                            try {
                                webServer.stop();
                            } catch (Exception e) {
                                LOG.warn("Can't stop  web-server", e);
                            }
                            Thread.sleep(1000);

                            listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
                            listener.open();
                            Thread.sleep(100);
                            webServer.start();
                            LOG.info(i + "attempts to restart webserver");
                            port = listener.getLocalPort();
                            if (port > 0) {
                                break;
                            }
                        }
                        if (port < 0) {
                            throw new Exception("listener.getLocalPort() is returning "
                                    + "less than 0 even after " + MAX_RETRIES + " resets");
                        }
                    }
                    // End of HADOOP-6386 workaround
                    break;
                } catch (IOException ex) {
                    // if this is a bind exception,
                    // then try the next port number.
                    if (ex instanceof BindException) {
                        if (true) {
                            throw (BindException) ex;
                        }
                    } else {
                        LOG.info("HttpServer.start() threw a non Bind IOException");
                        throw ex;
                    }
                } catch (MultiException ex) {
                    LOG.info("HttpServer.start() threw a MultiException");
                    throw ex;
                }
                listener.setPort((oriPort += 1));
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Problem starting http server", e);
        }
    }

    /**
     * stop the server
     */
    public void stop() throws Exception {
        listener.close();
        webServer.stop();
    }

    public void join() throws InterruptedException {
        webServer.join();
    }

}
