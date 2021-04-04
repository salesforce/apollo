/*
 * Copyright (c) 1996, 2018, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package sandbox.java.sql;

import java.util.Enumeration;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import sandbox.java.lang.Object;
import sandbox.java.lang.String;

/**
 * The basic service for managing a set of JDBC drivers.
 * <p>
 * <strong>NOTE:</strong> The {@link javax.sql.DataSource} interface, provides
 * another way to connect to a data source. The use of a {@code DataSource}
 * object is the preferred means of connecting to a data source.
 * <P>
 * As part of its initialization, the {@code DriverManager} class will attempt
 * to load available JDBC drivers by using:
 * <ul>
 * <li>The {@code jdbc.drivers} system property which contains a colon separated
 * list of fully qualified class names of JDBC drivers. Each driver is loaded
 * using the {@linkplain ClassLoader#getSystemClassLoader system class loader}:
 * <ul>
 * <li>{@code jdbc.drivers=foo.bah.Driver:wombat.sql.Driver:bad.taste.ourDriver}
 * </ul>
 *
 * <li>Service providers of the {@code sandbox.java.sql.Driver} class, that are
 * loaded via the {@linkplain ServiceLoader#load service-provider loading}
 * mechanism.
 * </ul>
 *
 * @implNote {@code DriverManager} initialization is done lazily and looks up
 *           service providers using the thread context class loader. The
 *           drivers loaded and available to an application will depend on the
 *           thread context class loader of the thread that triggers driver
 *           initialization by {@code DriverManager}.
 *
 *           <P>
 *           When the method {@code getConnection} is called, the
 *           {@code DriverManager} will attempt to locate a suitable driver from
 *           amongst those loaded at initialization and those loaded explicitly
 *           using the same class loader as the current application.
 *
 * @see Driver
 * @see Connection
 * @since 1.1
 */
public class DriverManager {

    private static volatile int                 loginTimeout = 0;
    private static volatile java.io.PrintWriter logWriter    = null;
    private static volatile java.io.PrintStream logStream    = null;
    // Used in println() to synchronize logWriter
    private final static Object logSync = new Object();

    /* Prevent the DriverManager class from being instantiated. */
    private DriverManager() {
    }

    // --------------------------JDBC 2.0-----------------------------

    /**
     * Retrieves the log writer.
     *
     * The <code>getLogWriter</code> and <code>setLogWriter</code> methods should be
     * used instead of the <code>get/setlogStream</code> methods, which are
     * deprecated.
     * 
     * @return a <code>java.io.PrintWriter</code> object
     * @see #setLogWriter
     * @since 1.2
     */
    public static java.io.PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * Sets the logging/tracing <code>PrintWriter</code> object that is used by the
     * <code>DriverManager</code> and all drivers.
     * <P>
     * If a security manager exists, its {@code checkPermission} method is first
     * called with a {@code SQLPermission("setLog")} permission to check that the
     * caller is allowed to call {@code setLogWriter}.
     *
     * @param out the new logging/tracing <code>PrintStream</code> object;
     *            <code>null</code> to disable logging and tracing
     * @throws SecurityException if a security manager exists and its
     *                           {@code checkPermission} method denies permission to
     *                           set the log writer.
     * @see SecurityManager#checkPermission
     * @see #getLogWriter
     * @since 1.2
     */
    public static void setLogWriter(java.io.PrintWriter out) {
        logStream = null;
        logWriter = out;
    }

    // ---------------------------------------------------------------

    /**
     * Attempts to establish a connection to the given database URL. The
     * <code>DriverManager</code> attempts to select an appropriate driver from the
     * set of registered JDBC drivers.
     * <p>
     * <B>Note:</B> If a property is specified as part of the {@code url} and is
     * also specified in the {@code Properties} object, it is implementation-defined
     * as to which value will take precedence. For maximum portability, an
     * application should only specify a property once.
     *
     * @param url  a database url of the form
     *             <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param info a list of arbitrary string tag/value pairs as connection
     *             arguments; normally at least a "user" and "password" property
     *             should be included
     * @return a Connection to the URL
     * @exception SQLException if a database access error occurs or the url is
     *                         {@code null}
     * @throws SQLTimeoutException when the driver has determined that the timeout
     *                             value specified by the {@code setLoginTimeout}
     *                             method has been exceeded and has at least tried
     *                             to cancel the current database connection attempt
     */
    public static Connection getConnection(String url, java.util.Properties info) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Attempts to establish a connection to the given database URL. The
     * <code>DriverManager</code> attempts to select an appropriate driver from the
     * set of registered JDBC drivers.
     * <p>
     * <B>Note:</B> If the {@code user} or {@code password} property are also
     * specified as part of the {@code url}, it is implementation-defined as to
     * which value will take precedence. For maximum portability, an application
     * should only specify a property once.
     *
     * @param url      a database url of the form
     *                 <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param user     the database user on whose behalf the connection is being
     *                 made
     * @param password the user's password
     * @return a connection to the URL
     * @exception SQLException if a database access error occurs or the url is
     *                         {@code null}
     * @throws SQLTimeoutException when the driver has determined that the timeout
     *                             value specified by the {@code setLoginTimeout}
     *                             method has been exceeded and has at least tried
     *                             to cancel the current database connection attempt
     */
    public static Connection getConnection(String url, String user, String password) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Attempts to establish a connection to the given database URL. The
     * <code>DriverManager</code> attempts to select an appropriate driver from the
     * set of registered JDBC drivers.
     *
     * @param url a database url of the form
     *            <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @return a connection to the URL
     * @exception SQLException if a database access error occurs or the url is
     *                         {@code null}
     * @throws SQLTimeoutException when the driver has determined that the timeout
     *                             value specified by the {@code setLoginTimeout}
     *                             method has been exceeded and has at least tried
     *                             to cancel the current database connection attempt
     */
    public static Connection getConnection(String url) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Attempts to locate a driver that understands the given URL. The
     * <code>DriverManager</code> attempts to select an appropriate driver from the
     * set of registered JDBC drivers.
     *
     * @param url a database URL of the form
     *            <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @return a <code>Driver</code> object representing a driver that can connect
     *         to the given URL
     * @exception SQLException if a database access error occurs
     */
    public static Driver getDriver(String url) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Registers the given driver with the {@code DriverManager}. A newly-loaded
     * driver class should call the method {@code registerDriver} to make itself
     * known to the {@code DriverManager}. If the driver is currently registered, no
     * action is taken.
     *
     * @param driver the new JDBC Driver that is to be registered with the
     *               {@code DriverManager}
     * @exception SQLException         if a database access error occurs
     * @exception NullPointerException if {@code driver} is null
     */
    public static void registerDriver(sandbox.java.sql.Driver driver) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Registers the given driver with the {@code DriverManager}. A newly-loaded
     * driver class should call the method {@code registerDriver} to make itself
     * known to the {@code DriverManager}. If the driver is currently registered, no
     * action is taken.
     *
     * @param driver the new JDBC Driver that is to be registered with the
     *               {@code DriverManager}
     * @param da     the {@code DriverAction} implementation to be used when
     *               {@code DriverManager#deregisterDriver} is called
     * @exception SQLException         if a database access error occurs
     * @exception NullPointerException if {@code driver} is null
     * @since 1.8
     */
    public static void registerDriver(sandbox.java.sql.Driver driver, DriverAction da) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes the specified driver from the {@code DriverManager}'s list of
     * registered drivers.
     * <p>
     * If a {@code null} value is specified for the driver to be removed, then no
     * action is taken.
     * <p>
     * If a security manager exists, its {@code checkPermission} method is first
     * called with a {@code SQLPermission("deregisterDriver")} permission to check
     * that the caller is allowed to deregister a JDBC Driver.
     * <p>
     * If the specified driver is not found in the list of registered drivers, then
     * no action is taken. If the driver was found, it will be removed from the list
     * of registered drivers.
     * <p>
     * If a {@code DriverAction} instance was specified when the JDBC driver was
     * registered, its deregister method will be called prior to the driver being
     * removed from the list of registered drivers.
     *
     * @param driver the JDBC Driver to remove
     * @exception SQLException if a database access error occurs
     * @throws SecurityException if a security manager exists and its
     *                           {@code checkPermission} method denies permission to
     *                           deregister a driver.
     *
     * @see SecurityManager#checkPermission
     */
    public static void deregisterDriver(Driver driver) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves an Enumeration with all of the currently loaded JDBC drivers to
     * which the current caller has access.
     *
     * <P>
     * <B>Note:</B> The classname of a driver can be found using
     * <CODE>d.getClass().getName()</CODE>
     *
     * @return the list of JDBC Drivers loaded by the caller's class loader
     * @see #drivers()
     */

    public static Enumeration<Driver> getDrivers() {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves a Stream with all of the currently loaded JDBC drivers to which the
     * current caller has access.
     *
     * @return the stream of JDBC Drivers loaded by the caller's class loader
     * @since 9
     */

    public static Stream<Driver> drivers() {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the maximum time in seconds that a driver will wait while attempting to
     * connect to a database once the driver has been identified.
     *
     * @param seconds the login time limit in seconds; zero means there is no limit
     * @see #getLoginTimeout
     */
    public static void setLoginTimeout(int seconds) {
        loginTimeout = seconds;
    }

    /**
     * Gets the maximum time in seconds that a driver can wait when attempting to
     * log in to a database.
     *
     * @return the driver login time limit in seconds
     * @see #setLoginTimeout
     */
    public static int getLoginTimeout() {
        return (loginTimeout);
    }

    /**
     * Sets the logging/tracing PrintStream that is used by the
     * <code>DriverManager</code> and all drivers.
     * <P>
     * If a security manager exists, its {@code checkPermission} method is first
     * called with a {@code SQLPermission("setLog")} permission to check that the
     * caller is allowed to call {@code setLogStream}.
     *
     * @param out the new logging/tracing PrintStream; to disable, set to
     *            <code>null</code>
     * @deprecated Use {@code setLogWriter}
     * @throws SecurityException if a security manager exists and its
     *                           {@code checkPermission} method denies permission to
     *                           set the log stream.
     * @see SecurityManager#checkPermission
     * @see #getLogStream
     */
    @Deprecated(since = "1.2")
    public static void setLogStream(java.io.PrintStream out) {
    }

    /**
     * Retrieves the logging/tracing PrintStream that is used by the
     * <code>DriverManager</code> and all drivers.
     *
     * @return the logging/tracing PrintStream; if disabled, is <code>null</code>
     * @deprecated Use {@code getLogWriter}
     * @see #setLogStream
     */
    @Deprecated(since = "1.2")
    public static java.io.PrintStream getLogStream() {
        return logStream;
    }

    /**
     * Prints a message to the current JDBC log stream.
     *
     * @param message a log or tracing message
     */
    public static void println(String message) {
        synchronized (logSync) {
            if (logWriter != null) {
                logWriter.println(message);

                // automatic flushing is never enabled, so we must do it ourselves
                logWriter.flush();
            }
        }
    }

}
