
//////////////////////////////////////////////////////////////////////
//                                                                  //
//  JCSP ("CSP for Java") Libraries                                 //
//  Copyright (C) 1996-2018 Peter Welch, Paul Austin and Neil Brown //
//                2001-2004 Quickstone Technologies Limited         //
//                2005-2018 Kevin Chalmers                          //
//                                                                  //
//  You may use this work under the terms of either                 //
//  1. The Apache License, Version 2.0                              //
//  2. or (at your option), the GNU Lesser General Public License,  //
//       version 2.1 or greater.                                    //
//                                                                  //
//  Full licence texts are included in the LICENCE file with        //
//  this library.                                                   //
//                                                                  //
//  Author contacts: P.H.Welch@kent.ac.uk K.Chalmers@napier.ac.uk   //
//                                                                  //
//////////////////////////////////////////////////////////////////////

package jcsp.lang;

/**
 * <p>
 * This interface should be implemented by classes that wish to act as
 * connection servers and to accept requests from <code>ConnectionClient</code>
 * objects.
 * </p>
 *
 * <p>
 * The server can call <code>request()</code> to allow a client to establish a
 * connection to the server and to obtain the client's initial request. This
 * should block until a client establishes a connection.
 * </p>
 *
 * <p>
 * Once a request has been received, the server should reply to the client. If
 * the server wants to close the connection then the server should call
 * <code>replyAndClose(Object)</code> or alternatively
 * <code>reply(Object, boolean)</code> with the <code>boolean</code> set to
 * <code>true</code>. If the server wants to keep the connection open, then it
 * should call <code>reply(Object)</code> or alternatively
 * <code>reply(Object, boolean)</code> with the <code>boolean</code> set to
 * <code>false</code>. The <code>reply(Object, boolean)</code> method is
 * provided for convenience in closing connections programatically.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public interface ConnectionServer<T> {
    /**
     * The factory for creating channels within servers.
     */
    static StandardChannelFactory FACTORY = new StandardChannelFactory();

    /**
     * <p>
     * Receives a request from a client. This will block until the client calls its
     * <code>request(Object)</code> method. Implementations may make this ALTable.
     * </p>
     *
     * <p>
     * After this method has returned, the server should call one of the reply
     * methods. Performing any external process synchronization between these method
     * calls could be potentially hazardous and could lead to deadlock.
     * </p>
     *
     * @return the <code>Object</code> sent by the client.
     */
    public T request() throws IllegalStateException;

    /**
     * <p>
     * Sends some data back to the client after a request has been received but
     * keeps the connection open. After calling this method, the server should call
     * <code>recieve()</code> to receive a further request.
     * </p>
     *
     * <p>
     * A call to this method is equivalent to a call to
     * <code>reply(Object, boolean)</code> with the boolean set to
     * <code>false</code>.
     * </p>
     *
     * @param data the data to send to the client.
     */
    public void reply(T data) throws IllegalStateException;

    /**
     * <p>
     * Sends some data back to the client after a request has been received. The
     * <code>boolean</code> close parameter indicates whether the connection should
     * be closed after this reply has been sent.
     * </p>
     *
     * <p>
     * This method should not block.
     * </p>
     *
     * @param data  the data to send back to client.
     * @param close <code>boolean</code> that should be <code>true</code> iff the
     *              connection should be dropped after the reply has been sent.
     */
    public void reply(T data, boolean close);

    /**
     * <p>
     * Sends some data back to the client and closes the connection.
     * </p>
     *
     * <p>
     * A call to this method is equivalent to a call to
     * <code>reply(Object, boolean)</code> with the boolean set to
     * <code>true</code>.
     * </p>
     *
     * @param data the data to send back to client.
     */
    public void replyAndClose(T data) throws IllegalStateException;
}
