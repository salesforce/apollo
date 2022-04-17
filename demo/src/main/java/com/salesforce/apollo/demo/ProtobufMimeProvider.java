/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * @author hal.hildebrand
 *
 */
@Provider
@Consumes("application/x-protobuf")
@Produces("application/x-protobuf")

public class ProtobufMimeProvider implements MessageBodyWriter<Message>, MessageBodyReader<Message> {
    // MessageBodyWriter Implementation
    @Override
    public long getSize(Message message, Class<?> arg1, Type arg2, Annotation[] arg3, MediaType arg4) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            message.writeTo(baos);
        } catch (IOException e) {
            return -1;
        }
        return baos.size();
    }

    // MessageBodyReader Implementation
    @Override
    public boolean isReadable(Class<?> arg0, Type arg1, Annotation[] arg2, MediaType arg3) {
        return Message.class.isAssignableFrom(arg0);
    }

    @Override
    public boolean isWriteable(Class<?> arg0, Type arg1, Annotation[] arg2, MediaType arg3) {
        return Message.class.isAssignableFrom(arg0);
    }

    @Override
    public Message readFrom(Class<Message> arg0, Type arg1, Annotation[] arg2, MediaType arg3,
                            MultivaluedMap<String, String> arg4,
                            InputStream istream) throws IOException, WebApplicationException {
        try {
            Method builderMethod = arg0.getMethod("newBuilder");
            GeneratedMessage.Builder<?> builder = (GeneratedMessage.Builder<?>) builderMethod.invoke(arg0);
            return builder.mergeFrom(istream).build();
        } catch (Exception e) {
            throw new WebApplicationException(e);
        }
    }

    @Override
    public void writeTo(Message message, Class<?> arg1, Type arg2, Annotation[] arg3, MediaType arg4,
                        MultivaluedMap<String, Object> arg5,
                        OutputStream ostream) throws IOException, WebApplicationException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        message.writeTo(baos);
        ostream.write(baos.toByteArray());
    }
}
