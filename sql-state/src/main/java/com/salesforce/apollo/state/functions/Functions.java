/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.functions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

import net.corda.djvm.source.UserSource;

/**
 * Represents a class loading catolog of deterministic Java implemented
 * functions for SQL stored procedures, functions, triggers n' such.
 * 
 * @author hal.hildebrand
 *
 */
public class Functions implements UserSource {
    /**
     * An in-memory class file manager.
     */
    static class ClassFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {

        /**
         * The class (only one class is kept).
         */
        JavaClassObject classObject;

        public ClassFileManager(StandardJavaFileManager standardManager) {
            super(standardManager);
        }

        @Override
        public ClassLoader getClassLoader(Location location) {
            return new SecureClassLoader() {
                @Override
                protected Class<?> findClass(String name) throws ClassNotFoundException {
                    byte[] bytes = classObject.getBytes();
                    return super.defineClass(name, bytes, 0, bytes.length);
                }

                @Override
                protected URL findResource(String name) {
                    try {
                        return classObject.toUri().toURL();
                    } catch (MalformedURLException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind,
                                                   FileObject sibling) throws IOException {
            classObject = new JavaClassObject(className, kind);
            return classObject;
        }
    }

    /**
     * An in-memory java class object.
     */
    static class JavaClassObject extends SimpleJavaFileObject {

        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        public JavaClassObject(String name, Kind kind) {
            super(URI.create("string:///" + name.replace('.', '/') + kind.extension), kind);
        }

        public byte[] getBytes() {
            return out.toByteArray();
        }

        @Override
        public OutputStream openOutputStream() throws IOException {
            return out;
        }
    }

    /**
     * An in-memory java source file object.
     */
    static class StringJavaFileObject extends SimpleJavaFileObject {

        private final String sourceCode;

        public StringJavaFileObject(String className, String sourceCode) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.sourceCode = sourceCode;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return sourceCode;
        }

    }

    /**
     * A URLConnection for use with URLs returned by MemoryClassLoader.getResource.
     */
    private static class MemoryURLConnection extends URLConnection {
        private byte[]      bytes;
        private InputStream in;

        MemoryURLConnection(URL u, byte[] bytes) {
            super(u);
            this.bytes = bytes;
        }

        @Override
        public void connect() throws IOException {
            if (!connected) {
                if (bytes == null) {
                    throw new FileNotFoundException(getURL().getPath());
                }
                in = new ByteArrayInputStream(bytes);
                connected = true;
            }
        }

        @Override
        public long getContentLengthLong() {
            return bytes.length;
        }

        @Override
        public String getContentType() {
            return "application/octet-stream";
        }

        @Override
        public InputStream getInputStream() throws IOException {
            connect();
            return in;
        }
    }

    /**
     * A URLStreamHandler for use with URLs returned by
     * MemoryClassLoader.getResource.
     */
    private class MemoryURLStreamHandler extends URLStreamHandler {
        @Override
        public URLConnection openConnection(URL u) {
            if (!u.getProtocol().equalsIgnoreCase(PROTOCOL)) {
                throw new IllegalArgumentException(u.toString());
            }
            return new MemoryURLConnection(u, compiledClasses.get(toBinaryName(u.getPath())));
        }

    }

    /**
     * The "com.sun.tools.javac.Main" (if available).
     */
    static final JavaCompiler JAVA_COMPILER;

    private static final String COMPILE_DIR = Utils.getProperty("java.io.tmpdir", ".");

    private static final int DOT_CLASS_LENGTH = ".class".length();

    static {
        JavaCompiler c;
        try {
            c = ToolProvider.getSystemJavaCompiler();
        } catch (Exception e) {
            // ignore
            c = null;
        }
        JAVA_COMPILER = c;
    }

    private static String getCompleteSourceCode(String packageName, String className, String source) {
        if (source.startsWith("package ")) {
            return source;
        }
        StringBuilder buff = new StringBuilder();
        if (packageName != null) {
            buff.append("package ").append(packageName).append(";\n");
        }
        int endImport = source.indexOf("@CODE");
        String importCode = "import java.util.*;\n" + "import java.math.*;\n" + "import java.sql.*;\n";
        if (endImport >= 0) {
            importCode = source.substring(0, endImport);
            source = source.substring("@CODE".length() + endImport);
        }
        buff.append(importCode);
        buff.append("public class ")
            .append(className)
            .append(" {\n" + "    public static ")
            .append(source)
            .append("\n" + "}\n");
        return buff.toString();
    }

    private static void handleSyntaxError(String output, int exitStatus) {
        if (0 == exitStatus) {
            return;
        }
        boolean syntaxError = false;
        final BufferedReader reader = new BufferedReader(new StringReader(output));
        try {
            for (String line; (line = reader.readLine()) != null;) {
                if (line.endsWith("warning") || line.endsWith("warnings")) {
                    // ignore summary line
                } else if (line.startsWith("Note:") || line.startsWith("warning:")) {
                    // just a warning (e.g. unchecked or unsafe operations)
                } else {
                    syntaxError = true;
                    break;
                }
            }
        } catch (IOException ignored) {
            // exception ignored
        }

        if (syntaxError) {
            output = StringUtils.replaceAll(output, COMPILE_DIR, "");
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, output);
        }
    }

    private final Map<String, byte[]> compiledClasses = new ConcurrentHashMap<>();
    private final URLStreamHandler    handler         = new MemoryURLStreamHandler();
    private final String              PROTOCOL        = "sourcelauncher-" + getClass().getSimpleName() + hashCode();

    @Override
    public void close() throws Exception {
        compiledClasses.clear();
    }

    public Class<?> compile(String packageAndClassName, String source,
                            ClassLoader parent) throws ClassNotFoundException {

        ClassLoader classLoader = new ClassLoader(parent) {

            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                String packageName = null;
                int idx = name.lastIndexOf('.');
                String className;
                if (idx >= 0) {
                    packageName = name.substring(0, idx);
                    className = name.substring(idx + 1);
                } else {
                    className = name;
                }
                String s = getCompleteSourceCode(packageName, className, source);
                s = source;
                byte[] classBytes = javaxToolsJavac(packageName, className, s);
                String binaryName = toBinaryName(packageAndClassName);
                compiledClasses.put(binaryName, classBytes);
                return defineClass(binaryName, classBytes, 0, classBytes.length);
            }
        };
        return classLoader.loadClass(packageAndClassName);
    }

    @Override
    public URL findResource(String name) {
        String binaryName = toBinaryName(name);
        if (binaryName == null || compiledClasses.get(binaryName) == null) {
            return null;
        }
        try {
            return new URL(PROTOCOL, null, -1, name, handler);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    @Override
    public Enumeration<URL> findResources(String name) {
        return new Enumeration<URL>() {
            private URL next = findResource(name);

            @Override
            public boolean hasMoreElements() {
                return (next != null);
            }

            @Override
            public URL nextElement() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                URL u = next;
                next = null;
                return u;
            }
        };
    }

    @Override
    public URL[] getURLs() {
        return new URL[0];
    }

    private byte[] javaxToolsJavac(String packageName, String className, String source) {
        String fullClassName = packageName == null ? className : packageName + "." + className;
        StringWriter writer = new StringWriter();
        try (ClassFileManager fileManager = new ClassFileManager(
                JAVA_COMPILER.getStandardFileManager(null, null, null))) {
            ArrayList<JavaFileObject> compilationUnits = new ArrayList<>();
            compilationUnits.add(new StringJavaFileObject(fullClassName, source));
            // cannot concurrently compile
            final boolean ok;
            synchronized (JAVA_COMPILER) {
                ok = JAVA_COMPILER.getTask(writer, fileManager, null, Arrays.asList("-target", "1.8", "-source", "1.8"),
                                           null, compilationUnits)
                                  .call();
            }
            String output = writer.toString();
            handleSyntaxError(output, (ok ? 0 : 1));
            return fileManager.classObject.getBytes();
        } catch (IOException e) {
            // ignored
            return null;
        }
    }

    /**
     * Converts a "resource name" (as used in the getResource* methods) to a binary
     * name if the name identifies a class
     * 
     * @param name the resource name
     * @return the binary name
     */
    private String toBinaryName(String name) {
        if (!name.endsWith(".class")) {
            return name;
        }
        return name.substring(0, name.length() - DOT_CLASS_LENGTH).replace('/', '.');
    }
}
