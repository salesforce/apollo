/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.functions;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
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

import com.salesforce.apollo.protocols.Utils;

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
     * The "com.sun.tools.javac.Main" (if available).
     */
    static final JavaCompiler JAVA_COMPILER;

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
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, output);
        }
    }

    private final File              cacheDir;
    private final Map<String, File> compiledClasses = new ConcurrentHashMap<>();

    public Functions() {
        File tempDir;
        try {
            tempDir = File.createTempFile("functions-" + System.identityHashCode(this), "dir");
        } catch (IOException e) {
            throw new IllegalStateException("unable to create temp directory for cached classes");
        }
        tempDir.delete();
        tempDir.mkdirs();
        tempDir.deleteOnExit();
        this.cacheDir = tempDir;
    }

    public Functions(File cacheDir) {
        Utils.clean(cacheDir);
        cacheDir.mkdirs();
        if (!cacheDir.isDirectory()) {
            throw new IllegalArgumentException(cacheDir.getAbsolutePath() + " must be directory");
        }
        this.cacheDir = cacheDir;
    }

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
                File clazzFile;
                try {
                    clazzFile = File.createTempFile(binaryName, "class", cacheDir);
                } catch (IOException e) {
                    throw new ClassNotFoundException("unable to store: " + packageAndClassName, e);
                }

                try (OutputStream os = new FileOutputStream(clazzFile)) {
                    os.write(classBytes);
                } catch (IOException e) {
                    throw new ClassNotFoundException("unable to store: " + packageAndClassName, e);
                }

                compiledClasses.put(binaryName, clazzFile);
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
        File file = compiledClasses.get(binaryName);
        try {
            return file == null ? null : file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException("unable to construct url for: " + file.getAbsolutePath(), e);
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
