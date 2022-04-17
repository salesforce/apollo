/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Arrays;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ScriptCompiler {
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

    private static final String COMPILE_DIR = Utils.getProperty("java.io.tmpdir", ".");

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

    /**
     * Get the complete source code (including package name, imports, and so on).
     *
     * @param packageName the package name
     * @param className   the class name
     * @param source      the (possibly shortened) source code
     * @return the full source code
     */
    static String getCompleteSourceCode(String packageName, String className, String source) {
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

    /**
     * Get the class object for the given source.
     * 
     * @param packageAndClassName
     * @param source              - the source of the class
     *
     * @return the class
     */
    public Class<?> getClass(String packageAndClassName, String source,
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
                return javaxToolsJavac(packageName, className, s);
            }
        };
        return classLoader.loadClass(packageAndClassName);
    }

    /**
     * Get the first public static method of the given class.
     *
     * @param className the class name
     * @return the method name
     */
    public Method getMethod(String className, String source, ClassLoader parent) throws ClassNotFoundException {
        Class<?> clazz = getClass(className, source, parent);
        Method[] methods = clazz.getDeclaredMethods();
        for (Method m : methods) {
            int modifiers = m.getModifiers();
            if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)) {
                String name = m.getName();
                if (!name.startsWith("_") && !m.getName().equals("main")) {
                    return m;
                }
            }
        }
        return null;
    }

    /**
     * Compile using the standard java compiler.
     *
     * @param packageName the package name
     * @param className   the class name
     * @param source      the source code
     * @return the class
     */
    Class<?> javaxToolsJavac(String packageName, String className, String source) {
        String fullClassName = packageName == null ? className : packageName + "." + className;
        StringWriter writer = new StringWriter();
        try (JavaFileManager fileManager = new ClassFileManager(JAVA_COMPILER.getStandardFileManager(null, null,
                                                                                                     null))) {
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
            return fileManager.getClassLoader(null).loadClass(fullClassName);
        } catch (ClassNotFoundException | IOException e) {
            throw DbException.convert(e);
        }
    }

}
