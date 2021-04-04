/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.message.DbException;

import com.salesforce.apollo.protocols.Utils;

import net.corda.djvm.SandboxConfiguration;
import net.corda.djvm.SandboxRuntimeContext;
import net.corda.djvm.TypedTaskFactory;
import net.corda.djvm.analysis.AnalysisConfiguration;
import net.corda.djvm.analysis.AnalysisConfiguration.Builder;
import net.corda.djvm.execution.ExecutionProfile;
import net.corda.djvm.messages.Severity;
import net.corda.djvm.rewiring.SandboxClassLoader;
import net.corda.djvm.source.ApiSource;
import net.corda.djvm.source.BootstrapClassLoader;
import net.corda.djvm.source.UserPathSource;
import net.corda.djvm.source.UserSource;

/**
 * Represents a class loading catolog of deterministic Java implemented
 * functions for SQL stored procedures, functions, triggers n' such.
 * 
 * @author hal.hildebrand
 *
 */
public class Functions implements UserSource {

    private static class ClassFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {

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

    private static class JavaClassObject extends SimpleJavaFileObject {

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

    private static class StringJavaFileObject extends SimpleJavaFileObject {

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

    public static final ApiSource   BOOTSTRAP;
    public static final Set<String> OVERRIDE_CLASSES = overrideClasses();

    private static final String     BOOTSTRAP_JAR    = "/deterministic-rt.jar";
    private static final int        DOT_CLASS_LENGTH = ".class".length();
    private static JavaCompiler     javaCompiler;
    private static final UserSource NULL_SOURCE      = new UserSource() {

                                                         @Override
                                                         public void close() throws Exception {
                                                         }

                                                         @Override
                                                         public URL findResource(String arg0) {
                                                             return getClass().getClassLoader().getResource(arg0);
                                                         }

                                                         @Override
                                                         public Enumeration<URL> findResources(String arg0) {
                                                             try {
                                                                 return getClass().getClassLoader().getResources(arg0);
                                                             } catch (IOException e) {
                                                                 throw new IllegalStateException(e);
                                                             }
                                                         }

                                                         @Override
                                                         public URL[] getURLs() {
                                                             return new URL[] {};
                                                         }
                                                     };

    static {
        try {
            File tempFile = File.createTempFile("bootstrap", ".jar");
            tempFile.delete();
            tempFile.deleteOnExit();
            try (InputStream is = Functions.class.getResourceAsStream(BOOTSTRAP_JAR);
                    FileOutputStream os = new FileOutputStream(tempFile);) {
                Utils.copy(is, os);
                os.flush();
            }
            BOOTSTRAP = new BootstrapClassLoader(tempFile.toPath());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to cache boxotstrap jar", e);
        }
    }

    private static UserSource dsqlApi() {
        URL url = Functions.class.getResource("/dsql-api.jar");
        return new UserPathSource(new URL[] { url });
    }

    public static AnalysisConfiguration defaultConfig() {
        AnalysisConfiguration config = AnalysisConfiguration.createRoot(dsqlApi(), Collections.emptySet(),
                                                                        Severity.TRACE, BOOTSTRAP, OVERRIDE_CLASSES);
        return config;
    }

    public static String getCompleteSourceCode(String packageName, String className, String source) {
        if (source.startsWith("package ")) {
            return source;
        }
        StringBuilder buff = new StringBuilder();
        if (packageName != null) {
            buff.append("package ").append(packageName).append(";\n");
        }
        int endImport = source.indexOf("@CODE");
        String importCode = """
                import java.util.*;
                import java.math.*;
                import sandbox.java.sql.*;

                """;
        if (endImport >= 0) {
            importCode = source.substring(0, endImport);
            source = source.substring("@CODE".length() + endImport);
        }
        buff.append(importCode);
        buff.append("public class ").append(className).append(" {\n    public static ").append(source).append("\n}\n");
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

    private static Set<String> overrideClasses() {
        return new HashSet<>(
                Arrays.asList("sandbox/org/h2/api/Trigger", "sandbox/java/sql/Array",
                              "sandbox/java/sql/BatchUpdateException", "sandbox/java/sql/Blob",
                              "sandbox/java/sql/CallableStatement", "sandbox/java/sql/ClientInfoStatus",
                              "sandbox/java/sql/Clob", "sandbox/java/sql/Connection",
                              "sandbox/java/sql/ConnectionBuilder", "sandbox/java/sql/DatabaseMetaData",
                              "sandbox/java/sql/DataTruncation", "sandbox/java/sql/Date", "sandbox/java/sql/Driver",
                              "sandbox/java/sql/DriverAction", "sandbox/java/sql/DriverInfo",
                              "sandbox/java/sql/DriverManager", "sandbox/java/sql/DriverPropertyInfo",
                              "sandbox/java/sql/JDBCType", "sandbox/java/sql/NClob",
                              "sandbox/java/sql/ParameterMetaData", "sandbox/java/sql/PreparedStatement",
                              "sandbox/java/sql/PseudoColumnUsage", "sandbox/java/sql/Ref",
                              "sandbox/java/sql/ResultSet", "sandbox/java/sql/ResultSetMetaData",
                              "sandbox/java/sql/RowId", "sandbox/java/sql/RowIdLifetime", "sandbox/java/sql/Savepoint",
                              "sandbox/java/sql/ShardingKey", "sandbox/java/sql/ShardingKeyBuilder",
                              "sandbox/java/sql/SQLClientInfoException", "sandbox/java/sql/SQLData",
                              "sandbox/java/sql/SQLDataException", "sandbox/java/sql/SQLException",
                              "sandbox/java/sql/SQLFeatureNotSupportedException", "sandbox/java/sql/SQLInput",
                              "sandbox/java/sql/SQLIntegrityConstraintViolationException",
                              "sandbox/java/sql/SQLInvalidAuthorizationSpecException",
                              "sandbox/java/sql/SQLNonTransientConnectionException",
                              "sandbox/java/sql/SQLNonTransientException", "sandbox/java/sql/SQLOutput",
                              "sandbox/java/sql/SQLPermission", "sandbox/java/sql/SQLRecoverableException",
                              "sandbox/java/sql/SQLSyntaxErrorException", "sandbox/java/sql/SQLTimeoutException",
                              "sandbox/java/sql/SQLTransactionRollbackException",
                              "sandbox/java/sql/SQLTransientConnectionException",
                              "sandbox/java/sql/SQLTransientException", "sandbox/java/sql/SQLType",
                              "sandbox/java/sql/SQLWarning", "sandbox/java/sql/SQLXML", "sandbox/java/sql/Statement",
                              "sandbox/java/sql/Struct", "sandbox/java/sql/Time", "sandbox/java/sql/Timestamp",
                              "sandbox/java/sql/Types", "sandbox/java/sql/Wrapper"));
    }

    private static File tempDir() throws IllegalStateException {
        File tempDir;
        try {
            tempDir = File.createTempFile("functions-" + UUID.randomUUID(), "dir");
        } catch (IOException e) {
            throw new IllegalStateException("unable to create temp directory for cached classes");
        }
        tempDir.delete();
        tempDir.mkdirs();
        tempDir.deleteOnExit();
        return tempDir;
    }

    private final File                  cacheDir;
    private final Map<String, File>     compiledClasses = new ConcurrentHashMap<>();
    private final SandboxRuntimeContext context;

    {
        JavaCompiler c;
        try {
            c = ToolProvider.getSystemJavaCompiler();
        } catch (Exception e) {
            throw new IllegalStateException("Java compiler required", e);
        }
        javaCompiler = c;
    }

    public Functions() throws IOException, ClassNotFoundException, IllegalStateException {
        this(defaultConfig(), ExecutionProfile.DEFAULT, tempDir());
    }

    public Functions(AnalysisConfiguration config, ExecutionProfile execution, File cacheDir)
            throws ClassNotFoundException, IOException {
        Utils.clean(cacheDir);
        cacheDir.mkdirs();
        if (!cacheDir.isDirectory()) {
            throw new IllegalArgumentException(cacheDir.getAbsolutePath() + " must be directory");
        }
        this.cacheDir = cacheDir;
        Builder childConfig = config.createChild(this);

        SandboxConfiguration cfg = SandboxConfiguration.createFor(childConfig.build(), execution);
        cfg.preload();
        context = new SandboxRuntimeContext(cfg);
    }

    @Override
    public void close() throws Exception {
        compiledClasses.clear();
    }

    public <T> Class<T> compile(String packageAndClassName, String source) throws ClassNotFoundException {

        ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {

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
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) classLoader.loadClass(packageAndClassName);
        return clazz;
    }

    public Trigger compileTrigger(String packageAndClassName, String source) throws ClassNotFoundException {
        compile(packageAndClassName, source);
        AtomicReference<Trigger> holder = new AtomicReference<>();

        context.use(ctx -> {
            SandboxClassLoader cl = ctx.getClassLoader();
            Class<?> triggerClass;
            try {
                triggerClass = cl.loadClass("sandbox." + packageAndClassName);
                holder.set(new SandboxTrigger(context,
                         triggerClass.getDeclaredConstructor().newInstance()));
            } catch (Exception e) {
                throw new IllegalStateException("cannot create trigger", e);
            }
        });
        return holder.get();
    }

    public void execute(Class<? extends Function<long[], Long>> clazz) {
        context.use(ctx -> {
            SandboxClassLoader cl = ctx.getClassLoader();

            // Create a reusable task factory.
            TypedTaskFactory taskFactory;
            try {
                taskFactory = cl.createTypedTaskFactory();
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                throw new IllegalStateException();
            }

            // Wrap SimpleTask inside an instance of sandbox.Task.
            Function<long[], Long> simpleTask = taskFactory.create(clazz);

            // Execute SimpleTask inside the sandbox.
            @SuppressWarnings("unused")
            Long result = simpleTask.apply(new long[] { 1000, 200, 30, 4 });
        });
    }

    @Override
    public URL findResource(String name) {
        String binaryName = toBinaryName(name);
        File file = compiledClasses.get(binaryName);
        if (file == null) {
            return null;
        }
        try {
            return file.toURI().toURL();
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
                javaCompiler.getStandardFileManager(null, null, null))) {
            ArrayList<JavaFileObject> compilationUnits = new ArrayList<>();
            compilationUnits.add(new StringJavaFileObject(fullClassName, source));
            // cannot concurrently compile
            final boolean ok;
            synchronized (javaCompiler) {
                ok = javaCompiler.getTask(writer, fileManager, null, Arrays.asList("-target", "1.8", "-source", "1.8"),
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
