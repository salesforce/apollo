/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.liquibase;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

import liquibase.Scope;
import liquibase.resource.AbstractResourceAccessor;
import liquibase.resource.InputStreamList;
import liquibase.util.StringUtil;

/**
 * @author hal.hildebrand
 *
 */
public class MigrationAccessor extends AbstractResourceAccessor implements AutoCloseable {

    private LinkedHashSet<Path> rootPaths = new LinkedHashSet<>();
    private Path                tempFile;

    public MigrationAccessor(ByteString jar) throws IOException {
        tempFile = Files.createTempFile("cl_", ".jar");
        Files.delete(tempFile);
        Files.copy(BbBackedInputStream.aggregate(jar.asReadOnlyByteBuffer()), tempFile);
        tempFile.toFile().deleteOnExit();
        FileSystem root = FileSystems.newFileSystem(tempFile);
        rootPaths.add(root.getPath("/"));
    }

    protected void addRootPath(Path path) {
        Scope.getCurrentScope()
             .getLog(getClass())
             .fine("Adding path " + path + " to resourceAccessor " + getClass().getName());
        rootPaths.add(path);
    }

    protected LinkedHashSet<Path> getRootPaths() {
        return rootPaths;
    }

    @Override
    public InputStreamList openStreams(String relativeTo, String streamPath) throws IOException {
        streamPath = streamPath.replace("\\", "/");
        streamPath = streamPath.replaceFirst("^[\\\\/]([a-zA-Z]:)", "$1");
        final InputStreamList streams = new InputStreamList();

        streamPath = streamPath.replaceFirst("^/", ""); // Path is always relative to the file roots

        if (relativeTo != null) {
            relativeTo = relativeTo.replace("\\", "/");
            relativeTo = relativeTo.replaceFirst("^[\\\\/]([a-zA-Z]:)", "$1");
            relativeTo = relativeTo.replaceFirst("^/", ""); // Path is always relative to the file roots
        }

        for (Path rootPath : rootPaths) {
            URI streamURI = null;
            if (rootPath == null) {
                continue;
            }
            InputStream stream = null;
            if (isCompressedFile(rootPath)) {
                String finalPath = streamPath;

                // Can't close zipFile here, as we are (possibly) returning its child stream
                ZipFile zipFile = new ZipFile(rootPath.toFile());
                if (relativeTo != null) {
                    ZipEntry relativeEntry = zipFile.getEntry(relativeTo);
                    if (relativeEntry == null || relativeEntry.isDirectory()) {
                        // not a file, maybe a directory
                        finalPath = relativeTo + "/" + streamPath;
                    } else {
                        // is a file, find path relative to parent
                        String actualRelativeTo = relativeTo;
                        if (actualRelativeTo.contains("/")) {
                            actualRelativeTo = relativeTo.replaceFirst("/[^/]+?$", "");
                        } else {
                            actualRelativeTo = "";
                        }
                        finalPath = actualRelativeTo + "/" + streamPath;
                    }

                }

                // resolve any ..'s and duplicated /'s and convert back to standard '/'
                // separator format
                finalPath = Paths.get(finalPath.replaceFirst("^/", "")).normalize().toString().replace("\\", "/");

                ZipEntry entry = zipFile.getEntry(finalPath);
                if (entry != null) {
                    // closing this stream will close zipFile
                    stream = new CloseChildWillCloseParentStream(zipFile.getInputStream(entry), zipFile);
                    streamURI = URI.create(rootPath.normalize().toUri() + "!" + entry.toString());
                } else {
                    zipFile.close();
                }
            } else {
                Path finalRootPath = rootPath;
                if (relativeTo != null) {
                    finalRootPath = finalRootPath.resolve(relativeTo);
                    if (Files.exists(finalRootPath)) {
                        if (Files.isDirectory(finalRootPath)) {
                            // relative to directory
                            finalRootPath = finalRootPath.getParent();
                        }
                    } else {
                        Scope.getCurrentScope()
                             .getLog(getClass())
                             .fine("No relative path " + relativeTo + " in " + rootPath);
                        continue;
                    }
                }
                try {
                    if (Paths.get(streamPath).startsWith(finalRootPath) ||
                        Paths.get(streamPath).startsWith("/" + finalRootPath)) {
                        streamPath = finalRootPath.relativize(Paths.get(streamPath)).toString();
                    }
                    if (Paths.get("/" + streamPath).startsWith(finalRootPath)) {
                        streamPath = finalRootPath.relativize(Paths.get("/" + streamPath)).toString();
                    }
                } catch (InvalidPathException ignored) {
                    // that is ok
                }

                if (Paths.get(streamPath).isAbsolute()) {
                    continue; // on a windows system with an absolute path that doesn't start with rootPath
                }

                Path resolved = finalRootPath.resolve(streamPath);
                if (Files.exists(resolved)) {
                    streamURI = resolved.toUri();
                    stream = new BufferedInputStream(Files.newInputStream(resolved));
                }

            }

            if (stream != null) {
                if (streamPath.toLowerCase().endsWith(".gz")) {
                    stream = new GZIPInputStream(stream);
                }

                streams.add(streamURI, stream);
            }

        }

        return streams;
    }

    @Override
    public SortedSet<String> list(String relativeTo, String path, boolean recursive, boolean includeFiles,
                                  boolean includeDirectories) throws IOException {
        final SortedSet<String> returnList = new TreeSet<>();

        int maxDepth = recursive ? Integer.MAX_VALUE : 1;

        for (final Path rootPath : getRootPaths()) {
            SimpleFileVisitor<Path> fileVisitor = new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (includeFiles && attrs.isRegularFile()) {
                        addToReturnList(file);
                    }
                    if (includeDirectories && attrs.isDirectory()) {
                        addToReturnList(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (includeDirectories) {
                        addToReturnList(dir);
                    }
                    return FileVisitResult.CONTINUE;
                }

                protected void addToReturnList(Path file) {
                    String pathToAdd;
                    if (isCompressedFile(rootPath)) {
                        pathToAdd = file.normalize().toString().substring(1); // pull off leading /
                    } else {
                        pathToAdd = rootPath.relativize(file).normalize().toString().replace("\\", "/");
                    }

                    pathToAdd = pathToAdd.replaceFirst("/$", "");
                    returnList.add(pathToAdd);
                }

            };

            if (isCompressedFile(rootPath)) {
                try (FileSystem fs = FileSystems.newFileSystem(rootPath, (ClassLoader) null)) {
                    Path basePath = fs.getRootDirectories().iterator().next();

                    if (relativeTo != null) {
                        basePath = basePath.resolve(relativeTo);
                        if (!Files.exists(basePath)) {
                            Scope.getCurrentScope()
                                 .getLog(getClass())
                                 .info("Relative path " + relativeTo + " in " + rootPath + " does not exist");
                            continue;
                        } else if (Files.isRegularFile(basePath)) {
                            basePath = basePath.getParent();
                        }
                    }

                    if (path != null) {
                        basePath = basePath.resolve(path).normalize();
                    }

                    Files.walkFileTree(basePath, Collections.singleton(FileVisitOption.FOLLOW_LINKS), maxDepth,
                                       fileVisitor);
                } catch (NoSuchFileException e) {
                    // nothing to do, return null
                }
            } else {
                Path basePath = rootPath;

                if (relativeTo != null) {
                    basePath = basePath.resolve(relativeTo);
                    if (!Files.exists(basePath)) {
                        Scope.getCurrentScope()
                             .getLog(getClass())
                             .info("Relative path " + relativeTo + " in " + rootPath + " does not exist");
                        continue;
                    } else if (Files.isRegularFile(basePath)) {
                        basePath = basePath.getParent();
                    }
                }

                if (path != null) {
                    if (path.startsWith("/") || path.startsWith("\\")) {
                        path = path.substring(1);
                    }

                    basePath = basePath.resolve(path);
                }

                if (!Files.exists(basePath)) {
                    continue;
                }
                Files.walkFileTree(basePath, Collections.singleton(FileVisitOption.FOLLOW_LINKS), maxDepth,
                                   fileVisitor);
            }
        }

        returnList.remove(path);
        return returnList;
    }

    /**
     * Returns true if the given path is a compressed file.
     */
    protected boolean isCompressedFile(Path path) {
        return path != null && Files.exists(path) &&
               (path.toString().startsWith("jar:") || path.toString().toLowerCase().endsWith(".jar") ||
                path.toString().toLowerCase().endsWith(".zip"));
    }

    @Override
    public String toString() {
        return getClass().getName() + " (" + StringUtil.join(getRootPaths(), ", ", new StringUtil.ToStringFormatter())
        + ")";
    }

    @Override
    public SortedSet<String> describeLocations() {
        SortedSet<String> returnSet = new TreeSet<>();

        for (Path path : getRootPaths()) {
            returnSet.add(path.toString());
        }

        return returnSet;
    }

    private static class CloseChildWillCloseParentStream extends FilterInputStream {

        private final Closeable parent;

        protected CloseChildWillCloseParentStream(InputStream in, Closeable parent) {
            super(in);
            this.parent = parent;
        }

        @Override
        public void close() throws IOException {
            super.close();
            parent.close();
        }
    }

    public void close() {
        try {
            Files.delete(tempFile);
        } catch (IOException e) {
            // ignored
        }
    }

}
