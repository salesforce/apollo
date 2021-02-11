/** 
 * (C) Copyright 2009 Hal Hildebrand, All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package com.salesforce.apollo.protocols;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.ClosedChannelException;
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.X509CertificateHolder;

/**
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */

public class Utils {

    private static enum ParsingState {
        BRACKET, DOLLAR, PASS_THROUGH
    }

    private static ThreadLocal<BitsStreamGenerator> BIT_STREAM_ENTROPY = new ThreadLocal<>() {
        @Override
        protected BitsStreamGenerator initialValue() {
            return new MersenneTwister();
        }
    };

    private static ThreadLocal<SecureRandom> ENTROPY = new ThreadLocal<>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };

    public static Object accessField(String fieldName, Object target) throws SecurityException, NoSuchFieldException,
                                                                      IllegalArgumentException, IllegalAccessException {
        Field field;
        try {
            field = target.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superClass = target.getClass().getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessField(fieldName, target, superClass);
        }
        field.setAccessible(true);
        return field.get(target);
    }

    public static Object accessField(String fieldName, Object target,
                                     Class<?> targetClass) throws SecurityException, NoSuchFieldException,
                                                           IllegalArgumentException, IllegalAccessException {
        Field field;
        try {
            field = targetClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superClass = targetClass.getSuperclass();
            if (superClass == null) {
                throw e;
            }
            return accessField(fieldName, target, superClass);
        }
        field.setAccessible(true);
        return field.get(target);
    }

    public static void addToZip(File root, File file, ZipOutputStream zos) throws IOException {
        String relativePath = Utils.relativize(root, file.getAbsoluteFile()).getPath();
        if (file.isDirectory()) {
            relativePath += '/';
        }
        ZipEntry ze = new ZipEntry(relativePath);
        zos.putNextEntry(ze);
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                addToZip(root, child, zos);
            }
        } else {
            try (FileInputStream fis = new FileInputStream(file)) {
                copy(fis, zos);
            }
        }
    }

    /**
     * Find a free port for any local address
     * 
     * @return the port number or -1 if none available
     */
    public static int allocatePort() {
        return allocatePort(null);
    }

    /**
     * Find a free port on the interface with the given address
     * 
     * @return the port number or -1 if none available
     */
    public static int allocatePort(InetAddress host) {
        InetSocketAddress address = host == null ? new InetSocketAddress(0) : new InetSocketAddress(host, 0);
        try (ServerSocket socket = new ServerSocket();) {
            socket.bind(address);
            return socket.getLocalPort();
        } catch (IOException e) {
        }
        return -1;
    }

    public static BitsStreamGenerator bitStreamEntropy() {
        return BIT_STREAM_ENTROPY.get();
    }

    public static BitsStreamGenerator bitStreamGenerator() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Clean the contents of a directory
     * 
     * @param directory
     */
    public static void clean(File directory) {
        if (directory.exists()) {
            if (directory.isDirectory()) {
                for (File file : directory.listFiles()) {
                    if (file.isDirectory()) {
                        remove(file);
                    } else {
                        if (!file.delete()) {
                            throw new IllegalStateException(String.format("Cannot delete [%s] ", file));
                        }
                    }
                }
            }
        }
    }

    public static void copy(File sourceFile, File destFile) throws IOException {
        copy(sourceFile, destFile, 4096);
    }

    /**
     * Copy the contents of the source file into the destination file using the
     * supplied buffer
     * 
     * @param sourceFile
     * @param destFile
     * @param buffer
     * @throws IOException
     */
    public static void copy(File sourceFile, File destFile, byte[] buffer) throws IOException {
        try (InputStream is = new FileInputStream(sourceFile); OutputStream os = new FileOutputStream(destFile);) {
            copy(is, os, buffer);
        }
    }

    /**
     * Copy the contents of the source file into the destination file using a buffer
     * of the supplied size
     * 
     * @param sourceFile
     * @param destFile
     * @param bufferSize
     * @throws IOException
     */
    public static void copy(File sourceFile, File destFile, int bufferSize) throws IOException {
        copy(sourceFile, destFile, new byte[bufferSize]);
    }

    /**
     * 
     * Copy and transform the zip entry to the destination. If the transformation
     * extensions contains the entry's extension, then ${xxx} style parameters are
     * replace with the supplied properties or System.getProperties()
     * 
     * @param dest
     * @param zf
     * @param ze
     * @param extensions
     * @param properties
     * @throws IOException
     */
    public static void copy(File dest, ZipFile zf, ZipEntry ze, Map<String, String> properties,
                            Collection<String> extensions) throws IOException {
        try (InputStream is = zf.getInputStream(ze)) {
            File outFile = new File(dest, ze.getName());
            if (ze.isDirectory()) {
                outFile.mkdirs();
            } else {
                File parent = outFile.getParentFile();
                if (parent != null) {
                    parent.mkdirs();
                }

                try (FileOutputStream fos = new FileOutputStream(outFile)) {
                    if (extensions.contains(getExtension(outFile.getName()))) {
                        replaceProperties(is, fos, properties);
                    } else {
                        copy(is, fos);
                    }
                }
            }
        }
    }

    /**
     * Copy the contents of the input stream into the output stream using the
     * default buffer size
     * 
     * @param is
     * @param os
     * @throws IOException
     */
    public static void copy(InputStream is, OutputStream os) throws IOException {
        copy(is, os, 16 * 1024);
    }

    /**
     * Copy the contents of the input stream to the output stream. It is the
     * caller's responsibility to close the streams.
     * 
     * @param is     - source
     * @param os     - destination
     * @param buffer - byte buffer to use
     * @throws IOException
     */
    public static void copy(InputStream is, OutputStream os, byte[] buffer) throws IOException {
        int len;
        while ((len = is.read(buffer)) > 0) {
            os.write(buffer, 0, len);
        }
    }

    /**
     * Copy the contents of the input stream to the output stream. It is the
     * caller's responsibility to close the streams.
     * 
     * @param is         - source
     * @param os         - destination
     * @param bufferSize - buffer size to use
     * @throws IOException
     */
    public static void copy(InputStream is, OutputStream os, int bufferSize) throws IOException {
        copy(is, os, new byte[bufferSize]);
    }

    /**
     * Replicate the entire contents of the source directory to the target directory
     * 
     * @param sourceLocation - must be a directory and must exist
     * @param targetLocation - if exists, must be a directory. Will be created with
     *                       full paths if does not exist
     * @throws IOException
     */
    public static void copyDirectory(File sourceLocation, File targetLocation) throws IOException {

        if (sourceLocation.isDirectory()) {
            if (!targetLocation.exists()) {
                if (!targetLocation.mkdirs()) {
                    throw new IllegalArgumentException(
                            String.format("Cannot create directory [%s]", targetLocation.getAbsolutePath()));
                }
            } else if (targetLocation.isFile()) {
                throw new IllegalArgumentException(
                        String.format("Target location must be a directory [%s]", targetLocation.getAbsolutePath()));
            }

            String[] children = sourceLocation.list();
            for (String element : children) {
                File child = new File(sourceLocation, element);
                File targetChild = new File(targetLocation, element);
                if (child.isDirectory()) {
                    copyDirectory(child, targetChild);
                } else {
                    copy(child, targetChild);
                }
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("[%s] is not a directory", sourceLocation.getAbsolutePath()));
        }
    }

    /**
     * Create a zip from the contents of a directory.
     * 
     * @param root - the root of the zip contents
     * @param os   - the output stream to create the zip with
     * @throws IOException - if anything goes wrong
     */
    public static void createZip(File root, boolean includeRoot, OutputStream os) throws IOException {
        if (!root.isDirectory()) {
            throw new IllegalArgumentException(String.format("%s is not a directory", root));
        }
        File directory = root.getAbsoluteFile();

        ZipOutputStream zos = new ZipOutputStream(os);
        if (includeRoot) {
            String rootPath = root.getName();
            if (!rootPath.endsWith("/")) {
                rootPath += '/';
            }
            ZipEntry ze = new ZipEntry(rootPath);
            zos.putNextEntry(ze);
            directory = directory.getParentFile();
        }
        for (File file : root.listFiles()) {
            addToZip(directory, file, zos);
        }
        zos.finish();
        zos.flush();
    }

    public static SecureRandom entropy() {
        return ENTROPY.get();
    }

    /**
     * Expand the zip resource into the destination, replacing any ${propName} style
     * properties with the corresponding values in the substitutions map
     * 
     * @param zip           - the zip file to expand
     * @param extensions    - the list of file extensions targeted for property
     *                      substitution
     * @param substitutions - the map of substitutions
     * @param destination   - the destination directory for the expansion
     * 
     * @throws IOException
     * @throws ZipException
     */
    public static void expandAndReplace(File zip, File dest, Map<String, String> substitutions,
                                        Collection<String> extensions) throws ZipException, IOException {
        try (InputStream is = new FileInputStream(zip)) {
            expandAndReplace(is, dest, substitutions, extensions);
        }
    }

    /**
     * 
     * Copy and transform the zip entry to the destination. If the transformation
     * extensions contains the entry's extension, then ${xxx} style parameters are
     * replace with the supplied properties or System.getProperties()
     * 
     * @param dest
     * @param zis
     * @param ze
     * @param extensions
     * @param properties
     * @throws IOException
     */
    public static void expandAndReplace(File dest, ZipInputStream zis, ZipEntry ze, Map<String, String> properties,
                                        Collection<String> extensions) throws IOException {
        File outFile = new File(dest, ze.getName());
        if (ze.isDirectory()) {
            outFile.mkdirs();
        } else {
            transform(properties, extensions, zis, outFile);
        }
    }

    /**
     * Expand the zip resource into the destination, replacing any ${propName} style
     * properties with the corresponding values in the substitutions map
     * 
     * @param is            - the zip input stream to expand
     * @param extensions    - the list of file extensions targeted for property
     *                      substitution
     * @param substitutions - the map of substitutions
     * @param destination   - the destination directory for the expansion
     * 
     * @throws IOException
     * @throws ZipException
     */
    public static void expandAndReplace(InputStream is, File dest, Map<String, String> substitutions,
                                        Collection<String> extensions) throws ZipException, IOException {
        if (!dest.exists() && !dest.mkdir()) {
            throw new IOException(String.format("Cannot create destination directory: %s", dest.getAbsolutePath()));
        }
        ZipInputStream zis = new ZipInputStream(is);
        ZipEntry ze = zis.getNextEntry();
        while (ze != null) {
            expandAndReplace(dest, zis, ze, substitutions, extensions);
            ze = zis.getNextEntry();
        }
    }

    /**
     * Provision the configured process directory from the zip resource
     * 
     * @param zip
     * @param extensions
     * @param map
     * @param destination
     * 
     * @throws IOException
     * @throws ZipException
     */
    public static void explode(File zip, File dest, Map<String, String> map,
                               Collection<String> extensions) throws ZipException, IOException {
        expandAndReplace(zip, dest, map, extensions);
    }

    /**
     * Find the substitution value for the key in the properties. If the supplied
     * properties are null, use the system properties.
     * 
     * @param key
     * @param props
     * @return
     */
    public static String findValue(final String key, final Map<String, String> props) {
        String value;
        // check from the properties
        if (props != null) {
            value = props.get(key.toString());
        } else {
            value = System.getProperty(key);
        }
        if (value == null) {
            // Check for a default value ${key:default}
            int colon = key.indexOf(':');
            if (colon > 0) {
                String realKey = key.substring(0, colon);
                if (props != null) {
                    value = props.get(realKey);
                } else {
                    value = System.getProperty(realKey);
                }

                if (value == null) {
                    // Check for a composite key, "key1,key2"
                    value = resolveCompositeKey(realKey, props);

                    // Not a composite key either, use the specified default
                    if (value == null) {
                        value = key.substring(colon + 1);
                    }
                }
            } else {
                // No default, check for a composite key, "key1,key2"
                value = resolveCompositeKey(key, props);
            }
        }
        return value;
    }

    public static InetAddress getAddress(NetworkInterface iface) {
        return getAddress(iface, true);
    }

    public static InetAddress getAddress(NetworkInterface iface, boolean requireIPV4) {
        InetAddress interfaceAddress = null;
        for (InterfaceAddress address : iface.getInterfaceAddresses()) {
            if (requireIPV4) {
                if (address.getAddress().getAddress().length == 4) {
                    interfaceAddress = address.getAddress();
                    break;
                }
            } else {
                interfaceAddress = address.getAddress();
            }
        }
        if (interfaceAddress == null) {
            throw new IllegalStateException(String.format("Unable ot determine bound %s address for interface '%s'",
                                                          requireIPV4 ? "IPV4" : "IPV4/6", iface));
        }
        return interfaceAddress;
    }

    /**
     * Answer the byte array containing the contents of the file
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public static byte[] getBits(File file) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        InputStream fis = new FileInputStream(file);
        copy(fis, baos);
        return baos.toByteArray();
    }

    /**
     * Answer the string representation of the document
     * 
     * @param openStream - ye olde stream
     * @return the string the stream represents
     * @throws IOException - if we're boned
     */
    public static String getDocument(InputStream is) throws IOException {
        return getDocument(is, new HashMap<String, String>());
    }

    /**
     * Answer the string representation of the document
     * 
     * @param openStream - ye olde stream
     * @param -          the replacement properties for the document
     * @return the string the stream represents
     * @throws IOException - if we're boned
     */
    public static String getDocument(InputStream is, Map<String, String> properties) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        replaceProperties(is, baos, properties);
        return baos.toString();
    }

    /**
     * Answer the string representation of the document
     * 
     * @param openStream - ye olde stream
     * @param -          the replacement properties for the document
     * @return the string the stream represents
     * @throws IOException - if we're boned
     */
    public static String getDocument(InputStream is, Properties properties) throws IOException {
        Map<String, String> props = transform(properties);
        return getDocument(is, props);
    }

    /**
     * Answer the extension of the file
     * 
     * @param file
     * @return
     */
    public static String getExtension(File file) {
        String name = file.getName();
        int index = name.lastIndexOf('.');
        if (index == -1) {
            return "";
        }
        return name.substring(index + 1);
    }

    /**
     * Answer the extension of the file
     * 
     * @param file
     * @return
     */
    public static String getExtension(String file) {
        int index = file.lastIndexOf('.');
        if (index == -1) {
            return "";
        }
        return file.substring(index + 1);
    }

    public static NetworkInterface getInterface(String ifaceName) throws SocketException {
        if (ifaceName == null) {
            NetworkInterface iface = NetworkInterface.getByIndex(1);
            if (iface == null) {
                throw new IllegalArgumentException(
                        "Supplied ANY address for endpoint: %s with no networkInterface defined, cannot find network interface 1 ");
            }
            return iface;
        } else {
            NetworkInterface iface = NetworkInterface.getByName(ifaceName);
            if (iface == null) {
                throw new IllegalArgumentException(String.format("Cannot find network interface: %s ", ifaceName));
            }
            return iface;
        }
    }

    public static HashKey getMemberId(X509Certificate c) {
        X509CertificateHolder holder;
        try {
            holder = new X509CertificateHolder(c.getEncoded());
        } catch (CertificateEncodingException | IOException e) {
            throw new IllegalArgumentException("invalid identity certificate for member: " + c, e);
        }
        Extension ext = holder.getExtension(Extension.subjectKeyIdentifier);

        byte[] id = ASN1OctetString.getInstance(ext.getParsedValue()).getOctets();
        return new HashKey(id);
    }

    /**
     * Answer the extension of the file
     * 
     * @param file
     * @return
     */
    public static String getNameWithoutExtension(File file) {
        String name = file.getName();
        int index = name.lastIndexOf('.');
        if (index == -1) {
            return "";
        }
        return name.substring(0, index);
    }

    /**
     * Answer a property map read from the stream
     * 
     * @param is - the stream containing the property map
     * @return the Map of properties
     * @throws IOException
     */
    public static Map<String, String> getProperties(InputStream is) throws IOException {
        Map<String, String> properties = new HashMap<>();
        Properties props = new Properties();
        props.load(is);
        for (Entry<Object, Object> entry : props.entrySet()) {
            properties.put((String) entry.getKey(), (String) entry.getValue());
        }
        return properties;
    }

    /**
     * Answer the string representation of the inputstream
     * 
     * @param openStream - ye olde stream
     * @return the string the stream represents
     * @throws IOException - if we're boned
     */
    public static String getString(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        copy(is, baos);
        return baos.toString();
    }

    /**
     * Remove and reinitialze the directory. The directory and full paths will be
     * created if it does not exist
     * 
     * @param directory
     */
    public static void initializeDirectory(File directory) {
        clean(directory);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IllegalStateException("Cannot create directory: " + directory);
        }
    }

    /**
     * Remove and reinitialze the directory. The directory and full paths will be
     * created if it does not exist
     * 
     * @param dir
     */
    public static void initializeDirectory(String dir) {
        initializeDirectory(new File(dir));
    }

    /**
     * Answer true if the io exception is a form of a closed connection
     * 
     * @param ioe
     * @return
     */
    public static boolean isClosedConnection(IOException ioe) {
        return ioe instanceof ClosedChannelException || "Broken pipe".equals(ioe.getMessage())
                || "Connection reset by peer".equals(ioe.getMessage());
    }

    public static File relativize(File parent, File child) {
        URI base = parent.toURI();
        URI absolute = child.toURI();
        URI relative = base.relativize(absolute);
        return new File(relative.getPath());
    }

    /**
     * Remove the file. If the file is a directory, the entire contents will be
     * recursively removed.
     * 
     * @param directoryOrFile
     */
    public static void remove(File directoryOrFile) {
        if (directoryOrFile.exists()) {
            if (directoryOrFile.isDirectory()) {
                for (File file : directoryOrFile.listFiles()) {
                    if (file.isDirectory()) {
                        remove(file);
                    } else {
                        if (!file.delete()) {
                            throw new IllegalStateException(String.format("Cannot delete [%s] ", file));
                        }
                    }
                }
            }
            if (!directoryOrFile.delete()) {
                throw new IllegalStateException(String.format("Cannot delete [%s] ", directoryOrFile));
            }
        }
    }

    /**
     * Go through the input stream and replace any occurance of ${p} with the
     * props.get(p) value. If there is no such property p defined, then the ${p}
     * reference will remain unchanged.
     * 
     * If the property reference is of the form ${p:v} and there is no such property
     * p, then the default value v will be returned.
     * 
     * If the property reference is of the form ${p1,p2} or ${p1,p2:v} then the
     * primary and the secondary properties will be tried in turn, before returning
     * either the unchanged input, or the default value.
     * 
     * @param in    - the file with possible ${x} references
     * @param out   - the file output for the transformed input
     * @param props - the source for ${x} property ref values, null means use
     *              System.getProperty()
     * @throws IOException
     */
    public static void replaceProperties(File in, File out, Map<String, String> props) throws IOException {
        try (InputStream is = new FileInputStream(in); OutputStream os = new FileOutputStream(out);) {

            replaceProperties(is, os, props);
        }
    }

    /**
     * Go through the input stream and replace any occurance of ${p} with the
     * props.get(p) value. If there is no such property p defined, then the ${p}
     * reference will remain unchanged.
     * 
     * If the property reference is of the form ${p:v} and there is no such property
     * p, then the default value v will be returned.
     * 
     * If the property reference is of the form ${p1,p2} or ${p1,p2:v} then the
     * primary and the secondary properties will be tried in turn, before returning
     * either the unchanged input, or the default value.
     * 
     * @param in    - the stream with possible ${x} references
     * @param out   - the output for the transformed input
     * @param props - the source for ${x} property ref values, null means use
     *              System.getProperty()
     */
    public static void replaceProperties(final InputStream in, final OutputStream out,
                                         final Map<String, String> props) throws IOException {
        Reader reader = new BufferedReader(new InputStreamReader(in));
        Writer writer = new BufferedWriter(new OutputStreamWriter(out));
        ParsingState state = ParsingState.PASS_THROUGH;

        StringBuffer keyBuffer = null;
        for (int next = reader.read(); next != -1; next = reader.read()) {
            char c = (char) next;
            switch (state) {
            case PASS_THROUGH: {
                if (c == '$') {
                    state = ParsingState.DOLLAR;
                } else {
                    writer.append(c);
                }
                break;
            }
            case DOLLAR: {
                if (c == '{') {
                    state = ParsingState.BRACKET;
                    keyBuffer = new StringBuffer();
                } else if (c == '$') {
                    writer.append('$'); // just saw $$
                } else {
                    state = ParsingState.PASS_THROUGH;
                    writer.append('$');
                    writer.append(c);
                }
                break;
            }
            case BRACKET: {
                if (c == '}') {
                    state = ParsingState.PASS_THROUGH;
                    if (keyBuffer.length() == 0) {
                        writer.append("${}");
                    } else {
                        String value = null;
                        String key = keyBuffer.toString();
                        value = findValue(key, props);

                        if (value != null) {
                            writer.append(value);
                        } else {
                            writer.append("${");
                            writer.append(key);
                            writer.append('}');
                        }
                    }
                    keyBuffer = null;
                } else if (c == '$') {
                    // We're inside of a ${ already, so bail and reset
                    state = ParsingState.DOLLAR;
                    writer.append("${");
                    writer.append(keyBuffer.toString());
                    keyBuffer = null;
                } else {
                    keyBuffer.append(c);
                }
            }
            }
        }
        writer.flush();
    }

    /**
     * Try to resolve a "key" from the provided properties by checking if it is
     * actually a "key1,key2", in which case try first "key1", then "key2". If all
     * fails, return null.
     * 
     * It also accepts "key1," and ",key2".
     * 
     * @param key   the key to resolve
     * @param props the properties to use
     * @return the resolved key or null
     */
    public static String resolveCompositeKey(final String key, Map<String, String> props) {
        String value = null;

        // Look for the comma
        int comma = key.indexOf(',');
        if (comma > -1) {
            // If we have a first part, try resolve it
            if (comma > 0) {
                // Check the first part
                String key1 = key.substring(0, comma);
                if (props != null) {
                    value = props.get(key1);
                } else {
                    value = System.getProperty(key1);
                }
            }
            // Check the second part, if there is one and first lookup failed
            if (value == null && comma < key.length() - 1) {
                String key2 = key.substring(comma + 1);
                if (props != null) {
                    value = props.get(key2);
                } else {
                    value = System.getProperty(key2);
                }
            }
        }
        // Return whatever we've found or null
        return value;
    }

    /**
     * Resolve a resource. First see if the supplied resource is an URL. If so, open
     * it and return the stream. If not, try for a file. If that exists and is not a
     * directory, then return that stream. Finally, look for a classpath resource,
     * relative to the supplied base class. If that exists, open the stream and
     * return that. Otherwise, barf
     * 
     * @param base     - the base class for resolving classpath resources - may be
     *                 null
     * @param resource - the resource to resolve
     * @return the InputStream of the resolved resource
     * @throws IOException - if something gnarly happens, or we can't find your
     *                     resource
     */
    public static InputStream resolveResource(Class<?> base, String resource) throws IOException {
        return resolveResourceURL(base, resource).openStream();
    }

    /**
     * Resolve a resource. Replace any ${foo} style properties in the resource with
     * the supplied properties map. First see if the supplied resource is an URL. If
     * so, open it and return the stream. If not, try for a file. If that exists and
     * is not a directory, then return that stream. Finally, look for a classpath
     * resource, relative to the supplied base class. If that exists, open the
     * stream and return that. Otherwise, barf
     * 
     * @param base       - the base class for resolving classpath resources - may be
     *                   null
     * @param resource   - the resource to resolve
     * @param properties - the properties to replace in the resource stream
     * @return the InputStream of the resolved resource
     * @throws IOException - if something gnarly happens, or we can't find your
     *                     resource
     */
    public static InputStream resolveResource(Class<?> base, String resource,
                                              Map<String, String> properties) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        replaceProperties(resolveResource(base, resource), baos, properties);
        return new ByteArrayInputStream(baos.toByteArray());
    }

    /**
     * Resolve a resource. Replace any ${foo} style properties in the resource with
     * the supplied properties map. First see if the supplied resource is an URL. If
     * so, open it and return the stream. If not, try for a file. If that exists and
     * is not a directory, then return that stream. Finally, look for a classpath
     * resource, relative to the supplied base class. If that exists, open the
     * stream and return that. Otherwise, barf
     * 
     * @param base       - the base class for resolving classpath resources - may be
     *                   null
     * @param resource   - the resource to resolve
     * @param properties - the properties to replace in the resource stream
     * @return the InputStream of the resolved resource
     * @throws IOException - if something gnarly happens, or we can't find your
     *                     resource
     */
    public static InputStream resolveResource(Class<?> base, String resource,
                                              Properties properties) throws IOException {
        return resolveResource(base, resource, transform(properties));
    }

    /**
     * Resolve a resource. First see if the supplied resource is an URL. If so, open
     * it and return the stream. If not, try for a file. If that exists and is not a
     * directory, then return that stream. Finally, look for a classpath resource,
     * relative to the supplied base class. If that exists, open the stream and
     * return that. Otherwise, barf
     * 
     * @param base     - the base class for resolving classpath resources - may be
     *                 null
     * @param resource - the resource to resolve
     * @return the URL of the resolved resource
     * @throws IOException - if something gnarly happens, or we can't find your
     *                     resource
     */
    public static URL resolveResourceURL(Class<?> base, String resource) throws IOException {
        try {
            URL url = new URL(resource);
            return url;
        } catch (MalformedURLException e) {
            Logger.getAnonymousLogger()
                  .fine(String.format("The resource is not a valid URL: %s\n Trying to find a corresponding file",
                                      resource));
        }
        File configFile = new File(resource);
        if (!configFile.exists()) {
            if (base == null) {
                throw new FileNotFoundException(String.format("resource does not exist as a file: %s", resource));
            }
        } else if (configFile.isDirectory()) {
            Logger.getAnonymousLogger()
                  .fine(String.format("resource is a directory: %s\n Trying to find corresponding resource", resource));
        } else {
            return configFile.toURI().toURL();
        }
        return base.getResource(resource);
    }

    /**
     * Transform the contents of the input stream, replacing any ${p} values in the
     * stream with the value in the supplied properties. The transformed contents
     * are placed in the supplied output file.
     * 
     * @param properties
     * @param extensions
     * @param is
     * @param outFile
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void transform(Map<String, String> properties, Collection<String> extensions, InputStream is,
                                 File outFile) throws FileNotFoundException, IOException {
        File parent = outFile.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }

        try (FileOutputStream fos = new FileOutputStream(outFile);) {
            if (extensions.contains(getExtension(outFile.getName()))) {
                replaceProperties(is, fos, properties);
            } else {
                copy(is, fos);
            }
        }
    }

    public static Map<String, String> transform(Properties properties) {
        Map<String, String> props = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            props.put((String) entry.getKey(), (String) entry.getValue());
        }
        return props;
    }

    public static boolean waitForCondition(int maxWaitTime, final int sleepTime, Supplier<Boolean> condition) {
        long endTime = System.currentTimeMillis() + maxWaitTime;
        while (System.currentTimeMillis() < endTime) {
            if (condition.get()) {
                return true;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return false;
    }

    public static boolean waitForCondition(int maxWaitTime, Supplier<Boolean> condition) {
        return waitForCondition(maxWaitTime, 100, condition);
    }
}
