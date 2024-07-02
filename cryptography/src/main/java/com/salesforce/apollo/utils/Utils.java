package com.salesforce.apollo.utils;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.BcX500NameDnImpl;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.cryptography.cert.Certificates;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;
import static java.util.stream.Collectors.toList;

/**
 * @author hal.hildebrand
 **/
public class Utils {

    private static final Collector<?, ?, ?> SHUFFLER = Collectors.collectingAndThen(
    Collectors.toCollection(ArrayList::new), list -> {
        Collections.shuffle(list);
        return list;
    });

    /**
     * Copy the contents of the input stream to the output stream. It is the caller's responsibility to close the
     * streams.
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
     * Remove the file. If the file is a directory, the entire contents will be recursively removed.
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

    /**
     * Copy the contents of the input stream into the output stream using the default buffer size
     *
     * @param is
     * @param os
     * @throws IOException
     */
    public static void copy(InputStream is, OutputStream os) throws IOException {
        copy(is, os, 16 * 1024);
    }

    /**
     * Copy the contents of the input stream to the output stream. It is the caller's responsibility to close the
     * streams.
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

    public static boolean waitForCondition(int maxWaitTime, final int sleepTime, Supplier<Boolean> condition) {
        long endTime = System.currentTimeMillis() + maxWaitTime;
        while (System.currentTimeMillis() <= endTime) {
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

    public static CertificateWithPrivateKey getMember(Digest id) {
        KeyPair keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        var notBefore = Instant.now();
        var notAfter = Instant.now().plusSeconds(10_000);
        String localhost = InetAddress.getLoopbackAddress().getHostName();
        X509Certificate generated = Certificates.selfSign(false,
                                                          encode(id, localhost, allocatePort(), keyPair.getPublic()),
                                                          keyPair, notBefore, notAfter, Collections.emptyList());
        return new CertificateWithPrivateKey(generated, keyPair.getPrivate());
    }

    public static CertificateWithPrivateKey getMember(int index) {
        byte[] hash = new byte[32];
        hash[0] = (byte) index;
        return getMember(new Digest(DigestAlgorithm.DEFAULT, hash));
    }

    public static BcX500NameDnImpl encode(Digest digest, String host, int port, PublicKey signingKey) {
        return new BcX500NameDnImpl(
        String.format("CN=%s, L=%s, UID=%s, DC=%s", host, port, qb64(digest), qb64(signingKey)));
    }

    @SuppressWarnings("unchecked")
    public static <T> Collector<T, ?, List<T>> toShuffledList() {
        return (Collector<T, ?, List<T>>) SHUFFLER;
    }

    /**
     * Find a free port for any local address
     *
     * @return the port number or -1 if none available
     */
    public static int allocatePort() {
        return allocatePort(null);
    }

    public static <T> Collector<T, ?, Stream<T>> toEagerShuffledStream() {
        return Collectors.collectingAndThen(toList(), list -> {
            Collections.shuffle(list);
            return list.stream();
        });
    }

    /**
     * Find a free port on the interface with the given address
     *
     * @return the port number or -1 if none available
     */
    public static int allocatePort(InetAddress host) {
        InetAddress address = host == null ? InetAddress.getLoopbackAddress() : host;

        try (ServerSocket socket = new ServerSocket(0, 0, address)) {
            socket.setReuseAddress(true);
            var localPort = socket.getLocalPort();
            socket.close();
            return localPort;
        } catch (IOException e) {
            return -1;
        }
    }

    public static <T> Callable<T> wrapped(Callable<T> c, Logger log) {
        return () -> {
            try {
                return c.call();
            } catch (Exception e) {
                log.error("Error in call", e);
                return null;
            }
        };
    }

    public static <T> Consumer<T> wrapped(Consumer<T> c, Logger log) {
        return t -> {
            try {
                c.accept(t);
            } catch (Exception e) {
                log.error("Error in call", e);
            }
        };
    }

    public static Runnable wrapped(Runnable r, Logger log) {
        return () -> {
            try {
                r.run();
            } catch (Throwable e) {
                log.error("Error in execution", e);
            }
        };
    }
}
