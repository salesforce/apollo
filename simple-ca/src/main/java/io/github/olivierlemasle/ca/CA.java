package io.github.olivierlemasle.ca;

import java.io.File;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.KeyStore;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x500.X500Name;

/**
 * Root of the <i>Certification Authority</i> DSL.
 * <p>
 * All methods in this class are static. They should be statically imported:
 * </p>
 * <p>
 * {@code import static io.github.olivierlemasle.ca.CA.*;}
 * </p>
 */
public final class CA {

    /**
     * Creates a builder object used to create a new {@link CSR} (Certificate Signing Request).
     *
     * @return a builder object
     * @see CSR
     */
    public static CsrBuilder createCsr() {
        return new CsrBuilderImpl();
    }

    /**
     * Creates a builder object used to create a new self-signed root certificate.
     *
     * @param subject
     *            Subject's Distinguished Name
     * @return a builder object
     */
    public static RootCertificateBuilder createSelfSignedCertificate(final DistinguishedName subject) {
        return new RootCertificateBuilderImpl(subject);
    }

    /**
     * Creates a builder object used to build a {@link DistinguishedName}.
     *
     * @return a builder object
     * @see DistinguishedName
     */
    public static DnBuilder dn() {
        return new DnBuilderImpl();
    }

    /**
     * Builds a {@link DistinguishedName} from a {@link String}.
     *
     * @param name
     * @return the {@link DistinguishedName} object
     */
    public static DistinguishedName dn(final String name) {
        return new BcX500NameDnImpl(name);
    }

    /**
     * Builds a {@link DistinguishedName} from a Bouncy Castle {@link X500Name} object.
     *
     * @param name
     * @return the {@link DistinguishedName} object
     */
    public static DistinguishedName dn(final X500Name name) {
        return new BcX500NameDnImpl(name);
    }

    /**
     * Builds a {@link DistinguishedName} from a {@link javax.security.auth.x500.X500Principal}.
     *
     * @param name
     * @return the {@link DistinguishedName} object
     */
    public static DistinguishedName dn(final X500Principal principal) {
        return new BcX500NameDnImpl(principal);
    }

    public static BigInteger generateRandomSerialNumber() {
        return SerialNumberGenerator.instance.generateRandomSerialNumber();
    }

    /**
     * Loads a {@link CSR} (Certificate Signing Request) from a file.
     *
     * @param csrFile
     *            CSR file
     * @return the CSR object
     */
    public static CsrLoader loadCsr(final File csrFile) {
        return new CsrLoaderImpl(csrFile);
    }

    /**
     * Loads a {@link CSR} (Certificate Signing Request) from a file.
     *
     * @param csrFileName
     *            CSR file path
     * @return the CSR object
     */
    public static CsrLoader loadCsr(final String csrFileName) {
        return new CsrLoaderImpl(csrFileName);
    }

    /**
     * Loads an existing {@link RootCertificate} from a {@code PKCS12} keystore.
     *
     * @param keystoreFile
     *            PKCS12 keystore file
     * @param password
     *            password of the keystore
     * @param alias
     *            Root certificate alias in the keystore
     * @return the loaded {@link RootCertificate}
     */
    public static RootCertificate loadRootCertificate(final File keystoreFile, final char[] password,
            final String alias) {
        return RootCertificateLoader.loadRootCertificate(keystoreFile, password, alias);
    }

    public static RootCertificate loadRootCertificate(final InputStream keystore, final char[] password,
            final String alias) {
        return RootCertificateLoader.loadRootCertificate(keystore, password, alias);
    }

    /**
     * Loads an existing {@link RootCertificate} from a {@code PKCS12} keystore.
     *
     * @param keystoreFile
     *            PKCS12 keystore, already "loaded"
     * @param alias
     *            Root certificate alias in the keystore
     * @return the loaded {@link RootCertificate}
     */
    public static RootCertificate loadRootCertificate(final KeyStore keystore, final String alias) {
        return RootCertificateLoader.loadRootCertificate(keystore, alias);
    }

    /**
     * Loads an existing {@link RootCertificate} from a {@code PKCS12} keystore.
     *
     * @param keystorePath
     *            path of the PKCS12 keystore
     * @param password
     *            password of the keystore
     * @param alias
     *            Root certificate alias in the keystore
     * @return the loaded {@link RootCertificate}
     */
    public static RootCertificate loadRootCertificate(final String keystorePath, final char[] password,
            final String alias) {
        return RootCertificateLoader.loadRootCertificate(keystorePath, password, alias);
    }

    public static KeyStoreReader readKeystore() {
        return new KeyStoreReaderImpl();
    }

    /**
     * No instantiation
     */
    private CA() {}

}
