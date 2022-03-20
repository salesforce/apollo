package com.salesforce.apollo.crypto;

import java.security.Provider;
import java.security.Security;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCryptoProvider;

/**
 * The generic sacrifice to the JCE provider gods
 * 
 * @author hal.hildebrand
 *
 */
public class ProviderUtils {

    static final String PROVIDER_NAME_BC     = BouncyCastleProvider.PROVIDER_NAME;
    static final String PROVIDER_NAME_BCJSSE = BouncyCastleJsseProvider.PROVIDER_NAME;

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static Provider            PROVIDER_BC;
    private static Provider            PROVIDER_JSSE;

    static {
        setup();
        PROVIDER_BC = Security.getProvider(PROVIDER_NAME_BC);
        PROVIDER_JSSE = Security.getProvider(PROVIDER_NAME_BCJSSE);
    }

    public static Provider getProviderBC() { 
        if (!initialized.get()) {
            throw new IllegalStateException("Provider has not been initialized");
        }
        return PROVIDER_BC;
    }

    public static Provider getProviderBCJSSE() {
        if (!initialized.get()) {
            throw new IllegalStateException("Provider has not been initialized");
        }
        return PROVIDER_JSSE;
    }

    static Provider createProviderBC() {
        return new BouncyCastleProvider();
    }

    static Provider createProviderBCJSSE() {
        return new BouncyCastleJsseProvider();
    }

    static Provider createProviderBCJSSE(boolean fips) {
        // TODO Use new constructor when available
//        return new BouncyCastleJsseProvider(fips);
        return new BouncyCastleJsseProvider(fips, new JcaTlsCryptoProvider());
    }

    static Provider createProviderBCJSSE(boolean fips, Provider bc) {
        return new BouncyCastleJsseProvider(fips, bc);
    }

    static Provider createProviderBCJSSE(Provider bc) {
        return new BouncyCastleJsseProvider(bc);
    }

    static Provider createProviderBCJSSE(String config) {
        return new BouncyCastleJsseProvider(config);
    }

    static ClassLoader getProviderClassLoaderBC() {
        return BouncyCastleProvider.class.getClassLoader();
    }

    static boolean hasInfo(Provider p, String infoSubstring) {
        return null != p && p.getInfo().contains(infoSubstring);
    }

    static boolean hasInfoBC(String infoSubstring) {
        return hasInfo(getProviderBC(), infoSubstring);
    }

    static boolean hasInfoBCJSSE(String infoSubstring) {
        return hasInfo(getProviderBCJSSE(), infoSubstring);
    }

    static boolean isFipsModeBCJSSE(Provider p) {
        return ((BouncyCastleJsseProvider) p).isFipsMode();
    }

    static boolean isFipsModeBCJSSE(Provider p, boolean fips) {
        return isFipsModeBCJSSE(p) == fips;
    }

    static boolean isProviderBC(Provider p) {
        return p instanceof BouncyCastleProvider;
    }

    static boolean isProviderBCJSSE(Provider p) {
        return p instanceof BouncyCastleJsseProvider;
    }

    static boolean isProviderBCJSSE(Provider p, boolean fips) {
        return isProviderBCJSSE(p) && isFipsModeBCJSSE(p, fips);
    }

    static void removeProviderBC() {
        Security.removeProvider(PROVIDER_NAME_BC);
    }

    static void removeProviderBCJSSE() {
        Security.removeProvider(PROVIDER_NAME_BCJSSE);
    }

    static void setup(boolean bcPriority, boolean bcjssePriority, boolean fips) {
        String javaVersion = System.getProperty("java.version");
        boolean oldJDK = javaVersion.startsWith("1.5") || javaVersion.startsWith("1.6");

        Provider bc = getProviderBC();
        Provider bcjsse = getProviderBCJSSE();

        if (bc == null) {
            bc = createProviderBC();
        } else {
            removeProviderBC();
        }

        if (bcjsse != null) {
            removeProviderBCJSSE();
        }
        if (!isProviderBCJSSE(bcjsse, fips)) {
            bcjsse = oldJDK ? createProviderBCJSSE(fips, bc) : createProviderBCJSSE(fips);
        }

        if (bcPriority) {
            Security.insertProviderAt(bc, 1);
        } else {
            Security.addProvider(bc);
        }

        if (bcjssePriority) {
            Security.insertProviderAt(bcjsse, bcPriority ? 2 : 1);
        } else {
            Security.addProvider(bcjsse);
        }
    }

    static void setupHighPriority(boolean fips) {
        Provider[] providers = Security.getProviders();
        if (providers.length >= 2 && isProviderBC(providers[0]) && isProviderBCJSSE(providers[1], fips)) {
            return;
        }

        setup(true, true, fips);
    }

    static void setupLowPriority(boolean fips) {
        Provider[] providers = Security.getProviders();
        if (providers.length >= 2 && isProviderBC(providers[providers.length - 2]) &&
            isProviderBCJSSE(providers[providers.length - 1], fips)) {
            return;
        }

        setup(false, false, fips);
    }

    private static void setup() {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        setupHighPriority(false);
        for (Provider p : Security.getProviders()) {
            System.out.println(p);
        }
    }
}
