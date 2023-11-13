package com.salesforce.apollo.utils;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Base64;

import javax.crypto.KeyGenerator;

import org.junit.jupiter.api.Test;

/*
This class is observing, effect of below different parameters in different combinations. 

1. algorithm choosen: System defaults, SHA1PRNG and Windows-PRNG. Didn't play around with NativePRNGBlocking and NonBlocking variety, as they are variant of NativePRNG (default on Unix like).
2. Between Unix like systems and Windows Operating Systems
3. Configuration of securerandom.source value in java.security file
4. Initial self-seeded or explicit sedded

Run below program a few times, and observe Randomizer bytes and Base64 value of Symmetric Key. If SecureRandom is insecure, output would be predictable across different run.

Conclusion:

SHA1PRNG algorithm, when explicitly seeded, pseudo-random output generated would be directly proportional to the entropy source provided. We always want entropy source to be provided by underlying OS. 
Thus, for Unix/Linux: default implementation selected would be NativePRNG and allow it to be self seeded. However, for windows default implementation selected would be SHA1PRNG, which with explicit seeding might reduce entropy. Thus, always choose an algorithm when running on Windows.

Unfortunately, explicitly selecting a windows specific algorithm on windows and letting JDK choose algorithm for Unix like systems, makes it less portable across Unix like systems and Windows. But beter safe than sorry.

Most secure and portable way of using SecureRandom in Unix like OS is:

SecureRandom secRan = new SecureRandom() ;
byte[] b = new byte[NO_OF_RANDOM_BYTES] ;
secRan.nextBytes(b);

In Windows:

secRan = SecureRandom.getInstance("Windows-PRNG") ;
byte[] b = new byte[NO_OF_RANDOM_BYTES] ;
secRan.nextBytes(b);

*/
public class CheckSecureRandomConfig {

    public static final int NO_OF_RANDOM_BYTES = 20;
    public static final int KEY_SIZE           = 256;

    @Test
    public void testConfig() {

        SecureRandom secRan = null;

        /*
         * 1. SecureRandom Configuration: - Default provider - Default implementation -
         * Default securerandom.source property - self-seeding
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. Most prefered option. - Provider: SUN - Algorithm:
         * NativePRNG - securerandom.source: file:/dev/random - seeding: self-seeding -
         * On Windows systems: Produces cryptographically secure random pseudo-data. Not
         * the most prefered option, since it defaults to SHA1PRNG. - Provider: SUN -
         * Algorithm: SHA1PRNG - securerandom.source: file:/dev/random - seeding:
         * self-seeding
         * 
         */
        try {
            secRan = new SecureRandom(); // SecureRandom default constructor will always use default provider and
                                         // algorithm.
            printRandomnessParameters(secRan, "1. Using default algorithm, securerandom.source and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 2. SecureRandom Configuration: - Default provider - Default implementation -
         * Default securerandom.source property - explicit-seeding
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. - Provider: SUN - Algorithm: NativePRNG -
         * securerandom.source: file:/dev/random - seeding: explicit seeding - On
         * Windows systems: DANGEROUS. SHA1PRNG algorithm is directly seeded with user
         * supplied source of entropy. This makes output directly proportional to source
         * of randomness. - Provider: SUN - Algorithm: SHA1PRNG - securerandom.source:
         * file:/dev/random - seeding: explicit seeding
         * 
         */
        try {
            secRan = new SecureRandom();
            secRan.setSeed(12345); // To see effects of using a low entropy source on pseudo random data generated
                                   // and Key Generation, used such a static value.
            printRandomnessParameters(secRan, "2. Using default algorithm, securerandom.source and explicit seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 3. SecureRandom Configuration: - Default provider - Algorithm: SHA1PRNG -
         * Default securerandom.source property: file:/dev/random - self-seeding
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. SHA1PRNG is seeded using file:/dev/random - Provider: SUN
         * - Algorithm: SHA1PRNG - securerandom.source: file:/dev/random - seeding:
         * self-seeding - On Windows systems: Produces cryptographically secure random
         * pseudo-data. SHA1PRNG is seeded using file:/dev/random - Provider: SUN -
         * Algorithm: SHA1PRNG - securerandom.source: file:/dev/random - seeding:
         * self-seeding
         * 
         */
        try {
            secRan = SecureRandom.getInstance("SHA1PRNG");
            printRandomnessParameters(secRan,
                                      "3. Using SHA1PRNG algorithm, default value of securerandom.source and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 4. SecureRandom Configuration: - Default provider - Default implementation:
         * SHA1PRNG - Default securerandom.source property: file:/dev/random - seeding:
         * explicit
         * 
         * Observations: - On Unix like systems: DANGEROUS. SHA1PRNG implementation is
         * directly seeded with the explicitly configured source of randomness. This
         * makes output directly proportional to source of randomness. - Provider: SUN -
         * Algorithm: SHA1PRNG - securerandom.source: file:/dev/random - seeding:
         * explicit - On Windows systems: DANGEROUS. SHA1PRNG implementation is directly
         * seeded with the explicitly configured source of randomness. This makes output
         * directly proportional to source of randomness. - Provider: SUN - Algorithm:
         * SHA1PRNG - securerandom.source: file:/dev/random - seeding: explicit
         * 
         * 
         * 
         */
        try {
            secRan = SecureRandom.getInstance("SHA1PRNG");
            secRan.setSeed(12345);
            printRandomnessParameters(secRan,
                                      "4. Using SHA1PRNG algorithm, default value of securerandom.source and explicit seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 5. SecureRandom Configuration: - Default provider - Default implementation:
         * SHA1PRNG - Changed securerandom.source property:
         * file://something-other-than-dev-random - self-seeding
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. - Provider: SUN - Algorithm: SHA1PRNG -
         * securerandom.source: file://something-other-than-dev-random - seeding:
         * self-seeding - On Windows systems: Produces cryptographically secure random
         * pseudo-data. - Provider: SUN - Algorithm: SHA1PRNG - securerandom.source:
         * file://something-other-than-dev-random - seeding: self-seeding
         * 
         */
        try {
            secRan = SecureRandom.getInstance("SHA1PRNG");
            Security.setProperty("securerandom.source", "file:/something-other-than-dev-random");
            printRandomnessParameters(secRan,
                                      "5. Using SHA1PRNG algorithm, changing securerandom.source value and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 6. SecureRandom Configuration: - Default provider - Default implementation:
         * SHA1PRNG - Default securerandom.source property:
         * file:/something-other-than-dev-random - seeding: explicit
         * 
         * Observations: - On Unix like systems: DANGEROUS. SHA1PRNG implementation is
         * directly seeded with the explicitly configured source of randomness. This
         * makes output directly proportional to source of randomness. - Provider: SUN -
         * Algorithm: SHA1PRNG - securerandom.source:
         * file:/something-other-than-dev-random - seeding: explicit - On Windows
         * systems: DANGEROUS. SHA1PRNG implementation is directly seeded with the
         * explicitly configured source of randomness. This makes output directly
         * proportional to source of randomness. - Provider: SUN - Algorithm: SHA1PRNG -
         * securerandom.source: file:/something-other-than-dev-random - seeding:
         * explicit
         */
        try {
            secRan = SecureRandom.getInstance("SHA1PRNG");
            secRan.setSeed(12345);
            printRandomnessParameters(secRan,
                                      "6. Using SHA1PRNG algorithm, changing securerandom.source value and explicit seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 7. SecureRandom Configuration: - Default provider - Default implementation -
         * securerandom.source property: file:/something-other-than-dev-random -
         * seeding: self
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. - Provider: SUN - Algorithm: NativePRNG -
         * securerandom.source: file:/something-other-than-dev-random - seeding: self -
         * On Windows systems: Produces cryptographically secure random pseudo-data. -
         * Provider: SUN - Algorithm: SHA1PRNG - securerandom.source:
         * file:/something-other-than-dev-random - seeding: self
         * 
         */
        try {
            secRan = new SecureRandom();
            printRandomnessParameters(secRan,
                                      "7. Using default algorithm, changing securerandom.source value and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 8. SecureRandom Configuration: - Default provider - Default implementation -
         * securerandom.source property: file:/something-other-than-dev-random -
         * self-seeding
         * 
         * Observations: - On Unix like systems: Produces cryptographically secure
         * random pseudo-data. - Provider: SUN - Algorithm: NativePRNG -
         * securerandom.source: file:/something-other-than-dev-random - seeding:
         * explicit - On Windows systems: DANGEROUS. SHA1PRNG implementation is directly
         * seeded with the explicitly configured source of randomness. This makes output
         * directly proportional to source of randomness. - Provider: SUN - Algorithm:
         * SHA1PRNG - securerandom.source: file:/something-other-than-dev-random -
         * seeding: explicit
         * 
         */
        try {
            secRan = new SecureRandom();
            secRan.setSeed(12345);
            printRandomnessParameters(secRan,
                                      "8. Using default algorithm, changing securerandom.source value and explicit-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + secRan.getAlgorithm() + " is not available on this system");
        }

        /*
         * 9. SecureRandom Configuration: - Default provider - implementation:
         * Windows-PRNG - securerandom.source property:
         * file:/something-other-than-dev-random - self-seeding
         * 
         * Observations: - On Unix like systems: Throws NoSuchAlgorithmException
         * 
         * - On Windows systems: Produces cryptographically secure random pseudo-data. -
         * Provider: SunMSCAPI - Algorithm: Windows-PRNG - securerandom.source:
         * file:/something-other-than-dev-random - seeding: self
         * 
         * 
         */
        try {
            secRan = SecureRandom.getInstance("Windows-PRNG");
            printRandomnessParameters(secRan,
                                      "9. Using Windows-PRNG algorithm, changing securerandom.source value and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + "Windows-PRNG" + " is not available on this system");
        }

        /*
         * 10. SecureRandom Configuration: - Default provider - implementation:
         * Windows-PRNG - securerandom.source property:
         * file:/something-other-than-dev-random - self-seeding
         * 
         * Observations: - On Unix like systems: Throws NoSuchAlgorithmException
         * 
         * - On Windows systems: Produces cryptographically secure random pseudo-data. -
         * Provider: SunMSCAPI - Algorithm: Windows-PRNG - securerandom.source:
         * file:/something-other-than-dev-random - seeding: explicit
         * 
         * 
         */
        try {
            secRan = SecureRandom.getInstance("Windows-PRNG");
            secRan.setSeed(12345);
            printRandomnessParameters(secRan,
                                      "10. Using Windows-PRNG algorithm, changing securerandom.source value and explicit seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + "Windows-PRNG" + " is not available on this system");
        }

        /*
         * 11. SecureRandom Configuration: - Default provider - implementation:
         * Windows-PRNG - Default securerandom.source property: file:/dev/random -
         * self-seeding
         * 
         * Observations: - On Unix like systems: Throws NoSuchAlgorithmException
         * 
         * - On Windows systems: Produces cryptographically secure random pseudo-data. -
         * Provider: SunMSCAPI - Algorithm: Windows-PRNG - securerandom.source:
         * file:/dev/random - seeding: self
         * 
         * 
         */
        try {
            secRan = SecureRandom.getInstance("Windows-PRNG");
            Security.setProperty("securerandom.source", "file:/dev/random");
            printRandomnessParameters(secRan,
                                      "11. Using Windows-PRNG algorithm, default securerandom.source value and self-seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + "Windows-PRNG" + " is not available on this system");
        }

        /*
         * 12. SecureRandom Configuration: - Default provider - implementation:
         * Windows-PRNG - Default securerandom.source property: file:/dev/random -
         * seeding: explicit
         * 
         * Observations: - On Unix like systems: Throws NoSuchAlgorithmException
         * 
         * - On Windows systems: Produces cryptographically secure random pseudo-data. -
         * Provider: SunMSCAPI - Algorithm: Windows-PRNG - securerandom.source:
         * file:/dev/random - seeding: explicit
         * 
         */
        try {
            secRan = SecureRandom.getInstance("Windows-PRNG");
            secRan.setSeed(12345);
            printRandomnessParameters(secRan,
                                      "12. Using Windows-PRNG algorithm, default securerandom.source value and explicit seeding ");
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Algorithm " + "Windows-PRNG" + " is not available on this system");
        }
    }

    /*
     * This method, prints the value of various parameters used like provider,
     * CSPRNG algorithm, randomness source. It also prints effect of using secRan on
     * a Symmetric Key to see if its truly random.
     */
    protected static void printRandomnessParameters(SecureRandom secRan,
                                                    String prefixMessage) throws NoSuchAlgorithmException {
        System.out.println(prefixMessage + " : " + "SecureRandom() Provider : " + secRan.getProvider().getName()
        + " Algorithm " + secRan.getAlgorithm() + " randomness source " + Security.getProperty("securerandom.source"));

        byte[] b = new byte[NO_OF_RANDOM_BYTES];
        secRan.nextBytes(b);

        System.out.println("Randomizer bytes = " + Base64.getEncoder().encodeToString(b));

        KeyGenerator symmKeyGen = KeyGenerator.getInstance("AES");
        symmKeyGen.init(KEY_SIZE, secRan);

        System.out.println("Key generated = "
        + Base64.getEncoder().encodeToString(symmKeyGen.generateKey().getEncoded()));

        System.out.println("==========================");

    }
}
