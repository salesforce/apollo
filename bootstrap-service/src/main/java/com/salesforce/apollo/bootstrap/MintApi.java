/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap;

import static com.salesforce.apollo.boostrap.schema.Tables.ASSIGNED_IDS;
import static com.salesforce.apollo.boostrap.schema.Tables.MEMBERS;
import static com.salesforce.apollo.boostrap.schema.Tables.SETTINGS;
import static io.github.olivierlemasle.ca.CA.dn;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.Connection;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.Timed;

import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.DistinguishedName;
import io.github.olivierlemasle.ca.DnBuilder;
import io.github.olivierlemasle.ca.RootCertificate;
import io.github.olivierlemasle.ca.Signer.SignerWithSerial;
import io.github.olivierlemasle.ca.SignerImpl;
import io.github.olivierlemasle.ca.ext.CertExtension;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hhildebrand
 */
@Path("/api/cnc")
public class MintApi {
    public static class MintRequest {
        private int    avalanchePort;
        private int    firefliesPort;
        private int    ghostPort;
        private String hostname;
        private String publicKey;
        private String signature;

        public MintRequest() {
        }

        public MintRequest(String hostname, int firefliesPort, int ghostPort, int avalanchePort, String publicKey,
                String signature) {
            this.hostname = hostname;
            this.firefliesPort = firefliesPort;
            this.ghostPort = ghostPort;
            this.avalanchePort = avalanchePort;
            this.publicKey = publicKey;
            this.signature = signature;
        }

        public int getAvalanchePort() {
            return avalanchePort;
        }

        public int getFirefliesPort() {
            return firefliesPort;
        }

        public int getGhostPort() {
            return ghostPort;
        }

        public String getHostname() {
            return hostname;
        }

        public String getPublicKey() {
            return publicKey;
        }

        public String getSignature() {
            return signature;
        }

    }

    public static class MintResult {
        private String       encodedCA;
        private String       encodedIdentity;
        private List<String> encodedSeeds;

        public MintResult() {
        };

        public MintResult(String encodedCA, String encodedIdentity, List<String> encodedSeeds) {
            this.encodedCA = encodedCA;
            this.encodedIdentity = encodedIdentity;
            this.encodedSeeds = encodedSeeds;
        }

        public String getEncodedCA() {
            return encodedCA;
        }

        public String getEncodedIdentity() {
            return encodedIdentity;
        }

        public List<String> getEncodedSeeds() {
            return encodedSeeds;
        }
    }

    public static final String   PORT_TEMPLATE       = "%s:%s:%s";
    public static final String   SHA_256             = "sha-256";
    public static final String   SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final Decoder DECODER             = Base64.getUrlDecoder();
    private static final Encoder ENCODER             = Base64.getUrlEncoder().withoutPadding();

    public static void loadSchema(DSLContext context) {
        Connection connection = context.configuration().connectionProvider().acquire();
        Database database;
        try {
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        } catch (DatabaseException e) {
            throw new IllegalStateException("Unable to get DB factory for: " + connection, e);
        }
        database.setLiquibaseSchemaName("public");
        try (Liquibase liquibase = new Liquibase("bootstrap-schema.yml",
                new ClassLoaderResourceAccessor(MintApi.class.getClassLoader()), database)) {
            liquibase.update((Contexts) null);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load schema", e);
        }

        // liquibase = new Liquibase("functions.yml", new
        // ClassLoaderResourceAccessor(MintApi.class.getClassLoader()),
        // database);
        // try {
        // liquibase.update((Contexts)null);
        // } catch (LiquibaseException e) {
        // throw new IllegalStateException("Cannot load functions", e);
        // }

//        try {
//            connection.commit();
//        } catch (SQLException e) {
//            throw new IllegalStateException("Cannot create trigger", e);
//        }
    }

    public static RootCertificate mint(DistinguishedName dn, int cardinality, double probabilityByzantine,
                                       double faultTolerance) {

        DnBuilder builder = dn();
        decodeDN(dn.getName()).entrySet().stream().filter(e -> !e.getKey().equals("O")).forEach(e -> {
            addToBuilder(builder, e);
        });

        builder.setOrganizationName(String.format("%s:%.6f:%.6f", cardinality, faultTolerance, probabilityByzantine));

        return CA.createSelfSignedCertificate(builder.build()).build();
    }

    private static void addToBuilder(DnBuilder builder, Entry<String, String> e) {
        switch (e.getKey()) {
        case "CN":
            builder.setCommonName(e.getValue());
            break;
        case "OU":
            builder.setOu(e.getValue());
            break;
        case "C":
            builder.setCountryName(e.getValue());
            break;
        case "ST":
            builder.setStreet(e.getValue());
            break;
        case "L":
            builder.setLocalityName(e.getValue());
            break;
        default:
            throw new IllegalArgumentException("Unknown component: " + e.getKey());
        }
    }

    private static Map<String, String> decodeDN(String dn) {
        LdapName ldapDN;
        try {
            ldapDN = new LdapName(dn);
        } catch (InvalidNameException e) {
            throw new IllegalArgumentException("invalid DN: " + dn, e);
        }
        Map<String, String> decoded = new HashMap<>();
        ldapDN.getRdns().forEach(rdn -> decoded.put(rdn.getType(), (String) rdn.getValue()));
        return decoded;
    }

    private final DSLContext      context;
    private final String          country          = "US";
    private final String          organization     = "World Company";
    private final String          orginazationUnit = "IT dep";
    private final RootCertificate root;
    private int                   seedCount;
    private final String          state            = "CA";
    private final SecureRandom    entropy          = new SecureRandom();

    public MintApi(RootCertificate root, DSLContext context, int seedCount) {
        this.root = root;
        this.context = context;
        this.seedCount = seedCount;
    }

    @POST()
    @Path("mint")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    /**
     * @return the MintResult
     */
    public MintResult mint(MintRequest mintRequest) {
        if (!validatePorts(mintRequest.getHostname(), mintRequest.getFirefliesPort(), mintRequest.getGhostPort(),
                           mintRequest.getAvalanchePort())) {
            throw new WebApplicationException("Invalid host/ports", Status.BAD_REQUEST);
        }
        DistinguishedName dn = dn().setCn(mintRequest.hostname)
                                   .setL(String.format(PORT_TEMPLATE, mintRequest.firefliesPort, mintRequest.ghostPort,
                                                       mintRequest.avalanchePort))
                                   .setO(organization)
                                   .setOu(orginazationUnit)
                                   .setSt(state)
                                   .setC(country)
                                   .build();
        X509EncodedKeySpec spec = new X509EncodedKeySpec(DECODER.decode(mintRequest.publicKey));
        KeyFactory kf;
        try {
            kf = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("WTF?");
        }
        PublicKey publicKey;
        try {
            publicKey = kf.generatePublic(spec);
        } catch (InvalidKeySpecException e) {
            throw new WebApplicationException("Invalid public key", Status.BAD_REQUEST);
        }

        if (!verify(publicKey.getEncoded(), forVerification(publicKey), DECODER.decode(mintRequest.getSignature()))) {
            throw new WebApplicationException("Signature on public key does not verify", Status.BAD_REQUEST);
        }

        return context.transactionResult(config -> {
            DSLContext create = DSL.using(config);
            byte[] id = create.select(ASSIGNED_IDS.ID)
                              .from(ASSIGNED_IDS)
                              .where(ASSIGNED_IDS.VERSION.isNull())
                              .orderBy(DSL.rand())
                              .limit(1)
                              .fetchOne()
                              .value1();
            int caVersion = create.select(DSL.max(SETTINGS.VERSION)).from(SETTINGS).fetchOne().value1();
            Record1<Integer> fetch = create.select(DSL.max(MEMBERS.VERSION))
                                           .from(MEMBERS)
                                           .where(MEMBERS.ID.eq(id))
                                           .fetchOne();
            int previousVersion = fetch.value1() == null ? -1 : fetch.value1();
            int thisVersion = previousVersion + 1;
            create.insertInto(MEMBERS).set(MEMBERS.ID, id).set(MEMBERS.VERSION, thisVersion).execute();
            create.mergeInto(ASSIGNED_IDS)
                  .columns(ASSIGNED_IDS.ID, ASSIGNED_IDS.VERSION)
                  .values(id, thisVersion)
                  .execute();
            Certificate certificate = mintNode(publicKey, id, dn);
            byte[] derEncoded = certificate.getX509Certificate().getEncoded();
            create.update(MEMBERS)
                  .set(MEMBERS.SIGNING_CA, caVersion)
                  .set(MEMBERS.CERTIFICATE, derEncoded)
                  .set(MEMBERS.CERTIFICATE_HASH, hashOf(derEncoded))
                  .set(MEMBERS.HOST, mintRequest.getHostname())
                  .set(MEMBERS.FIREFLIES_PORT, mintRequest.getFirefliesPort())
                  .set(MEMBERS.GHOST_PORT, mintRequest.getGhostPort())
                  .set(MEMBERS.AVALANCHE_PORT, mintRequest.getAvalanchePort())
                  .where(MEMBERS.ID.eq(id))
                  .and(MEMBERS.VERSION.eq(thisVersion))
                  .execute();
            return new MintResult(
                    ENCODER.encodeToString(create.select(SETTINGS.CA_CERTIFICATE)
                                                 .from(SETTINGS)
                                                 .where(SETTINGS.VERSION.eq(caVersion))
                                                 .fetchOne()
                                                 .value1()),
                    ENCODER.encodeToString(derEncoded), chooseSeeds(id, create));
        });
    }

    @POST()
    @Path("recycle")
    @Timed
    public void recycle(UUID id) {

    }

    Signature forVerification(PublicKey publicKey) {
        Signature signature;
        try {
            signature = Signature.getInstance(SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initVerify(publicKey);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid public key", e);
        }
        return signature;
    }

    private List<String> chooseSeeds(byte[] id, DSLContext create) {
        List<String> seeds = create.select(MEMBERS.CERTIFICATE)
                                   .from(MEMBERS)
                                   .where(MEMBERS.ID.in(create.select(ASSIGNED_IDS.ID)
                                                              .from(ASSIGNED_IDS)
                                                              .where(ASSIGNED_IDS.VERSION.isNotNull())
                                                              .and(ASSIGNED_IDS.ID.ne(id))
                                                              .orderBy(DSL.rand())))
                                   .limit(seedCount)
                                   .stream()
                                   .map(r -> r.value1())
                                   .map(encoded -> ENCODER.encodeToString(encoded))
                                   .collect(Collectors.toList());
        return seeds;
    }

    private byte[] hashOf(byte[] content) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("No hash algorithm found: " + SHA_256);
        }
        md.update(content);
        return md.digest();
    }

    private Certificate mintNode(PublicKey publicKey, byte[] id, DistinguishedName dn) {

        ByteBuffer cereal = ByteBuffer.wrap(new byte[16]);
        cereal.putLong(entropy.nextLong());
        cereal.putLong(entropy.nextLong());

        SignerWithSerial signer = new SignerImpl(
                new KeyPair(root.getX509Certificate().getPublicKey(),
                        root.getPrivateKey()),
                dn(root.getX509Certificate().getSubjectX500Principal().getName()), publicKey, dn)
                                                                                                 .setSerialNumber(new BigInteger(
                                                                                                         cereal.array()));

        ByteBuffer idBuff = ByteBuffer.wrap(id);
        signer.addExtension(new CertExtension(Extension.subjectKeyIdentifier, false,
                new SubjectKeyIdentifier(idBuff.array())));
        signer.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        return signer.sign(false);
    }

    private boolean validatePorts(String hostString, int fPort, int gPort, int aPort) {
        // no op for now. should be checked for validity in realsies - HSH
        return true;
    }

    private boolean verify(byte[] content, Signature s, byte[] signature) {
        try {
            s.update(content);
            return s.verify(signature);
        } catch (SignatureException e) {
            LoggerFactory.getLogger(getClass()).debug("invalid signature", e);
            return false;
        }
    }

}
