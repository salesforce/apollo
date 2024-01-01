/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demesnes.isolate;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.demesne.proto.ViewChange;
import com.salesforce.apollo.stereotomy.event.proto.*;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.model.demesnes.Demesne;
import com.salesforce.apollo.model.demesnes.DemesneImpl;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.LogManager;

/**
 * GraalVM Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 */
public class DemesneIsolate {
    private static final AtomicReference<DemesneImpl> demesne = new AtomicReference<>();
    private static final Lock                         lock    = new ReentrantLock();
    private static final Logger                       log     = LoggerFactory.getLogger(DemesneIsolate.class);

    static {
        System.setProperty(".level", "FINEST");
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_createIsolate", builtin = CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_active")
    private static boolean active(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId)
    throws GeneralSecurityException {
        final Demesne d = demesne.get();
        return d == null ? false : d.active();
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_commit")
    private static void commit(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                               JByteArray eventCoordinates, int eventCoordinatesLen) {
        final Demesne d = demesne.get();
        if (d != null) {
            var coordBuff = CTypeConversion.asByteBuffer(
            jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, eventCoordinates, false), eventCoordinatesLen);
            EventCoords coords;
            try {
                coords = EventCoords.parseFrom(coordBuff);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse event coordinates", e);
                throw new IllegalStateException("Unable to parse event coordinates", e);
            }
            d.commit(coords);
        }
    }

    private static void configureLogging(final DemesneParameters parameters) {
        final var loggingConfig = parameters.getLoggingConfig();
        File configFile = new File(loggingConfig);
        if (!loggingConfig.isBlank() || configFile.exists()) {
            System.err.println("Using logging configuration: " + configFile.getAbsolutePath());
            try {
                final var config = new FileInputStream(configFile);
                LogManager.getLogManager().updateConfiguration(config, s -> (o, n) -> n);
            } catch (FileNotFoundException e) {
                System.err.println("No logging configuration found: " + configFile.getAbsolutePath());
                System.setProperty(".level", "INFO");
            } catch (SecurityException | IOException e) {
                System.setProperty(".level", "FINEST");
                log.error("Unable to initialize logging configuration", e);
                throw new IllegalStateException("Unable to initialize logging configuration", e);
            }
        } else {
            System.err.println("No logging configuration");
            System.setProperty(".level", "INFO");
        }

        log.trace("Testing");
    }

    private static EventCoordinates coords(byte[] coords) {
        try {
            return EventCoordinates.from(EventCoords.parseFrom(coords));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid digest: {}", coords);
            throw new IllegalArgumentException("Invalid digest: " + coords, e);
        }
    }

    private static Digest digest(byte[] digest) {
        try {
            return Digest.from(Digeste.parseFrom(digest));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid digest: {}", digest);
            throw new IllegalArgumentException("Invalid digest: " + digest, e);
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_inception")
    private static JByteArray inception(JNIEnvironment jniEnv, JClass clazz,
                                        @CEntryPoint.IsolateThreadContext long isolateId, JByteArray ident,
                                        int identLen, JByteArray spec, int specLen) {
        final Demesne d = demesne.get();
        if (d != null) {
            var identBuff = CTypeConversion.asByteBuffer(
            jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, ident, false), identLen);
            var specBuff = CTypeConversion.asByteBuffer(
            jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, spec, false), specLen);
            Ident identifier;
            try {
                identifier = Ident.parseFrom(identBuff);
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse inception specification", e);
                return jniEnv.getFunctions().getNewByteArray().call(jniEnv, 0);
            }
            IdentifierSpecification.Builder<SelfAddressingIdentifier> specification;
            try {
                specification = IdentifierSpecification.Builder.from(IdentifierSpec.parseFrom(specBuff));
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to parse inception specification", e);
                return jniEnv.getFunctions().getNewByteArray().call(jniEnv, 0);
            }
            final var inception = d.inception(identifier, specification);
            log.info("Inception: {}", inception);
            final var bytes = inception.getBytes();
            final var returnArray = jniEnv.getFunctions().getNewByteArray().call(jniEnv, bytes.length);
            final var buf = CTypeConversion.toCBytes(bytes);
            jniEnv.getFunctions().getSetByteArrayRegion().call(jniEnv, returnArray, 0, bytes.length, buf.get());
            return returnArray;
        }
        log.error("No Demesne!");
        return jniEnv.getFunctions().getNewByteArray().call(jniEnv, 0);
    }

    private static void launch(JNIEnvironment jniEnv, ByteBuffer data, JClass clazz)
    throws GeneralSecurityException, IOException {
        final var parameters = DemesneParameters.parseFrom(data);
        configureLogging(parameters);
        launch(jniEnv, parameters, clazz);
    }

    private static void launch(JNIEnvironment jniEnv, DemesneParameters parameters, JClass clazz)
    throws GeneralSecurityException, IOException {
        try {
            lock.lock();
            if (demesne.get() != null) {
                return;
            }
            demesne.set(new DemesneImpl(parameters));
        } finally {
            lock.unlock();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_launch")
    private static boolean launch(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                                  JByteArray parameters, int parametersLen) {
        var parametersBuff = CTypeConversion.asByteBuffer(
        jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, parameters, false), parametersLen);
        log.trace("Launch Demesne Isolate: {}", isolateId);
        try {
            launch(jniEnv, parametersBuff, clazz);
            return true;
        } catch (GeneralSecurityException | IOException e) {
            log.error("Cannot launch demesne", e);
            return false;
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_rotate")
    private static CCharPointer rotate(JNIEnvironment jniEnv, JClass clazz,
                                       @CEntryPoint.IsolateThreadContext long isolateId, JByteArray spec, int specLen) {
        final Demesne d = demesne.get();
        if (d != null) {
            var specBuff = CTypeConversion.asByteBuffer(
            jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, spec, false), specLen);
            RotationSpecification.Builder specification;
            try {
                specification = RotationSpecification.Builder.from(RotationSpec.parseFrom(specBuff));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Unable to parse controlling rotation specification", e);
            }
            return CTypeConversion.toCBytes(d.rotate(specification).getBytes()).get();
        }
        return CTypeConversion.toCBytes(RotationEvent.getDefaultInstance().toByteArray()).get();
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_start")
    private static void start(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId)
    throws GeneralSecurityException {
        final Demesne d = demesne.get();
        if (d != null) {
            d.start();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_stop")
    private static void stop(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId)
    throws GeneralSecurityException {
        final Demesne d = demesne.get();
        if (d != null) {
            d.stop();
        }
    }

    private static JByteArray toByteArray(InceptionEvent rotate) {
        // TODO Auto-generated method stub
        return null;
    }

    private static JByteArray toByteArray(RotationEvent rotate) {
        // TODO Auto-generated method stub
        return null;
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_viewChange")
    private static boolean viewChange(JNIEnvironment jniEnv, JClass clazz,
                                      @CEntryPoint.IsolateThreadContext long isolateId, JByteArray vc, int size) {
        var buff = CTypeConversion.asByteBuffer(jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, vc, false),
                                                size);
        final Demesne current = demesne.get();
        if (current == null) {
            log.warn("No Demesne created");
            return false;
        }
        ViewChange viewChange;
        try {
            viewChange = ViewChange.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse DemesnesViewChange", e);
            return false;
        }
        current.viewChange(Digest.from(viewChange.getView()),
                           viewChange.getJoiningList().stream().map(EventCoordinates::from).toList(),
                           viewChange.getLeavingList().stream().map(Digest::from).toList());
        return true;
    }
}
