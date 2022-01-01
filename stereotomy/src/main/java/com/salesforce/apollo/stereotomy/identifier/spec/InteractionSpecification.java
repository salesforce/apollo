/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier.spec;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class InteractionSpecification {

    public static class Builder implements Cloneable {
        private Format           format  = Format.PROTOBUF;
        private Identifier       identifier;
        private EventCoordinates lastEvent;
        private Digest           priorEventDigest;
        private final List<Seal> seals   = new ArrayList<>();
        private Signer           signer;
        private Version          version = Stereotomy.currentVersion();

        public Builder() {
        }

        public Builder addAllSeals(List<Seal> seals) {
            this.seals.addAll(requireNonNull(seals));
            return this;
        }

        public InteractionSpecification build() {
            return new InteractionSpecification(this.format, identifier, lastEvent.getSequenceNumber() + 1, lastEvent,
                                                signer, seals, version, priorEventDigest);
        }

        @Override
        public Builder clone() {
            Builder clone;
            try {
                clone = (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
            return clone;
        }

        public Format getFormat() {
            return format;
        }

        public Identifier getIdentifier() {
            return identifier;
        }

        public EventCoordinates getLastEvent() {
            return lastEvent;
        }

        public Digest getPriorEventDigest() {
            return priorEventDigest;
        }

        public List<Seal> getSeals() {
            return seals;
        }

        public Signer getSigner() {
            return signer;
        }

        public Version getVersion() {
            return version;
        }

        public Builder setCbor() {
            format = Format.CBOR;
            return this;
        }

        public Builder setIdentifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder setJson() {
            format = Format.JSON;
            return this;
        }

        public Builder setLastEvent(EventCoordinates lastEvent) {
            this.lastEvent = lastEvent;
            return this;
        }

        public Builder setMessagePack() {
            format = Format.MESSAGE_PACK;
            return this;
        }

        public Builder setPriorEventDigest(Digest priorEventDigest) {
            this.priorEventDigest = priorEventDigest;
            return this;
        }

        public Builder setSeal(Seal seal) {
            seals.add(requireNonNull(seal));
            return this;
        }

        public Builder setSigner(Signer signer) {
            this.signer = signer;
            return this;
        }

        public Builder setVersion(Version version) {
            this.version = version;
            return this;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private final Format           format;
    private final Identifier       identifier;
    private final EventCoordinates previous;
    private final Digest           priorEventDigest;
    private final List<Seal>       seals;
    private final long             sequenceNumber;
    private final Signer           signer;
    private final Version          version;

    public InteractionSpecification(Format format, Identifier identifier, long sequenceNumber,
                                    EventCoordinates previous, Signer signer, List<Seal> seals, Version version,
                                    Digest priorEventDigest) {
        this.format = format;
        this.identifier = identifier;
        this.sequenceNumber = sequenceNumber;
        this.previous = previous;
        this.signer = signer;
        this.seals = List.copyOf(seals);
        this.version = version;
        this.priorEventDigest = priorEventDigest;
    }

    public Format getFormat() {
        return format;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public EventCoordinates getPrevious() {
        return previous;
    }

    public Digest getPriorEventDigest() {
        return priorEventDigest;
    }

    public List<Seal> getSeals() {
        return seals;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Signer getSigner() {
        return signer;
    }

    public Version getVersion() {
        return version;
    }

}
