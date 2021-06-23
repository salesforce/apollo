/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.security.Key;
import java.security.KeyPair;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.stereotomy.Stereotomy.ControllableIdentifier;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ViewContext implements MembershipListener<Member> {
    private static final Logger log = LoggerFactory.getLogger(ViewContext.class);

    /**
     * Answer the live successors of the hash on the base context view
     */
    public static Context<Member> viewFor(Digest hash, Context<? super Member> baseContext) {
        Context<Member> newView = new Context<Member>(hash, baseContext.getRingCount());
        Set<Member> successors = new HashSet<Member>();
        baseContext.successors(hash, m -> {
            if (successors.size() == baseContext.getRingCount()) {
                return false;
            }
            boolean contained = successors.contains(m);
            successors.add(m);
            return !contained;
        });
        assert successors.size() == baseContext.getRingCount();
        successors.forEach(e -> {
            if (baseContext.isActive(e)) {
                newView.activate(e);
            } else {
                newView.offline(e);
            }
        });
        assert newView.getActive().size() + newView.getOffline().size() == baseContext.getRingCount();
        return newView;
    }

    private final KeyPair                consensusKeyPair;
    private final Context<Member>        context;
    private final DigestAlgorithm        digestAlgorithm;
    private final boolean                isViewMember;
    private final SigningMember          member;
    private final Map<Digest, PublicKey> validators = new HashMap<>();

    public ViewContext(DigestAlgorithm digestAlgorithm, Context<Member> context, SigningMember member,
            KeyPair consensusKeyPair, List<ViewMember> members) {
        assert consensusKeyPair != null;
        this.context = context;
        this.member = member;
        this.consensusKeyPair = consensusKeyPair;
        this.digestAlgorithm = digestAlgorithm;
        members.forEach(vm -> {
            Digest mID = new Digest(vm.getId());
            if (member.getId().equals(mID)) {
                PublicKey consensusKey = publicKey(vm.getConsensusKey());
                if (!consensusKeyPair.getPublic().equals(consensusKey)) {
                    log.warn("Consensus key: {} for view {} does not match current consensus key: {} on: {}",
                             DigestAlgorithm.DEFAULT.digest(consensusKeyPair.getPublic().getEncoded()), context.getId(),
                             DigestAlgorithm.DEFAULT.digest(consensusKey.getEncoded()), member);
                } else {
                    log.trace("Consensus key for view {} matches current consensus key on: {}", context.getId(),
                              member);
                    validators.put(mID, consensusKeyPair.getPublic());
                }
            } else {
                validators.computeIfAbsent(mID, k -> {
                    PublicKey consensusKey = publicKey(vm.getConsensusKey());
                    if (consensusKey == null) {
                        log.debug("invalid view member, cannot deserialize consensus key for: {} on: {}", mID, member);
                        return null;
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("Adding consensus key: {} for: {} on: {}",
                                  DigestAlgorithm.DEFAULT.digest(consensusKey.getEncoded()), mID, member);
                    }
                    return consensusKey;
                });
            }
        });
        isViewMember = validators.containsKey(member.getId());
        log.debug("View context established for: {} is member: {} is view member: {} validators: {}", member,
                  context.getMember(member.getId()) != null, isViewMember, validators.size());
    }

    public ViewContext(DigestAlgorithm digestAlgorithm, Digest id, Context<Member> baseContext, SigningMember m,
            KeyPair consensusKeyPair, List<ViewMember> members) {
        this(digestAlgorithm, viewFor(id, baseContext), m, consensusKeyPair, members);
        baseContext.register(this);
    }

    public ViewContext(DigestAlgorithm digestAlgorithm, Reconfigure view, Context<Member> baseContext,
            SigningMember member, KeyPair consensusKeyPair) {
        this(digestAlgorithm, Digest.from(view.getId()), baseContext, member, consensusKeyPair, view.getViewList());
    }

    public void activeAll() {
        context.getOffline().forEach(m -> context.activate(m));
    }

    public int activeCardinality() {
        return context.getActive().size();
    }

    public Stream<Member> allMembers() {
        return context.allMembers();
    }

    public int cardinality() {
        return context.cardinality();
    }

    public ViewContext cloneWith(List<ViewMember> members) {
        return new ViewContext(digestAlgorithm, context, member, consensusKeyPair, members);
    }

    public Messenger createMessenger(Parameters params, Executor executor) {
        return new Messenger(params.member, context, params.communications, params.msgParameters, executor);
    }

    @Override
    public void fail(Member member) {
        context.offlineIfActive(member.getId());
    }

    public Validate generateValidation(Digest hash, Block block) {
        JohnHancock signature = new Signer(
                0, consensusKeyPair.getPrivate())
                                                 .sign(digestAlgorithm.digest(block.getHeader().toByteString())
                                                                      .toByteString());

        if (log.isTraceEnabled()) {
            log.trace("generating validation of: {} key: {} on: {}", hash,
                      digestAlgorithm.digest(consensusKeyPair.getPublic().getEncoded()), member);
        }
        return generateValidation(hash, signature);
    }

    public Validate generateValidation(Digest hash, JohnHancock signature) {
        if (signature == null) {
            log.error("Unable to sign block: {} on: {}", hash, member);
            return null;
        }
        Validate validation = Validate.newBuilder()
                                      .setId(member.getId().toByteString())
                                      .setHash(hash.toByteString())
                                      .setSignature(signature.toByteString())
                                      .build();
        return validation;
    }

    public Member getActiveMember(Digest fromID) {
        return context.getActiveMember(fromID);
    }

    public KeyPair getConsensusKey() {
        return consensusKeyPair;
    }

    public Digest getId() {
        return context.getId();
    }

    public Member getMember() {
        return member;
    }

    public Member getMember(Digest from) {
        return context.getMember(from);
    }

    public Key getPublic() {
        return consensusKeyPair.getPublic();
    }

    public Member getRegent(int regent) {
        return context.ring(0).get(regent);
    }

    public int getRingCount() {
        return context.getRingCount();
    }

    public ControllableIdentifier getIdentifier() {
        // TODO
        return null;
    }

    public ViewMember getView() {
        ByteString encoded = bs(consensusKeyPair.getPublic());
        JohnHancock signed = member.sign(encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", member);
            return null;
        }
        return ViewMember.newBuilder()
                         .setId(member.getId().toByteString())
                         .setConsensusKey(encoded)
                         .setSignature(signed.toByteString())
                         .build();
    }

    public boolean isMember() {
        return context.getMember(member.getId()) != null;
    }

    public boolean isViewMember() {
        return isViewMember;
    }

    public int majority() {
        return cardinality() - toleranceLevel();
    }

    @Override
    public void recover(Member member) {
        context.activateIfOffline(member.getId());
    }

    public Stream<Member> streamRandomRing() {
        return context.ring(Utils.bitStreamEntropy().nextInt(context.getRingCount())).stream();
    }

    public int timeToLive() {
        return context.timeToLive();
    }

    public int toleranceLevel() {
        return context.toleranceLevel();
    }

    public boolean validate(Block block, Validate v) {
        Digest hash = new Digest(v.getHash());
        final Digest memberID = new Digest(v.getId());
        final PublicKey key = validators.get(memberID);
        if (log.isTraceEnabled()) {
            log.trace("validating: {} from: {} key: {} on: {}", hash, memberID,
                      digestAlgorithm.digest(key.getEncoded()), member);
        }
        if (key == null) {
            log.debug("No validator key to validate: {} for: {} on: {}", hash, memberID, member);
            return false;
        }
        Member member = context.getMember(memberID);
        if (member == null) {
            log.debug("No member found for {}", memberID);
            return false;
        }

        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.lookup(key);

        Digest headerHash = digestAlgorithm.digest(block.getHeader().toByteString());
        boolean verified = signatureAlgorithm.verify(key, signature(v.getSignature()), headerHash.toByteString());
        if (!verified) {
            log.error("Error validating block signature of {} did not match", memberID);
        }
        return verified;
    }

    public boolean validate(CertifiedBlock block) {
//      Function<Digest, Signature> validators = h -> {
//      Member member = view.getMember(h);
//      if (member == null) {
//          return null;
//      }
//      return member.forValidation(Conversion.DEFAULT_SIGNATURE_ALGORITHM);
//  };
//  return block.getCertificationsList()
//              .parallelStream()
//              .filter(c -> verify(validators, block.getBlock(), c))
//              .limit(toleranceLevel + 1)
//              .count() >= toleranceLevel + 1;
        return true;
    }
}
