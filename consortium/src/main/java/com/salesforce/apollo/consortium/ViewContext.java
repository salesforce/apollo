/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.support.SigningUtils.sign;

import java.security.Key;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
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
import com.salesforce.apollo.consortium.support.SigningUtils;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
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
    public static Context<Member> viewFor(HashKey hash, Context<? super Member> baseContext) {
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

    private final KeyPair                 consensusKeyPair;
    private final Context<Member>         context;
    private final boolean                 isViewMember;
    private final Member                  member;
    private final Map<HashKey, PublicKey> validators = new HashMap<>();

    public ViewContext(Context<Member> context, Member member, KeyPair consensusKeyPair, List<ViewMember> members) {
        assert consensusKeyPair != null;
        this.context = context;
        this.member = member;
        this.consensusKeyPair = consensusKeyPair;
        members.forEach(vm -> {
            HashKey mID = new HashKey(vm.getId());
            if (member.getId().equals(mID)) {
                byte[] encoded = vm.getConsensusKey().toByteArray();
                PublicKey consensusKey = SigningUtils.publicKeyOf(encoded);
                if (!consensusKeyPair.getPublic().equals(consensusKey)) {
                    log.warn("Consensus key: {} for view {} does not match current consensus key: {} on: {}",
                             new HashKey(Conversion.hashOf(consensusKeyPair.getPublic().getEncoded())), context.getId(),
                             new HashKey(Conversion.hashOf(consensusKey.getEncoded())), member);
                } else {
                    log.trace("Consensus key for view {} matches current consensus key on: {}", context.getId(),
                              member);
                    validators.put(mID, consensusKeyPair.getPublic());
                }
            } else {
                validators.computeIfAbsent(mID, k -> {
                    byte[] encoded = vm.getConsensusKey().toByteArray();
                    PublicKey consensusKey = SigningUtils.publicKeyOf(encoded);
                    if (consensusKey == null) {
                        log.debug("invalid view member, cannot deserialize consensus key for: {} on: {}", mID, member);
                        return null;
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("Adding consensus key: {} for: {} on: {}",
                                  new HashKey(Conversion.hashOf(consensusKey.getEncoded())), mID, member);
                    }
                    return consensusKey;
                });
            }
        });
        isViewMember = validators.containsKey(member.getId());
        log.debug("View context established for: {} is member: {} is view member: {} validators: {}", member,
                  context.getMember(member.getId()) != null, isViewMember, validators.size());
    }

    public ViewContext(HashKey id, Context<Member> baseContext, Member m, KeyPair consensusKeyPair,
            List<ViewMember> members) {
        this(viewFor(id, baseContext), m, consensusKeyPair, members);
        baseContext.register(this);
    }

    public ViewContext(Reconfigure view, Context<Member> baseContext, Member member, KeyPair consensusKeyPair) {
        this(new HashKey(view.getId()), baseContext, member, consensusKeyPair, view.getViewList());
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
        return new ViewContext(context, member, consensusKeyPair, members);
    }

    public Messenger createMessenger(Parameters params, Executor executor) {
        return new Messenger(params.member, params.signature, context, params.communications, params.msgParameters,
                executor);
    }

    @Override
    public void fail(Member member) {
        context.offlineIfActive(member.getId());
    }

    public Validate generateValidation(HashKey hash, Block block) {
        byte[] signature = sign(consensusKeyPair.getPrivate(), Utils.secureEntropy(),
                                Conversion.hashOf(block.getHeader().toByteString()));
        if (log.isTraceEnabled()) {
            log.trace("generating validation of: {} key: {} on: {}", hash,
                      new HashKey(Conversion.hashOf(consensusKeyPair.getPublic().getEncoded())), member);
        }
        return generateValidation(hash, signature);
    }

    public Validate generateValidation(HashKey hash, byte[] signature) {
        if (signature == null) {
            log.error("Unable to sign block: {} on: {}", hash, member);
            return null;
        }
        Validate validation = Validate.newBuilder()
                                      .setId(member.getId().toByteString())
                                      .setHash(hash.toByteString())
                                      .setSignature(ByteString.copyFrom(signature))
                                      .build();
        return validation;
    }

    public Member getActiveMember(HashKey fromID) {
        return context.getActiveMember(fromID);
    }

    public KeyPair getConsensusKey() {
        return consensusKeyPair;
    }

    public HashKey getId() {
        return context.getId();
    }

    public Member getMember() {
        return member;
    }

    public Member getMember(HashKey from) {
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

    public ViewMember getView(Signature signature) {
        byte[] encoded = consensusKeyPair.getPublic().getEncoded();
        byte[] signed = sign(signature, encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", member);
            return null;
        }
        return ViewMember.newBuilder()
                         .setId(member.getId().toByteString())
                         .setConsensusKey(ByteString.copyFrom(encoded))
                         .setSignature(ByteString.copyFrom(signed))
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
        HashKey hash = new HashKey(v.getHash());
        final HashKey memberID = new HashKey(v.getId());
        final PublicKey key = validators.get(memberID);
        if (log.isTraceEnabled()) {
            log.trace("validating: {} from: {} key: {} on: {}", hash, memberID,
                      new HashKey(Conversion.hashOf(key.getEncoded())), member);
        }
        if (key == null) {
            log.debug("No validator key to validate: {} for: {} on: {}", hash, memberID, member);
            return false;
        }
        Signature signature = SigningUtils.signatureForVerification(key);
        Member member = context.getMember(memberID);
        if (member == null) {
            log.debug("No member found for {}", memberID);
            return false;
        }

        byte[] headerHash = Conversion.hashOf(block.getHeader().toByteString());
        try {
            signature.update(headerHash);
        } catch (SignatureException e) {
            log.error("Error updating validation signature of {}", memberID, e);
            return false;
        }
        try {
            boolean verified = signature.verify(v.getSignature().toByteArray());
            if (!verified) {
                log.error("Error validating block signature of {} did not match", memberID);
            }
            return verified;
        } catch (SignatureException e) {
            log.error("Error validating signature of {}", memberID, e);
            return false;
        }
    }

    public boolean validate(CertifiedBlock block) {
//      Function<HashKey, Signature> validators = h -> {
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
