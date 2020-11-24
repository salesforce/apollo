/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.SigningUtils.sign;

import java.security.Key;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

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
    private final SecureRandom            entropy;
    private final boolean                 isViewMember;
    private final Member                  member;
    private final Map<HashKey, PublicKey> validators = new HashMap<>();

    public ViewContext(Context<Member> context, Member member, KeyPair consensusKeyPair, List<ViewMember> members,
            SecureRandom entropy) {
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
                    log.warn("Consensus key for view {} does not match current consensus key on: {}", context.getId(),
                             member);
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
                    log.info("Adding consensus key for: {} on: {}", mID, member);
                    return consensusKey;
                });
            }
        });
        isViewMember = validators.containsKey(member.getId());
        log.info("View context established for: {} is member: {} is view member: {} validators: {}", member,
                 context.getMember(member.getId()) != null, isViewMember, validators.size());
        this.entropy = entropy;
    }

    public ViewContext(HashKey id, Context<Member> baseContext, Member m, KeyPair consensusKeyPair,
            List<ViewMember> members, SecureRandom entropy) {
        this(viewFor(id, baseContext), m, consensusKeyPair, members, entropy);
        baseContext.register(this);
    }

    public ViewContext(Reconfigure view, Context<Member> baseContext, Member member, KeyPair consensusKeyPair,
            SecureRandom entropy) {
        this(new HashKey(view.getId()), baseContext, member, consensusKeyPair, view.getViewList(), entropy);
    }

    public int cardinality() {
        return context.cardinality();
    }

    public ViewContext cloneWith(List<ViewMember> members) {
        log.info("Cloning view context: {} on: {} members: {}", context.getId(), member, members.size());
        return new ViewContext(context, member, consensusKeyPair, members, entropy);
    }

    public Messenger createMessenger(Parameters params) {
        return new Messenger(params.member, params.signature, context, params.communications, params.msgParameters);
    }

    @Override
    public void fail(Member member) {
        context.offlineIfActive(member.getId());
    }

    public Validate generateValidation(HashKey hash, Block block) {
        byte[] signature = sign(consensusKeyPair.getPrivate(), entropy,
                                Conversion.hashOf(block.getHeader().toByteString()));
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

    public HashKey getId() {
        return context.getId();
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

    @Override
    public void recover(Member member) {
        context.activateIfOffline(member.getId());
    }

    public Stream<Member> streamRandomRing() {
        return context.ring(entropy.nextInt(context.getRingCount())).stream();
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
        log.trace("Validation: {} from: {}", hash, memberID);
        final PublicKey key = validators.get(memberID);
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

    public KeyPair getConsensusKey() {
        return consensusKeyPair;
    }

    public int majority() {
        return cardinality() - toleranceLevel();
    }
}
