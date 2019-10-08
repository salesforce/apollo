/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.Member.uuidBits;
import static com.salesforce.apollo.fireflies.View.uuid;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.collect.Sets;
import com.salesforce.apollo.avro.Message;
import com.salesforce.apollo.avro.MessageDigest;
import com.salesforce.apollo.avro.MessageGossip;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MessageBuffer {
	private final static TimeBasedGenerator GENERATOR = Generators.timeBasedGenerator();
	private final static Logger log = LoggerFactory.getLogger(MessageBuffer.class);

	private final int bufferSize;
	private final Map<UUID, Long> maxTimes = new ConcurrentHashMap<>();
	private final Map<UUID, Message> state = new ConcurrentHashMap<>();
	private final int tooOld;

	public MessageBuffer(int bufferSize, int tooOld) {
		this.bufferSize = bufferSize;
		this.tooOld = tooOld;
	}

	public void clear() {
		maxTimes.clear();
		state.clear();
	}

	public void gc() {
		if (state.size() > bufferSize) {
			compact();
		}
	}

	/**
	 * @return the digest state
	 */
	public List<MessageDigest> getDigests() {
		return state.values().stream().map(m -> m.getDigest()).collect(Collectors.toList());
	}

	/**
	 * Merge the updates.
	 * 
	 * @param updates
	 * @param validator
	 * @return the list of new messages for this buffer
	 */
	public List<Message> merge(List<Message> updates, Predicate<Message> validator) {
		return updates.stream().filter(validator).filter(message -> put(message)).collect(Collectors.toList());
	}

	public MessageGossip process(List<MessageDigest> requested) {
		MessageGossip gossip = new MessageGossip();
		Set<UUID> received = new HashSet<>(requested.size());
		gossip.setDigests(requested.stream().filter(d -> {
			UUID id = uuid(d.getId());
			received.add(id);
			Message update = state.get(id);
			if (update != null) {
				update.getDigest().setAge(Math.max(update.getDigest().getAge(), d.getAge()));
				return false;
			}
			return true;
		}).collect(Collectors.toList()));
		gossip.setUpdates(Sets.difference(state.keySet(), received).stream().map(id -> state.get(id))
				.collect(Collectors.toList()));

		log.trace("want messages: {} updates: {}", gossip.getDigests().size(), gossip.getUpdates().size());
		return gossip;
	}

	/**
	 * Insert a new message into the buffer from the node
	 * 
	 * @param ts
	 * @param bytes
	 * @param from
	 * @param channel
	 * @return the inserted Message
	 */
	public Message put(long ts, byte[] bytes, Node from, int channel) {
		UUID id = GENERATOR.generate();
		Message update = createUpdate(channel, id, ts, bytes, from.forSigning(), from.getId());
		put(update);
		log.trace("broadcasting: {}", id);
		return update;
	}

	public List<Message> updatesFor(List<MessageDigest> digests) {
		return digests.stream().map(digest -> state.get(uuid(digest.getId()))).filter(m -> m != null)
				.collect(Collectors.toList());
	}

	private void compact() {
		log.trace("Compacting buffer");
		removeOutOfDate();
		purgeTheAged();
	}

	private Message createUpdate(int channel, UUID id, long ts, byte[] content, Signature signature, UUID from) {
		byte[] s;
		try {
			signature.update(content);
			s = signature.sign();
		} catch (SignatureException e) {
			throw new IllegalStateException("Unable to sign message content", e);
		}
		return new Message(new MessageDigest(uuidBits(from), uuidBits(id), 0, System.currentTimeMillis()), channel,
				ByteBuffer.wrap(content), ByteBuffer.wrap(s));
	}

	private void purgeTheAged() {
		Message max;
		while (state.size() > bufferSize) {
			max = null;
			for (Message u : state.values()) {
				if (max == null) {
					max = u;
				} else if (u.getDigest().getAge() > max.getDigest().getAge()
						&& u.getDigest().getTime() >= max.getDigest().getTime()) {
					max = u;
				}
			}
			if (max == null) {
				break;
			}
			UUID removed = uuid(max.getDigest().getId());
			state.remove(removed);
			log.trace("removing: {}", removed);
		}
	}

	private boolean put(Message update) {
		AtomicBoolean updated = new AtomicBoolean(false);
		UUID id = uuid(update.getDigest().getId());
		state.compute(id, (k, v) -> {
			if (v == null) {
				// first time, update
				Long current = maxTimes.compute(k,
						(mid, max) -> Math.max(max == null ? 0 : max, update.getDigest().getTime()));
				if (current - update.getDigest().getTime() > tooOld) {
					// too old, discard
					return null;
				}
				updated.set(true);
				return update;
			}
			v.getDigest().setAge(Math.max(v.getDigest().getAge(), update.getDigest().getAge()));
			return v; // regossiped, but not updated
		});
		return updated.get();
	}

	private void removeOutOfDate() {
		state.entrySet().forEach(entry -> {
			Long max = maxTimes.get(uuid(entry.getValue().getDigest().getSource()));
			if (max != null && (max - entry.getValue().getDigest().getTime()) > tooOld) {
				state.remove(entry.getKey());
				log.trace("removing: {}", entry.getKey());
			}
		});
	}
}
