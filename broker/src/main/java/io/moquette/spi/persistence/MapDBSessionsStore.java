/*
 * Copyright (c) 2012-2015 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.spi.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.MessageGUID;
import io.moquette.spi.impl.Utils;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.persistence.MapDBPersistentStore.PersistentSession;

/**
 * ISessionsStore implementation backed by MapDB.
 *
 * @author andrea
 */
class MapDBSessionsStore implements ISessionsStore {

    private static final Logger LOG = LoggerFactory.getLogger(MapDBSessionsStore.class);

    //maps clientID->[MessageId -> guid]
    private ConcurrentMap<String, ConcurrentMap<Integer, MessageGUID>> m_inflightStore;
    //map clientID <-> set of currently in flight packet identifiers
    private Map<String, Set<Integer>> m_inFlightIds;
    private ConcurrentMap<String, PersistentSession> m_persistentSessions;
    //maps clientID->[guid*], insertion order cares, it's queue
    private ConcurrentMap<String, List<MessageGUID>> m_enqueuedStore;
    //maps clientID->[MessageId -> guid]
    private ConcurrentMap<String, Map<Integer, MessageGUID>> m_secondPhaseStore;

    private final DB m_db;
    private final MapDBMessagesStore m_messagesStore;

    MapDBSessionsStore(DB db, MapDBMessagesStore messagesStore) {
        m_db = db;
        m_messagesStore = messagesStore;
    }

    @Override
    public void initStore() {
        m_inflightStore = (ConcurrentMap<String, ConcurrentMap<Integer, MessageGUID>>) m_db.hashMap("inflight").createOrOpen();
        m_inFlightIds = (Map<String, Set<Integer>>) m_db.hashMap("inflightPacketIDs").createOrOpen();
        m_persistentSessions = (ConcurrentMap<String, PersistentSession>) m_db.hashMap("sessions").createOrOpen();
        m_enqueuedStore = (ConcurrentMap<String, List<MessageGUID>>) m_db.hashMap("sessionQueue").createOrOpen();
        m_secondPhaseStore = (ConcurrentMap<String, Map<Integer, MessageGUID>>) m_db.hashMap("secondPhase").createOrOpen();
    }

    @Override
    public void addNewSubscription(Subscription newSubscription) {
        LOG.debug("addNewSubscription invoked with subscription {}", newSubscription);
        final String clientID = newSubscription.getClientId();
        Map<String, Subscription> subscriptions = (Map<String, Subscription>) m_db.hashMap("subscriptions_" + clientID).createOrOpen();
        subscriptions.put(newSubscription.getTopicFilter(), newSubscription);

        if (LOG.isTraceEnabled()) {
            LOG.trace("subscriptions_{}: {}", clientID, subscriptions);
        }
    }

    @Override
    public void removeSubscription(String topicFilter, String clientID) {
        LOG.debug("removeSubscription topic filter: {} for clientID: {}", topicFilter, clientID);
        if (!m_db.exists("subscriptions_" + clientID)) {
            return;
        }
        Map<String, Subscription> subscriptions = (Map<String, Subscription>) m_db.hashMap("subscriptions_" + clientID).open();
        subscriptions.remove(topicFilter);
        
        //m_db.getHashMap("subscriptions_" + clientID).remove(topicFilter);
    }

    @Override
    public void wipeSubscriptions(String clientID) {
    	Map<String, Subscription> subscriptions = (Map<String, Subscription>) m_db.hashMap("subscriptions_" + clientID).open();
        LOG.debug("wipeSubscriptions");
        if (LOG.isTraceEnabled()) {
            LOG.trace("Subscription pre wipe: subscriptions_{}: {}", clientID, subscriptions);
        }
        //m_db.delete("subscriptions_" + clientID);
        subscriptions.clear();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Subscription post wipe: subscriptions_{}: {}", clientID, subscriptions);
        }
    }

    @Override
    public List<ClientTopicCouple> listAllSubscriptions() {
        final List<ClientTopicCouple> allSubscriptions = new ArrayList<>();
        for (String clientID : m_persistentSessions.keySet()) {
            ConcurrentMap<String, Subscription> clientSubscriptions = (ConcurrentMap<String, Subscription>) m_db.hashMap("subscriptions_" + clientID).createOrOpen();
            for (String topicFilter : clientSubscriptions.keySet()) {
                allSubscriptions.add(new ClientTopicCouple(clientID, topicFilter));
            }
        }
        LOG.debug("retrieveAllSubscriptions returning subs {}", allSubscriptions);
        return allSubscriptions;
    }

    @Override
    public Subscription getSubscription(ClientTopicCouple couple) {
        ConcurrentMap<String, Subscription> clientSubscriptions = (ConcurrentMap<String, Subscription>) m_db.hashMap("subscriptions_" + couple.clientID).createOrOpen();
        LOG.debug("subscriptions_{}: {}", couple.clientID, clientSubscriptions);
        return clientSubscriptions.get(couple.topicFilter);
    }

    @Override
    public List<Subscription> getSubscriptions() {
        List<Subscription> subscriptions = new ArrayList<>();
        for (String clientID : m_persistentSessions.keySet()) {
            ConcurrentMap<String, Subscription> clientSubscriptions = (ConcurrentMap<String, Subscription>) m_db.hashMap("subscriptions_" + clientID).createOrOpen();
            subscriptions.addAll(clientSubscriptions.values());
        }
        return subscriptions;
    }

    @Override
    public boolean contains(String clientID) {
        return m_db.exists("subscriptions_" + clientID);
    }

    @Override
    public ClientSession createNewSession(String clientID, boolean cleanSession) {
        LOG.debug("createNewSession for client <{}> with clean flag <{}>", clientID, cleanSession);
        if (m_persistentSessions.containsKey(clientID)) {
            LOG.error("already exists a session for client <{}>, bad condition", clientID);
            throw new IllegalArgumentException("Can't create a session with the ID of an already existing" + clientID);
        }
        LOG.debug("clientID {} is a newcome, creating it's empty subscriptions set", clientID);
        m_persistentSessions.putIfAbsent(clientID, new PersistentSession(cleanSession));
        return new ClientSession(clientID, m_messagesStore, this, cleanSession);
    }

    @Override
    public ClientSession sessionForClient(String clientID) {
        if (!m_persistentSessions.containsKey(clientID)) {
            return null;
        }

        PersistentSession storedSession = m_persistentSessions.get(clientID);
        return new ClientSession(clientID, m_messagesStore, this, storedSession.cleanSession);
    }

    @Override
    public void updateCleanStatus(String clientID, boolean cleanSession) {
        m_persistentSessions.put(clientID, new MapDBPersistentStore.PersistentSession(cleanSession));
    }

    /**
     * Return the next valid packetIdentifier for the given client session.
     * */
    @Override
    public int nextPacketID(String clientID) {
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient == null) {
            int nextPacketId = 1;
            inFlightForClient = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
            inFlightForClient.add(nextPacketId);
            this.m_inFlightIds.put(clientID, inFlightForClient);
            return nextPacketId;

        }

        int maxId = inFlightForClient.isEmpty() ? 0 : Collections.max(inFlightForClient);
        int nextPacketId = (maxId + 1) % 0xFFFF;
        inFlightForClient.add(nextPacketId);
        return nextPacketId;
    }

    @Override
    public void inFlightAck(String clientID, int messageID) {
        LOG.info("acknowledging inflight clientID <{}> messageID {}", clientID, messageID);
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.error("Can't find the inFlight record for client <{}>", clientID);
            return;
        }
        m.remove(messageID);

        //remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }
    }

    @Override
    public void inFlight(String clientID, int messageID, MessageGUID guid) {
        ConcurrentMap<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            m = new ConcurrentHashMap<>();
        }
        m.put(messageID, guid);
        LOG.info("storing inflight clientID <{}> messageID {} guid <{}>", clientID, messageID, guid);
        this.m_inflightStore.put(clientID, m);
    }

    @Override
    public void bindToDeliver(MessageGUID guid, String clientID) {
        List<MessageGUID> guids = Utils.defaultGet(m_enqueuedStore, clientID, new ArrayList<MessageGUID>());
        guids.add(guid);
        m_enqueuedStore.put(clientID, guids);
        m_messagesStore.incUsageCounter(guid);
    }

    @Override
    public Collection<MessageGUID> enqueued(String clientID) {
        return Utils.defaultGet(m_enqueuedStore, clientID, new ArrayList<MessageGUID>());
    }

    @Override
    public void removeEnqueued(String clientID, MessageGUID guid) {
        List<MessageGUID> guids = Utils.defaultGet(m_enqueuedStore, clientID, new ArrayList<MessageGUID>());
        guids.remove(guid);
        m_enqueuedStore.put(clientID, guids);
        m_messagesStore.decUsageCounter(guid);
        //if counter gets to 0 then remove from storage
        m_messagesStore.removeStoredMessage(guid);
    }

    @Override
    public void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID) {
        LOG.info("acknowledging inflight clientID <{}> messageID {}", clientID, messageID);
        Map<Integer, MessageGUID> m = this.m_inflightStore.get(clientID);
        if (m == null) {
            LOG.error("Can't find the inFlight record for client <{}>", clientID);
            return;
        }
        MessageGUID guid = m.remove(messageID);

        //remove from the ids store
        Set<Integer> inFlightForClient = this.m_inFlightIds.get(clientID);
        if (inFlightForClient != null) {
            inFlightForClient.remove(messageID);
        }

        LOG.info("Moving to second phase store");
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, new HashMap<Integer, MessageGUID>());
        messageIDs.put(messageID, guid);
        m_secondPhaseStore.put(clientID, messageIDs);
    }

    @Override
    public MessageGUID secondPhaseAcknowledged(String clientID, int messageID) {
        Map<Integer, MessageGUID> messageIDs = Utils.defaultGet(m_secondPhaseStore, clientID, new HashMap<Integer, MessageGUID>());
        MessageGUID guid = messageIDs.remove(messageID);
        m_secondPhaseStore.put(clientID, messageIDs);
        return guid;
    }

    @Override
    public MessageGUID mapToGuid(String clientID, int messageID) {
        ConcurrentMap<Integer, MessageGUID> messageIdToGuid = (ConcurrentMap<Integer, MessageGUID>) m_db.hashMap(messageId2GuidsMapName(clientID)).createOrOpen();
        return messageIdToGuid.get(messageID);
    }

    static String messageId2GuidsMapName(String clientID) {
        return "guidsMapping_" + clientID;
    }

	@Override
	public StoredMessage getInflightMessage(String clientID, int messageID) {
		Map<Integer, MessageGUID> clientEntries = m_inflightStore.get(clientID);
		if (clientEntries == null)
			return null;
        MessageGUID guid = clientEntries.get(messageID);
        LOG.info("inflight messageID {} guid <{}>", messageID, guid);
		if (guid == null)
			return null;
		return m_messagesStore.getMessageByGuid(guid);
	}
}
