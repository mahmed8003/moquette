package io.moquette.spi;

public interface IPersistentStore {
	
	public void initStore();
	
	public IMessagesStore messagesStore();
	
	public ISessionsStore sessionsStore();
	
	public void close();

}
