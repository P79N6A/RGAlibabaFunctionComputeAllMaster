package com.rg.alibaba;

import java.util.ArrayList;
import java.util.List;

public class SessionsEvent {

	private String SessionID;
	private List<Events> Events = new ArrayList<Events>();
	
	public SessionsEvent(){
		
	}

	public String getSessionID() {
		return SessionID;
	}

	public void setSessionID(String SessionID) {
		this.SessionID = SessionID;
	}

	public List<Events> getEvents() {
		return Events;
	}

	public void setEvents(List<Events> Events) {
		this.Events = Events;
	}

	@Override
	public String toString() {
		return "SessionsEvent [SessionID=" + SessionID + ", Events=" + Events
				+ "]";
	}

}
