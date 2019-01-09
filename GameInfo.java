package com.rg.alibaba;

import java.util.List;

public class GameInfo {
	
	private String gameKey;
	private String gameId;
	private String platform;
	private String gameName;
	private String deviceMasterTableName;
	private String sessionsTableName;
	List<String> liveBuild;
	
	public GameInfo(){
		
	}

	public String getGameKey() {
		return gameKey;
	}

	public void setGameKey(String gameKey) {
		this.gameKey = gameKey;
	}

	public String getGameId() {
		return gameId;
	}

	public void setGameId(String gameId) {
		this.gameId = gameId;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getGameName() {
		return gameName;
	}

	public void setGameName(String gameName) {
		this.gameName = gameName;
	}

	public String getDeviceMasterTableName() {
		return deviceMasterTableName;
	}

	public void setDeviceMasterTableName(String deviceMasterTableName) {
		this.deviceMasterTableName = deviceMasterTableName;
	}

	public String getSessionsTableName() {
		return sessionsTableName;
	}

	public void setSessionsTableName(String sessionsTableName) {
		this.sessionsTableName = sessionsTableName;
	}

	public List<String> getLiveBuild() {
		return liveBuild;
	}

	public void setLiveBuild(List<String> liveBuild) {
		this.liveBuild = liveBuild;
	}

	@Override
	public String toString() {
		return "GameInfo [gameKey=" + gameKey + ", gameId=" + gameId
				+ ", platform=" + platform + ", gameName=" + gameName
				+ ", deviceMasterTableName=" + deviceMasterTableName
				+ ", sessionsTableName=" + sessionsTableName + ", liveBuild="
				+ liveBuild + "]";
	}

}
