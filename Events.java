package com.rg.alibaba;

public class Events {

	private String Name;
	private String Timestamp;
	private String Subevent;
	private String CurrencyType;
	private String CurrencyValue;
	private String AccountBalance;
	private String ReferralUrl;
	private String NetworkStatus;

	public Events(){
		
	}

	public String getName() {
		return Name;
	}

	public void setName(String Name) {
		this.Name = Name;
	}

	public String getTimestamp() {
		return Timestamp;
	}

	public void setTimestamp(String timestamp) {
		Timestamp = timestamp;
	}

	public String getSubevent() {
		return Subevent;
	}

	public void setSubevent(String Subevent) {
		this.Subevent = Subevent;
	}

	public String getCurrencyType() {
		return CurrencyType;
	}

	public void setCurrencyType(String CurrencyType) {
		this.CurrencyType = CurrencyType;
	}

	public String getCurrencyValue() {
		return CurrencyValue;
	}

	public void setCurrencyValue(String CurrencyValue) {
		this.CurrencyValue = CurrencyValue;
	}

	public String getAccountBalance() {
		return AccountBalance;
	}

	public void setAccountBalance(String AccountBalance) {
		this.AccountBalance = AccountBalance;
	}

	public String getReferralUrl() {
		return ReferralUrl;
	}

	public void setReferralUrl(String ReferralUrl) {
		this.ReferralUrl = ReferralUrl;
	}

	public String getNetworkStatus() {
		return NetworkStatus;
	}

	public void setNetworkStatus(String NetworkStatus) {
		this.NetworkStatus = NetworkStatus;
	}

	@Override
	public String toString() {
		return "Events [Name=" + Name + ", Timestamp=" + Timestamp
				+ ", Subevent=" + Subevent + ", CurrencyType=" + CurrencyType
				+ ", CurrencyValue=" + CurrencyValue + ", AccountBalance="
				+ AccountBalance + ", ReferralUrl=" + ReferralUrl
				+ ", NetworkStatus=" + NetworkStatus + "]";
	}
}
