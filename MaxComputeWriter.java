package com.rg.alibaba;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.aliyun.fc.runtime.Context;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.ip2location.IP2Location;
import com.ip2location.IPResult;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MaxComputeWriter {

	public static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static JedisPool pool = null;
    public static IP2Location loc = null;
    /*public static final Map<String, String> map = new HashMap<String, String>(){
    	{
    		//wwe
    		put("d9aa96284f57ce80a050e5f4564ca3b30df0fc4f", "{\"gameKey\":\"WWE_IOS_GL_F\",\"gameId\":63,\"platform\":\"ios\",\"gameName\":\"WWE\",\"deviceMasterTableName\":\"device_master_wwe\",\"sessionsTableName\":\"sessions_wwe\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("5ed07414bbae2c91cde7e8584b245afe1fa668ef", "{\"gameKey\":\"WWE_GP_GL_F\",\"gameId\":63,\"platform\":\"android\",\"gameName\":\"WWE\",\"deviceMasterTableName\":\"device_master_wwe\",\"sessionsTableName\":\"sessions_wwe\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("a78885a65550b9ceb7c11f800a40d3e05a9a85e4", "{\"gameKey\":\"WWE_AZ_GL_F\",\"gameId\":63,\"platform\":\"amazon\",\"gameName\":\"WWE\",\"deviceMasterTableName\":\"device_master_wwe\",\"sessionsTableName\":\"sessions_wwe\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");

    		//drones2
    		put("161009b20d513ec2a7fc96a2cf924c6d6e8f4697", "{\"gameKey\":\"DRN2_IOS_GL_F\",\"gameId\":45,\"platform\":\"ios\",\"gameName\":\"Drones 2\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("a47bd5b8348eb9d8d0722ed4e38a6fa9ce8eeb02", "{\"gameKey\":\"DRN2_GP_GL_F\",\"gameId\":45,\"platform\":\"android\",\"gameName\":\"Drones 2\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("470229231a9c7a6ea9c8feb40b688ea65cff344a", "{\"gameKey\":\"DRN2_AZ_GL_F\",\"gameId\":45,\"platform\":\"amazon\",\"gameName\":\"Drones 2\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");

    		//dd
    		put("e2082e97d4dd21735b2ab985b120d3308fd1e685", "{\"gameKey\":\"DD_IOS_GL_F\",\"gameId\":30,\"platform\":\"ios\",\"gameName\":\"Duck Dynasty\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("2cbb2c162a6fb68020ab469cebae32d03ad38a25", "{\"gameKey\":\"DD_GP_GL_F\",\"gameId\":30,\"platform\":\"android\",\"gameName\":\"Duck Dynasty\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("39d761dc55f18b32f58f6d17213d10698f90f735", "{\"gameKey\":\"DD_AZ_GL_F\",\"gameId\":30,\"platform\":\"amazon\",\"gameName\":\"Duck Dynasty\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");

    		//itb
    		put("681652d5bc3d673a27fc6a755bb240185ead373d", "{\"gameKey\":\"ITB_IOS_GL_F\",\"gameId\":45,\"platform\":\"ios\",\"gameName\":\"ITB\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("a200ebb529d259cf2f828d8e3fe52b85ea847274", "{\"gameKey\":\"ITB_GP_GL_F\",\"gameId\":45,\"platform\":\"android\",\"gameName\":\"ITB\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");
    		put("5e4501154f5e3db305fa0f4c4eeb8a87cb3bb8e6", "{\"gameKey\":\"ITB_AZ_GL_F\",\"gameId\":45,\"platform\":\"amazon\",\"gameName\":\"ITB\",\"deviceMasterTableName\":\"device_master\",\"sessionsTableName\":\"sessions\",\"liveBuild\":[\"1.14.275\",\"1.13.266\"]}");

    	}

    };*/

    static {
    	pool = new JedisPool(new JedisPoolConfig(), "cachealibaba.reliancegames.com", 6379);
    	//pool = new JedisPool(new JedisPoolConfig(), "47.74.234.179", 6379);
    	loc = new IP2Location();
    	ClassLoader classLoader = MaxComputeWriter.class.getClassLoader();
    	File file = new File(classLoader.getResource("IP2LOCATION-LITE-DB5.BIN").getFile());
    	loc.IPDatabasePath = file.getAbsolutePath();

    }

    public static void main(String[] args) throws FileNotFoundException, ParseException {
        //String[] msg = new String[]{" {\"DeviceID\":\"3c34e017757d60b8\",\"IdentifierForVendor\":\"null\",\"CountryName\":\"null\",\"UniqueIdentifier\":\"3c34e017757d60b8\",\"MACId\":\"null\",\"MAC_SHA1\":\"575966ae-101d-4beb-9f5c-782c511be807\",\"GameVersion\":\"1.2.02\",\"GameID\":\"a200ebb529d259cf2f828d8e3fe52b85ea847274\",\"Platform\":\"Android\",\"Device\":\"TRT-LX1\",\"Sessions\":[{\"SessionID\":\"3c34e017757d60b8201807250946424613\",\"Events\":[{\"Name\":\"application_resume\",\"Timestamp\":\"201807250956015782\",\"Subevent\":\"application_resume\",\"CurrencyType\":\"\",\"CurrencyValue\":\"\",\"AccountBalance\":\"\",\"ReferralUrl\":\"\",\"NetworkStatus\":\"online\"}]},{\"SessionID\":\"3c34e017757d60b8201807250946424613\",\"Events\":[{\"Name\":\"application_resume\",\"Timestamp\":\"201807250956015782\",\"Subevent\":\"application_resume\",\"CurrencyType\":\"\",\"CurrencyValue\":\"\",\"AccountBalance\":\"\",\"ReferralUrl\":\"\",\"NetworkStatus\":\"online\"}]}]}\n"};
    	//insertIntoMaxCompute(msg,null,null);
    }

    public static void insertIntoMaxCompute(String[] logs,Context context) {
    	Calendar calendar = Calendar.getInstance();
        Account account = new AliyunAccount("LTAIfBYu7RN3kms2", "IzGIxcfcjPp95iGDRmmXHhmnkGdGdM");
        Odps odps = new Odps(account);
        String odpsUrl = "http://service.ap-southeast-1.maxcompute.aliyun.com/api";
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject("rgames");
        List<Map<String, Object>> mapList = Arrays.stream(logs).map(log -> {
            Map<String, Object> map = parseJSON(log);
            return map;
        }).collect(Collectors.toList());
        String gameSha = null;
        String buildVersion = null;

        try {

        	//get game sha from json
            for (Map<String, Object> map : mapList) {
            	gameSha = map.get("GameID").toString();
            	buildVersion = map.get("GameVersion").toString();
            	if(gameSha == null || gameSha.equalsIgnoreCase("")){
            		throw new Exception("null gameid in json");
            	}
    		}

        	 //get game info from gamesha
        	 GameInfo gameInfo = new GameInfo();
        	 String gameInfoString  = getGameInfo(gameSha);

        	 if(gameInfoString != null) {
        		gameInfo = new ObjectMapper().readValue(gameInfoString, GameInfo.class);
        		String sql = buildBatchSQL(mapList,gameInfo,gameSha,buildVersion,null); //call device_master
                 //System.out.println(buildBatchSessionsSql(mapList));
                String sqlSession = buildBatchSessionsSql(mapList,calendar,gameInfo,gameSha,buildVersion); //call sessions
                context.getLogger().info("runsql="+sql);
                Instance i = SQLTask.run(odps, sql);
                context.getLogger().info("after runsql="+sql);
                context.getLogger().info("runsql_session="+sqlSession);
                Instance i2 = SQLTask.run(odps, sqlSession);
                context.getLogger().info("after runsqlsql_session="+sqlSession);

        	 }else {
            		 throw new Exception("no game info in redis:game id: "+gameSha);
            }

        } catch (Exception ex) {
        	ex.printStackTrace();
            StringBuffer exsb = new StringBuffer();

            exsb.append(ex.getMessage());
            for(StackTraceElement ele :ex.getStackTrace()){
                exsb.append(ele.getClassName()).append(".").append(ele.getMethodName()).append(":").append(ele.getLineNumber()).append("====");
            }
            context.getLogger().error(exsb.toString());
        }

    }

    private static String getGameInfo(String gameSha) {
    	Jedis redis = pool.getResource();
    	String gameInfo = redis.get(gameSha);
    	redis.close();
    	return gameInfo;
    	//return map.get(gameSha);
	}

	/*public static String buildSQL(Map<String, Object> map) {
        Map<String, String> columnsMap = new HashMap<>();
        columnsMap.put("UniqueIdentifier", "UDID");
        columnsMap.put("MACId", "MAC_Id");
        columnsMap.put("MAC_SHA1", "MAC_SHA1");
        columnsMap.put("GameVersion", "VERSION");
        columnsMap.put("GameID", "GAME_ID");
        columnsMap.put("Platform", "PLATFORM");
        columnsMap.put("Device", "DEVICE");
        List<String> columnList = columnsMap.entrySet().stream()
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        List<String> valueList = columnsMap.entrySet().stream()
                .map(entry -> "'" + map.get(entry.getKey()).toString() + "'")
                .filter(x -> !(x == null))
                .collect(Collectors.toList());
        String columnsString = String.join(",", columnList);
        String valuesString = String.join(",", valueList);
        String sql = "insert into device_master_ios (" +
                columnsString + ")" + " values (" +
                valuesString + ")" + ";";
        return sql;

    }*/


    public static String buildBatchSQL(List<Map<String, Object>> mapList, GameInfo gameInfo, String gameSha, String buildVersion, String ip) throws IOException {
    	String countryName = "";
    	String countryCode = "";
        Map<String, String> columnsMap = new HashMap<>();
        columnsMap.put("UniqueIdentifier", "unique_id");
        columnsMap.put("MACId", "mac_id");
        columnsMap.put("MAC_SHA1", "mac_sha1");
        columnsMap.put("GameVersion", "version");
        columnsMap.put("GameID", "game_id");
        columnsMap.put("Platform", "platform");
        columnsMap.put("Device", "device");
        columnsMap.put("IdentifierForVendor", "vendor_id");

       //create column list for insert
        List<String> columnList = columnsMap.entrySet().stream()
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        columnList.add("ts");
        columnList.add("country");
        columnList.add("country_code");
        columnList.add("ip_address");
        String columnsString = String.join(",", columnList);

        //get country
        if(ip != null && !(ip.equalsIgnoreCase(""))){
	        IPResult rec = loc.IPQuery(ip);

	        if(rec != null){
	        	countryName = rec.getCountryLong().toUpperCase();
	        	countryCode = rec.getCountryShort().toUpperCase();
	        }
        }else{
        	ip = "";
        }

        //create device_master insert query
        List<String> valueList = mapList.stream().map(map -> {
            List<String> values = columnsMap.entrySet().stream()
                    .map(entry -> "'" + map.get(entry.getKey()).toString() + "'")
                    .collect(Collectors.toList());
            return "(" + String.join(",", values) +", datetime'"+dateTimeFormat.format(new Date().getTime()+19800000)+"','%countryName%','%countryCode%','%ipAddress%')";
        }).collect(Collectors.toList());

        String valuesString = String.join(",", valueList);

        valuesString = valuesString.replaceAll(gameSha, gameInfo.getGameKey())
        		                   .replaceAll("%countryName%", countryName)
        		                   .replaceAll("%countryCode%", countryCode)
        		                   .replaceAll("%ipAddress%", ip);

        String sql = "insert into %tableNameDeviceMaster% (" +
                columnsString + ")" + " values " +
                valuesString + "" + ";";

        if(gameInfo.getLiveBuild() == null || gameInfo.getLiveBuild().size()<=0){
        	sql = sql.replace("%tableNameDeviceMaster%", gameInfo.getDeviceMasterTableName());
        }else{
        	if(gameInfo.getLiveBuild().contains(buildVersion)){
        		sql = sql.replace("%tableNameDeviceMaster%", gameInfo.getDeviceMasterTableName());
        	}else{
        		sql = sql.replace("%tableNameDeviceMaster%", "device_master_sandbox");
        	}
        }

        return sql;

    }

    public static Map<String, Object> parseJSON(String jsonStr) {

        Map<String, Object> map = JSON.parseObject(
                jsonStr, new TypeReference<Map<String, Object>>() {
                });

        return map;
    }


    @SuppressWarnings("unchecked")
	public static String buildBatchSessionsSql(List<Map<String, Object>> mapList, Calendar calendar, GameInfo gameInfo, String gameSha, String buildVersion){
    	String sql = "INSERT INTO %tableSessions% (session_id,game_id,timestamp,event,parameter,version,currency_id,currency_value,account_balance,network,dt,unique_id,"
    										+ "param_1,param_2,param_3,param_4,param_5,param_6,param_7,param_8,param_9,param_10,param_11,param_12,param_13,param_14,param_15,"
    										+ "param_16,param_17,param_18,param_19,param_20,param_21,param_22,param_23,param_24,param_25,systemdttm) values ";
    	List<String> sql_values = new ArrayList<String>();

    	try{
    		String gameId = gameInfo.getGameKey();
	    	for (Map<String, Object> map : mapList) {
				//String gameVersion = map.get("GameVersion").toString();
				String deviceId = map.get("DeviceID").toString();
	  			//List<SessionsEvent> sessionsEventList = (List<SessionsEvent>) map.get("Sessions");

	  			List<JSONObject> jsonObjSessionList = (List<JSONObject>) map.get("Sessions");


	  			for (JSONObject jsonObjectSession : jsonObjSessionList) {

					for(int i =0;i<jsonObjectSession.getJSONArray("Events").size();i++){
						Events event = new Events();
						event.setName((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Name"));
						event.setCurrencyType((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("CurrencyType"));
						event.setNetworkStatus((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("NetworkStatus"));
						event.setCurrencyValue((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("CurrencyValue"));
						event.setReferralUrl((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("ReferralUrl"));
						event.setSubevent((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Subevent"));
						event.setTimestamp((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Timestamp"));


						sql_values.add("('"+jsonObjectSession.getString("SessionID")+"','"+gameId+"',"+getRestOftheEvents(buildVersion,event,deviceId,calendar)+")");
					}
				}

	            sql += String.join(",", sql_values)+ "" + ";";

	            if(gameInfo.getLiveBuild() == null || gameInfo.getLiveBuild().size()<=0){
	            	sql = sql.replace("%tableSessions%", gameInfo.getSessionsTableName());
	            }else{
	            	if(gameInfo.getLiveBuild().contains(buildVersion)){
	            		sql = sql.replace("%tableSessions%", gameInfo.getSessionsTableName());
	            	}else{
	            		sql = sql.replace("%tableSessions%", "sessions_sandbox");
	            	}
	            }

			}
    	}catch(Exception e){
    		e.printStackTrace();
    		//return null;
    	}

    	return sql;
    }

    public static String getRestOftheEvents(String version,Events event,String deviceId, Calendar calendar) throws ParseException{

    	String var=null;
    	Map<String,String> parameterMap = new HashMap<String, String>();
    	parameterMap = getParameter(event);
    	/*var = "'"+getTimestamp(event.getTimestamp(), calendar,"dateTime")+"','"+event.getName()+"','"+parameterMap.get("event")+"','"+version+"','"
    			 +event.getCurrencyType()+"','"+event.getCurrencyValue()+"','"
    			 +event.getAccountBalance()+"','"+event.getNetworkStatus()+"','"
    			 +getTimestamp(event.getTimestamp(), calendar,"date")+"','"
    			 +deviceId+"',"+parameterMap.get("subevent")+",'"+dateTimeFormat.format(new Date())+"'";*/

    	var = "datetime'"+getTimestamp(event.getTimestamp(), calendar,"dateTime")+"','"+event.getName()+"','"+parameterMap.get("event")+"','"+version+"','"
   			 +event.getCurrencyType()+"','"+event.getCurrencyValue()+"','"
   			 +event.getAccountBalance()+"','"+event.getNetworkStatus()+"',datetime'"
   			 +getTimestamp(event.getTimestamp(), calendar,"dateTime")+"','"
   			 +deviceId+"',"+parameterMap.get("subevent")+",datetime'"+dateTimeFormat.format(new Date().getTime()+19800000)+"'";

    	return var;
    }

    public static Map getParameter(Events event){

    	String subEvents = null;
    	Map<String,String> parameterSubParamMap = new HashMap<String, String>();
    	List<String> subEventList = new ArrayList<String>();
    	String[] subeventsArray = event.getSubevent().split(";");

    	for(int i=0;i<26;i++){
    		if(i==0){
    			parameterSubParamMap.put("event", subeventsArray[0]);
    		}else{
    			if(i < subeventsArray.length){
    				subEventList.add("'"+subeventsArray[i]+"'") ;
    			}else{
    				subEventList.add("''");
    			}
    		}
    	}
    	subEvents = String.join(",", subEventList);
    	parameterSubParamMap.put("subevent", subEvents);

    	return parameterSubParamMap;
    }

public static String getTimestamp(String timestamp, Calendar calendar, String type){

        try {

		    Integer year = Integer.parseInt(timestamp.substring(0, 4));
		    Integer month = Integer.parseInt(timestamp.substring(4, 6)) - 1;
		    Integer date = Integer.parseInt(timestamp.substring(6, 8));
		    Integer hourOfDay = Integer.parseInt(timestamp.substring(8, 10));
		    Integer minute = Integer.parseInt(timestamp.substring(10, 12));
		    Integer second = Integer.parseInt(timestamp.substring(12, 14));

		    calendar.set(year, month, date, hourOfDay, minute, second);

		    if(type.equalsIgnoreCase("dateTime")){
		    	return dateTimeFormat.format(calendar.getTime());
		    }else{
		    	return dateFormat.format(calendar.getTime());
		    }


		    //return calendar.getTime().toString();
		} catch (Exception e) {
		    throw new RuntimeException(e);
		}
    }

public static void insertIntoMaxCompute(String[] logs, Context context,
		String ip) {

	Calendar calendar = Calendar.getInstance();
    Account account = new AliyunAccount("LTAIfBYu7RN3kms2", "IzGIxcfcjPp95iGDRmmXHhmnkGdGdM");
    Odps odps = new Odps(account);
    String odpsUrl = "http://service.ap-southeast-1.maxcompute.aliyun.com/api";
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject("rgames");
    List<Map<String, Object>> mapList = Arrays.stream(logs).map(log -> {
        Map<String, Object> map = parseJSON(log);
        return map;
    }).collect(Collectors.toList());
    String gameSha = null;
    String buildVersion = null;

    try {

    	//get game sha from json
        for (Map<String, Object> map : mapList) {

        	gameSha = map.get("GameID").toString();
        	buildVersion = map.get("GameVersion").toString();
        	if(gameSha == null || gameSha.equalsIgnoreCase("")){
        		throw new Exception("null gameid in json");
        	}
		}

    	 //get game info from gamesha
    	 GameInfo gameInfo = new GameInfo();
    	 String gameInfoString  = getGameInfo(gameSha);

    	 if(gameInfoString != null) {
    		gameInfo = new ObjectMapper().readValue(gameInfoString, GameInfo.class);
    		String sql = buildBatchSQL(mapList,gameInfo,gameSha,buildVersion,ip); //call device_master
             //System.out.println(buildBatchSessionsSql(mapList));
            String sqlSession = buildBatchSessionsSql(mapList,calendar,gameInfo,gameSha,buildVersion); //call sessions
            context.getLogger().info("runsql="+sql);
            Instance i = SQLTask.run(odps, sql);
            //i.waitForSuccess();
			//context.getLogger().info(String.valueOf(i.isSuccessful()));
            context.getLogger().info("after runsql="+sql);
            context.getLogger().info("runsql_session="+sqlSession);
            Instance i2 = SQLTask.run(odps, sqlSession);
            //i2.waitForSuccess();
			//context.getLogger().info(String.valueOf(i2.isSuccessful()));
            context.getLogger().info("after runsqlsql_session="+sqlSession);

    	 }else {
        		 throw new Exception("no game info in redis:game id: "+gameSha);
        }

    } catch (Exception ex) {
    	ex.printStackTrace();
        StringBuffer exsb = new StringBuffer();

        exsb.append(ex.getMessage());
        for(StackTraceElement ele :ex.getStackTrace()){
            exsb.append(ele.getClassName()).append(".").append(ele.getMethodName()).append(":").append(ele.getLineNumber()).append("====");
        }
        context.getLogger().error(exsb.toString());
    }
}

}