package com.rg.alibaba;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.aliyun.fc.runtime.Context;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.ip2location.IP2Location;
import com.ip2location.IPResult;

import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class MaxComputeTunnelWriter {

    public static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static JedisPool pool = null;
    public static IP2Location loc = null;
    private static TableTunnel tunnel = null;
    private static Odps odps = null;
    public static final Map<String, String> gameIdMap = new HashMap<String, String>(){
		{
    	
    		//wwe
    		put("d9aa96284f57ce80a050e5f4564ca3b30df0fc4f", "WWE_IOS_GL_F");
    		put("5ed07414bbae2c91cde7e8584b245afe1fa668ef", "WWE_GP_GL_F");
    		put("a78885a65550b9ceb7c11f800a40d3e05a9a85e4", "WWE_AZ_GL_F");
    		
    	}
	};

    static {
       /* pool = new JedisPool(new JedisPoolConfig(), "cachealibaba.reliancegames.com", 6379);
    	//pool = new JedisPool(new JedisPoolConfig(), "47.74.209.80", 6379);
    	
        loc = new IP2Location();
        ClassLoader classLoader = MaxComputeTunnelWriter.class.getClassLoader();
        File file = new File(classLoader.getResource("IP2LOCATION-LITE-DB5.BIN").getFile());
        loc.IPDatabasePath = file.getAbsolutePath();*/

        Account account = new AliyunAccount("LTAIfBYu7RN3kms2", "IzGIxcfcjPp95iGDRmmXHhmnkGdGdM");
        String odpsUrl = "http://service.ap-southeast-1.maxcompute.aliyun.com/api";
        odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject("rgames");
        tunnel = new TableTunnel(odps);

    }


    private static String getGameInfo(String gameSha) {
        /*Jedis redis = pool.getResource();
        String gameInfo = redis.get(gameSha);
        redis.close();
        return gameInfo;*/
        //return map.get(gameSha);
    	return null;
    }


    public static void batchInsert(List<Map<String, Object>> mapList, GameInfo gameInfo, String gameSha, String buildVersion, Context context) throws IOException {
        context.getLogger().debug("batchInsert start");
        String table = getTable(gameInfo, buildVersion);
        Date ts = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd z");
        format.setTimeZone(TimeZone.getTimeZone("IST"));
        String ds = format.format(ts);
        PartitionSpec partitionSpec = new PartitionSpec("ds='" + ds + "'");

        Table tableObject = odps.tables().get(table);

        try {
            if (!tableObject.hasPartition(partitionSpec)) {
                tableObject.createPartition(partitionSpec);
            }
            TableTunnel.UploadSession uploadSession = tunnel.createUploadSession("rgames", table, partitionSpec);
            RecordWriter recordWriter = uploadSession.openRecordWriter(0);
            for (Map<String, Object> obj : mapList) {
                Record record = uploadSession.newRecord();
                record.set("unique_id", obj.get("UniqueIdentifier"));
                record.set("mac_id", obj.get("MACId"));
                record.set("mac_sha1", obj.get("MAC_SHA1"));
                record.set("version", obj.get("GameVersion"));
                record.set("game_id",gameIdMap.get(obj.get("GameID")));
                record.set("platform", obj.get("Platform"));
                record.set("device", obj.get("Device"));
                record.set("vendor_id", obj.get("IdentifierForVendor"));
                //record.setDatetime("ts", ts);
                String ip=obj.get("ip").toString();
                //get country
                String countryName = "";
                String countryCode = "";
//                if(ip != null && !(ip.equalsIgnoreCase(""))){
//                    IPResult rec = loc.IPQuery(ip);
//                    if(rec != null){
//                        countryName = rec.getCountryLong().toUpperCase();
//                        countryCode = rec.getCountryShort().toUpperCase();
//                    }
//                }else{
//                    ip = "";
//                }
                record.set("ip_address",ip);
                record.set("country",countryName);
                record.set("country_code",countryCode);
                
                List<JSONObject> jsonObjSessionList = (List<JSONObject>) obj.get("Sessions");
      			
	  			
      			for (JSONObject jsonObjectSession : jsonObjSessionList) {
    				
    				for(int i =0;i<jsonObjectSession.getJSONArray("Events").size();i++){
    					Events event = new Events();
    					String[] parameterArray = new String[26];
    					
    					event.setName((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Name"));
    					event.setCurrencyType((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("CurrencyType"));
    					event.setNetworkStatus((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("NetworkStatus"));
    					event.setCurrencyValue((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("CurrencyValue"));
    					event.setAccountBalance((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("AccountBalance"));
    					event.setReferralUrl((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("ReferralUrl"));
    					event.setSubevent((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Subevent"));
    					event.setTimestamp((String) jsonObjectSession.getJSONArray("Events").getJSONObject(i).get("Timestamp"));
    				
    					parameterArray = getParameter(event.getSubevent());
    					
    					record.set("session_id",jsonObjectSession.getString("SessionID"));
    					
    					record.set("event",event.getName());
    					record.set("currency_id",event.getCurrencyType());
    					record.set("currency_value",event.getCurrencyValue());
    					record.set("network",event.getNetworkStatus());
    					record.set("referal_url",event.getReferralUrl());
    					record.set("account_balance",event.getAccountBalance());
    					
    					record.set("parameter",parameterArray[0]);
    					record.set("param_1",parameterArray[1]);
    					record.set("param_2",parameterArray[2]);
    					record.set("param_3",parameterArray[3]);
    					record.set("param_4",parameterArray[4]);
    					record.set("param_5",parameterArray[5]);
    					record.set("param_6",parameterArray[6]);
    					record.set("param_7",parameterArray[7]);
    					record.set("param_8",parameterArray[8]);
    					record.set("param_9",parameterArray[9]);
    					record.set("param_10",parameterArray[10]);
    					record.set("param_11",parameterArray[11]);
    					record.set("param_12",parameterArray[12]);
    					record.set("param_13",parameterArray[13]);
    					record.set("param_14",parameterArray[14]);
    					record.set("param_15",parameterArray[15]);
    					record.set("param_16",parameterArray[16]);
    					record.set("param_17",parameterArray[17]);
    					record.set("param_18",parameterArray[18]);
    					record.set("param_19",parameterArray[19]);
    					record.set("param_20",parameterArray[20]);
    					record.set("param_21",parameterArray[21]);
    					record.set("param_22",parameterArray[22]);
    					record.set("param_23",parameterArray[23]);
    					record.set("param_24",parameterArray[24]);
    					record.set("param_25",parameterArray[25]);
    					
    					record.set("timestamp",getTimestamp(event.getTimestamp(), "dateTime"));
    					record.set("systemdttm",ds);
    					
    					try {
    	                    recordWriter.write(record);
    	                } catch (IOException e) {
    	                    e.printStackTrace();
    	                }
    				}
    			}

                /*try {
                    recordWriter.write(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }*/
            }
            recordWriter.close();
            uploadSession.commit(new Long[]{0L});

        } catch (Exception e) {
            context.getLogger().error(e.toString());
            e.printStackTrace();
        }
        context.getLogger().debug("batchInsert end, " + "inserted:" + mapList.size() );
    }

    private static String getTable(GameInfo gameInfo, String buildVersion) {
    	return "allmaster";
        //return "device_master_sandbox_ben_test";
//		if(gameInfo.getLiveBuild() == null || gameInfo.getLiveBuild().size()<=0){
//			return  gameInfo.getDeviceMasterTableName();
//		}else{
//			if(gameInfo.getLiveBuild().contains(buildVersion)){
//				return  gameInfo.getDeviceMasterTableName();
//			}else{
//				return "device_master_sandbox";
//			}re
//		}
    }


    public static Map<String, Object> parseJSON(String jsonStr) {
        Map<String, Object> map = JSON.parseObject(
                jsonStr, new TypeReference<Map<String, Object>>() {
                });

        return map;
    }


    public static void insertIntoMaxCompute(List<String> logs, Context context) {
        List<Map<String, Object>> mapList = logs.stream().map(log -> {
            Map<String, Object> map = parseJSON(log);
            return map;
        }).collect(Collectors.toList());
        String gameSha = null;
        String buildVersion = null;
        try {
            /*for (Map<String, Object> map : mapList) {
                gameSha = map.get("GameID").toString();
                buildVersion = map.get("GameVersion").toString();
                if (gameSha == null || gameSha.equalsIgnoreCase("")) {
                    throw new Exception("null gameid in json");
                }
            }
            // TODO: bug fix;  performance issues of redis;
            GameInfo gameInfo;
            String gameInfoString = getGameInfo(gameSha);
            if (gameInfoString != null) {
                gameInfo = new ObjectMapper().readValue(gameInfoString, GameInfo.class);
                batchInsert(mapList, gameInfo, gameSha, buildVersion, context); //call device_master
            } else {
                throw new Exception("no game info in redis:game id: " + gameSha);
            }*/
        	
        	GameInfo gameInfo=null;
        	batchInsert(mapList, gameInfo, gameSha, buildVersion, context); //call device_master

        } catch (Exception ex) {
            ex.printStackTrace();
            StringBuffer exsb = new StringBuffer();
            exsb.append(ex.getMessage());
            for (StackTraceElement ele : ex.getStackTrace()) {
                exsb.append(ele.getClassName()).append(".").append(ele.getMethodName()).append(":").append(ele.getLineNumber()).append("====");
            }
            context.getLogger().error(exsb.toString());
        }
    }
    
    public static String[] getParameter(String subevent) {
		String arr[] = subevent.split(";");
		String[] subEventArray = {"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "" };

		for (int i = 0; i < arr.length; i++) {
		    if (i == 26) {
			break;
		    }
		    subEventArray[i] = arr[i];
		}
		
		return subEventArray;
	}
    
   public static String getTimestamp(String timestamp, String type){
	   Calendar calendar = Calendar.getInstance();
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

}

