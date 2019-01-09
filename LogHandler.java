package com.rg.alibaba;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.Credentials;
import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.fc.runtime.StreamRequestHandler;
import com.aliyun.log.etl_function.common.Consts;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Logs;
import com.aliyun.openservices.log.common.Logs.Log;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.DataFormatException;

//import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

public class LogHandler implements StreamRequestHandler {

    private final static int MAX_RETRY_TIMES = 30;
    private final static int RETRY_SLEEP_MILLIS = 200;
    private final static int QUOTA_RETRY_SLEEP_MILLIS = 2000;

    //for debug use
    public  static String BeginCursor;
    public  static String EndCurosr;

    private FunctionComputeLogger logger = null;
    private FunctionEvent event = null;
    private String accessKeyId = "";
    private String accessKeySecret = "";
    private String securityToken = "";

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        context.getLogger().debug("handleRequest start");
        this.logger = context.getLogger();
        this.event = new FunctionEvent(this.logger);
        this.event.parseFromInputStream(inputStream);
        Credentials credentials = context.getExecutionCredentials();
        this.accessKeyId = credentials.getAccessKeyId();
        this.accessKeySecret = credentials.getAccessKeySecret();
        this.securityToken = credentials.getSecurityToken();

        String logProjectName = this.event.getLogProjectName();
        String logLogstoreName = this.event.getLogLogstoreName();
        int logShardId = this.event.getLogShardId();
        String logBeginCursor = this.event.getLogBeginCursor();
        String logEndCurosr = this.event.getLogEndCursor();

        //for debug use
        BeginCursor=logBeginCursor;
        EndCurosr=logEndCurosr;

        Client sourceClient = new Client(this.event.getLogEndpoint(), this.accessKeyId, this.accessKeySecret);
        sourceClient.SetSecurityToken(this.securityToken);
        String cursor = logBeginCursor;
        Map<String, List<String>> mapIpRga = new HashMap<String, List<String>>();
        List<String> logList2 = new ArrayList<>();
        while (true) {
            List<LogGroupData> logGroupDataList = null;
            String nextCursor = "";
            int retryTime = 0;
            int invalidRetryTime = 0;
            while (true) {
                String errorCode, errorMessage;
                int sleepMillis = RETRY_SLEEP_MILLIS;
                try {
                    BatchGetLogResponse logDataRes = sourceClient.BatchGetLog(logProjectName, logLogstoreName, logShardId,
                            1000, cursor, logEndCurosr);
                    logGroupDataList = logDataRes.GetLogGroups();
                    nextCursor = logDataRes.GetNextCursor();
                    break;
                } catch (LogException le) {
                    errorCode = le.GetErrorCode();
                    errorMessage = le.GetErrorMessage().replaceAll("\\n", " ");
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES + ", error_code: "
                            + errorCode + ", error_message: " + errorMessage + ", request_id: " + le.GetRequestId());
                    if (errorCode.equalsIgnoreCase("ReadQuotaExceed") || errorCode.equalsIgnoreCase("ShardReadQuotaExceed")) {
                        sleepMillis = QUOTA_RETRY_SLEEP_MILLIS;
                    } else if (errorCode.equalsIgnoreCase("Unauthorized") || errorCode.equalsIgnoreCase("InvalidAccessKeyId")
                            || errorCode.equalsIgnoreCase("ProjectNotExist") || errorCode.equalsIgnoreCase("LogStoreNotExist")
                            || errorCode.equalsIgnoreCase("ShardNotExist")) {
                        ++invalidRetryTime;
                    }
                } catch (Exception e) {
                    errorCode = "UnknownException";
                    errorMessage = e.getMessage().replaceAll("\\n", " ");
                    this.logger.warn("BatchGetLog fail, project_name: " + logProjectName + ", job_name: " + this.event.getJobName()
                            + ", task_id: " + this.event.getTaskId() + ", retry_time: " + retryTime + "/" + MAX_RETRY_TIMES
                            + ", error_code: " + errorCode + ", error_message: " + errorMessage);
                }
                if (invalidRetryTime >= 2 || retryTime >= MAX_RETRY_TIMES) {
                    throw new IOException("BatchGetLog fail, retry_time: " + retryTime + ", error_code: " + errorCode
                            + ", error_message: " + errorMessage);
                }
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
                ++retryTime;
            }

            for (LogGroupData logGroupData : logGroupDataList) {
				try {
					List<Logs.Log> logsList = logGroupData.GetLogGroup().getLogsList();
					String source = logGroupData.GetLogGroup().getSource();
                    for (Log log : logsList) {
						String ip = null;
						String isCompressed = null;
						String rgaData = null;
						for (int i = 0; i < log.getContentsCount(); i++){
							switch (log.getContents(i).getKey().trim()) {
								case Consts.IP_PARAM:
									ip = log.getContents(i).getValue();
									break;
								case Consts.RGA_PARAM:
									rgaData = log.getContents(i).getValue();
									break;
								case Consts.ISCOMPRESSED_PARAM:
									isCompressed = log.getContents(i).getValue().toLowerCase();
									break;
								default:
									break;
							}
						}

                            if(isCompressed != null && isCompressed.equalsIgnoreCase("true")){
                                byte[] base64DecededByte = Base64.decodeBase64(rgaData.getBytes());
                                byte[] decompressedBytesData = Util.decompress(base64DecededByte);
                                rgaData=new String(decompressedBytesData);
                            }
                            if(ip == null || ip.equalsIgnoreCase("")){
                                ip =source;
                            }

                            List<String> logsForIp = mapIpRga.getOrDefault(ip,null);
						    if(logsForIp==null){
						        logsForIp=new ArrayList<>();
						        mapIpRga.put(ip,logsForIp);
                            }
                            logsForIp.add(rgaData);

                    }// end outer for
					
				} catch (LogException | DataFormatException ex) {
					StringBuffer exsb = new StringBuffer();
					exsb.append(ex.getMessage()+":");
		            for(StackTraceElement ele :ex.getStackTrace()){
		                exsb.append(ele.getClassName()).append(".").append(ele.getMethodName()).append(":").append(ele.getLineNumber()).append("====");
		            }
		            context.getLogger().error(exsb.toString());
				}
            }
            cursor = nextCursor;
            if (cursor.equals(logEndCurosr)) {
            	handleData(mapIpRga,context);
                break;
            }
        }
        context.getLogger().debug("handleRequest end");
    }

	private void handleData(Map<String, List<String>> mapIpRga, Context context) {
        List<String> allLogs=new ArrayList<>();

		for (Map.Entry<String , List<String>> entry : mapIpRga.entrySet()) {
			String ip = entry.getKey();
			List<String> logList = mapIpRga.get(ip);
	        Object[] ilist = logList.stream().filter(s -> {
	            return s.trim().startsWith("{") && s.trim().endsWith("}");
	        }).toArray();
	        String[] array = new String[ilist.length];
	        for(int index=0;index<array.length;index++){
	            String json = ilist[index].toString().trim();
	            array[index] = logList.get(index).trim();
	        }

            for (String log: array) {
                String logWithIp=log.replaceFirst("\\{","{\"ip\": \""+ip+"\",");
                allLogs.add(logWithIp);
            }
		}

        try {
            MaxComputeTunnelWriter.insertIntoMaxCompute(allLogs,context);
        }catch (Exception ex){
            StringBuffer exsb = new StringBuffer();
            for(StackTraceElement ele :ex.getStackTrace()){
                exsb.append(ele.getClassName()).append(".").append(ele.getMethodName()).append(":").append(ele.getLineNumber()).append("====");
            }
            context.getLogger().error(exsb.toString());
        }

    }


}
