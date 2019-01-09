package com.rg.alibaba;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.Credentials;
import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.fc.runtime.StreamRequestHandler;
import com.aliyun.log.etl_function.common.Consts;
import com.aliyun.log.etl_function.common.FunctionEvent;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Logs.Log;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyuncs.fc.client.FunctionComputeClient;
import com.aliyuncs.fc.config.Config;
import com.aliyuncs.fc.constants.Const;
import com.aliyuncs.fc.request.InvokeFunctionRequest;
import com.aliyuncs.fc.response.InvokeFunctionResponse;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

//import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

public class LogSubscriber implements StreamRequestHandler {

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        Credentials credentials = context.getExecutionCredentials();
        Config config = new Config("ap-southeast-1", "5590233040044099",
                credentials.getAccessKeyId(), credentials.getAccessKeySecret(),credentials.getSecurityToken(),false);
        FunctionComputeClient fcClient = new FunctionComputeClient(config);
        InvokeFunctionRequest invkReq = new InvokeFunctionRequest("rga", "logHandler");
        byte[] payload = IOUtils.toByteArray(inputStream);
        invkReq.setPayload(payload);
        invkReq.setInvocationType(Const.INVOCATION_TYPE_ASYNC);
        fcClient.invokeFunction(invkReq);
        context.getLogger().info("dispatched:"+new String(payload));
    }
}
