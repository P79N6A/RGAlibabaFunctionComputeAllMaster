package com.rg.alibaba;

import com.aliyun.fc.runtime.Context;
import com.aliyun.fc.runtime.Credentials;
import com.aliyun.fc.runtime.FunctionComputeLogger;
import com.aliyun.fc.runtime.FunctionParam;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.zip.DataFormatException;

import org.apache.commons.codec.binary.Base64;


public class Main {
    public static void main(String[] args) throws IOException {
        LogHandler functionCompute = new LogHandler();
        //String inputString="{\"parameter\":{},\"source\":{\"endpoint\":\"http://ap-southeast-1.log.aliyuncs.com\",\"projectName\":\"reliancegames\",\"logstoreName\":\"analytics_ben_test\",\"shardId\":0,\"beginCursor\":\"MTU0MzM4OTIyMTM3NDQ4NjAzOQ==\",\"endCursor\":\"MTU0MzM4OTIyMTM3NDUxNTk1Mw==\"},\"jobName\":\"e8189ca4ff4fc17116d3f58cfcfd31f1c44f7ab6\",\"taskId\":\"f114064d-f8a2-4fbd-93f9-9402613c1587\",\"cursorTime\":1542789809}";
         String inputString = " {\"parameter\":{},\"source\":{\"endpoint\":\"http://ap-southeast-1.log.aliyuncs.com\",\"projectName\":\"reliancegames\",\"logstoreName\":\"analytics\",\"shardId\":0,\"beginCursor\":\"MTU0MjI3MTAwNzIwMDg2MDA0Mg==\",\"endCursor\":\"MTU0MjI3MTAwNzIwMDg2MDA0NQ==\"},\"jobName\":\"5547ca1dcd52ee61a55d00956c549b3270830194\",\"taskId\":\"65931f6f-60ee-457d-97cc-7be265a4e624\",\"cursorTime\":1545819494}"; 
        System.out.println(inputString);
        InputStream stream = new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
        Context context=new Context() {
            @Override
            public String getRequestId() {
                return null;
            }

            @Override
            public Credentials getExecutionCredentials() {
                return new Credentials() {
                    @Override
                    public String getAccessKeyId() {
                        return "LTAIgWF0G7WtVA6X";
                    }

                    @Override
                    public String getAccessKeySecret() {
                        return "OStjqSkmnJRqQx6JRAjMtxE0clUIh9";
                    }

                    @Override
                    public String getSecurityToken() {
                        return null;
                    }
                };
            }

            @Override
            public FunctionParam getFunctionParam() {
                return null;
            }

            @Override
            public FunctionComputeLogger getLogger() {
                return new FunctionComputeLogger() {
                    @Override
                    public void trace(String s) {

                    }

                    @Override
                    public void debug(String s) {
                        System.out.println(s);
                    }

                    @Override
                    public void info(String s) {
                        System.out.println(s);
                    }

                    @Override
                    public void warn(String s) {

                    }

                    @Override
                    public void error(String s) {

                    }
                };
            }
        };
        functionCompute.handleRequest(stream,System.out,context);
    	
    	/*//String data = "eJzdl19vmzAQwL8Lr2s2bDB/xhNtaBNtS6NAUlVTZTlwWVAdkxlIVZV+95mGtpNArJMmTYW8cGff5e7nszk/aGM4pDFMx9pnzUXuOl6TNeiW5cYx0k60aQKiSDcpyPNMrkAkmVQTRcm5GlyK9GcJr1O6XXzzz6bJq5ESaTjxkdIgZMDGJesRQwSPTJY4I2ZAMnItC5jFiOM6jrK4YDtYgczTTNRGH5H5EdukGXgKnECi2yYy12sGOHZRnIANDnHMNTYJ2wDaMMtyYKNs5pwVm0zulJWSjsmr98nSvwqmHyL/YvRVR5U/n9KvcACOsZoVQl7/ea59/v7wLHQCwzpykI6JCsbEmGBDGQcHhedoOlPxKqupqFWZvKehYPt8mxV0wcQtXe7pqWR3XIE80aJ0B3nBdntl8Ltb07Jsu46pXEPtRg07toc9vfmpB1memnBWSgkivo/u93BM9lmzYrxsVH4cZ6UoThlnIm50C9iAlIwvJT8qZlDcZfI2LFhRqkS0TPBUgPZ483jyL3A0HqjyL4u+zE1i6a3MDeJh4oXR5eKafrscB9VS8Cy+heT1pZqvAhqsglnUHvTOJQA1cDVWWYskFT8+zFdzavwm+5ynNR1a41VsVTxuhQ1UIeI4nu58wu6nOk7Pm13OgioMLl0azBGdqWhwFfjhdXU+vZhEdBFc+YtxI6ADek+L1FGzZ1sm6QQYpwu1iw5Av6T9y2ch1C7cp+Wry9f0kNcQfudgImC7vwJjG+26HiKYsNyDpKdZlqupvUBIu1LQyxGnDwTHZP4mFhbuZHF83j8FP/ryJgy21fnVGwqGQID8ca9OjE3K+R9QOF0NABkMiqfGq+6GIslEvmeyzrIPiKMbHVsEN13RsJi8oUM0HdTBo8aBnk+OAQG5SnlyxmTSTwST9pbRPffdNs292wbirUhjxvuRGG0kTaMxOCLz7A7kNitz6Edit9uO17vVsJCE2+xux0QfD0KsjjbMfOnEBgHk5ebN8jTuvXdjo32q/qfquHn8BTZlbiU=";
    	String data = "eJzdlm1vmzAQx7/KxOsQ2cY2uH5FgDTRVlblqZqmCrlgOlRqMgdaVaXffSZJ12broqzatKm8AO6OuzO/M39xb4XypkjlOLSOLBiBkIWE2Nghjo2D0LEZc5EdwCDCyHMdSkOrZ40zqeoiL6QeVnohVVZpk4wxw8GQubbjetjGjGHbhxGxB9jxfEKHjjsAJnmuiq+NfCpxeNsTPxhn5nHVlOXGTKYjHxoPGbIwDAi1KcGhjUEY2AxErg1dOAgAGyKzGJNxLK7lQupVUamuax/iPnLBNrAGkDEhGEUezombSg8IQIAkOSYUp8K5cECWgzzFuck5LUWdV/raZBlrA9HcF6cic3pdt6lcdZ1W1tHn+0fjYMoIQA8CRCBBDoSQUlMwujHMNuVis2BT6UMlskJdJrPCmD2ru6xqcb00oWcFPExdul5RcyG7GibM+hRDx/P4SRTP29NFlESLKJ610QJhTJKRKMvqVkr1bt00iT+GEWwjf/qpHY6PR7NkEp35k3BrwBvIj/2T6E/U2R5mtUGjtVTp3exuKTeQHz0LUTZbl5+mVaPqgSiFSre+icyl1qKc63LjiGV9W+mraS3qxvCzKlUWSloP5w+9vzWZjScZFpdf6sQ01vW++WDm7Y7n1fhmuliWctROZ/4kQS1oIQWo77keHxZKGUyVfhZzgMv5ACax2cei5IxySDu/+Sp4/JFzhFtE3sos4iqTyVSWMq07Afj1OBBgP34ur9/OgHMKgLk8keUctm+D7IH6AwmGdBcodPqeB6n3G7oB9oI+XMf213kj+nNmXlzWyXyZiXrPaFzCMIW7o9lJ5VEcdiN65MKnVaNT+TMe+AIfDF4ihCj0/hNKOyodqWwvKArgwaKwf4/NVSZ1La6kjuXtMzk24tBHhPH3QsmNG7awBdzhZ4Xi5n/FZY4JGyVB5LtIg9ac1pt+2hLQdmLzL2Z0/vANDQMHqg==";    	
		try {
			byte[] deocedByte = Base64.decodeBase64(data.getBytes());
	    	byte[] decompressedBytesData = Util.decompress(deocedByte);
			System.out.println(new String(decompressedBytesData));
		} catch (DataFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	
    }
}
