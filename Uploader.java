package com.rg.alibaba;

import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;

public class Uploader{
	private RecordWriter recordWriter;

	public Uploader(RecordWriter recordWriter, TableTunnel.UploadSession uploadSession) {
		this.recordWriter = recordWriter;
		this.uploadSession = uploadSession;
	}

	private TableTunnel.UploadSession uploadSession;

	public RecordWriter getRecordWriter() {
		return recordWriter;
	}

	public TableTunnel.UploadSession getUploadSession() {
		return uploadSession;
	}

}
