package org.jbpm.process.workitem.camel;

import org.jbpm.process.workitem.camel.request.FTPRequestPayloadMapper;
import org.jbpm.process.workitem.camel.request.RequestPayloadMapper;
import org.jbpm.process.workitem.camel.uri.FTPURIMapper;
import org.jbpm.process.workitem.camel.uri.FileURIMapper;
import org.jbpm.process.workitem.camel.uri.GenericURIMapper;
import org.jbpm.process.workitem.camel.uri.SQLURIMapper;

public class CamelHandlerFactory {
	
	public static CamelHandler sftpHandler() {
		return new CamelHandler(new FTPURIMapper("sftp"), new FTPRequestPayloadMapper("payload"));
	}
	
	public static CamelHandler ftpHandler() {
		return new CamelHandler(new FTPURIMapper("ftp"), new FTPRequestPayloadMapper("payload"));
	}
	
	public static CamelHandler ftpsHandler() {
		return new CamelHandler(new FTPURIMapper("ftps"), new FTPRequestPayloadMapper("payload"));
	}
	

	public static CamelHandler fileHandler() {
		return new CamelHandler(new FileURIMapper(), new RequestPayloadMapper("payload"));
	}

	
	public static CamelHandler sqlHandler() {
		return new CamelHandler(new SQLURIMapper(), new RequestPayloadMapper("payload"));
	}
	
	public static CamelHandler genericHandler(String schema, String pathLocation) {
		return new CamelHandler(new GenericURIMapper(schema, pathLocation), new RequestPayloadMapper("payload"));
	}
	
}
