package org.jbpm.process.workitem.camel;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.jbpm.process.workitem.AbstractLogOrThrowWorkItemHandler;
import org.jbpm.process.workitem.camel.request.RequestMapper;
import org.jbpm.process.workitem.camel.request.RequestPayloadMapper;
import org.jbpm.process.workitem.camel.response.ResponseMapper;
import org.jbpm.process.workitem.camel.response.ResponsePayloadMapper;
import org.jbpm.process.workitem.camel.uri.SQLURIMapper;
import org.jbpm.process.workitem.camel.uri.URIMapper;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;

import bitronix.tm.resource.jdbc.PoolingDataSource;

public class CamelHandler extends AbstractLogOrThrowWorkItemHandler {

	private final ResponseMapper responseMapper;
	private final RequestMapper requestMapper;
	private final URIMapper uriConverter;
	private CamelContext context;
	
	@Resource(lookup = "java:jboss/datasources/ExampleDS")
	public static DataSource ds;
	
    private static final String DB_USER = "eusk";
	private static final String DB_PASSWD = "eusk";
	private static final String DB_URL = "jdbc:postgresql://localhost:5432/poc";
	private static final String DB_DRIVER = "org.postgresql.Driver";
	    

	 	
	
	
	public CamelHandler() throws SQLException, URISyntaxException{
		
		this(new SQLURIMapper(), new RequestPayloadMapper("payload"), new ResponsePayloadMapper("Result"));
		
		try {
			InitialContext ctx = new InitialContext();
			ds = (DataSource)ctx.lookup("java:jboss/datasources/ExampleDS");
			
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (ds == null) {
			   ds = setupDataSource();
			}   
			System.out.println("DataSource es "+ds);
	        SimpleRegistry simpleRegistry = new SimpleRegistry();
	        simpleRegistry.put("myDs", ds);
			
			this.context= new DefaultCamelContext(simpleRegistry);

	
		
	}
	
	public CamelHandler(URIMapper converter) {
		this(converter, new RequestPayloadMapper());
	}
	
	public CamelHandler(URIMapper converter, RequestMapper processorMapper) {
		this(converter, processorMapper, new ResponsePayloadMapper());
	}
	
	public CamelHandler(URIMapper converter, RequestMapper processorMapper, ResponseMapper responseMapper) {
		this.uriConverter = converter;
		this.requestMapper = processorMapper;
		this.responseMapper = responseMapper;
	}
	
	public CamelHandler(URIMapper converter, RequestMapper processorMapper, ResponseMapper responseMapper, boolean logException) {
		this(converter, processorMapper, responseMapper);
		setLogThrownException(logException);
	}
	
	public CamelHandler(URIMapper converter, RequestMapper processorMapper, ResponseMapper responseMapper, CamelContext context) {
		this(converter, processorMapper, responseMapper);
		this.context = context;
	}
	
    public static PoolingDataSource setupDataSource() {
        PoolingDataSource pds = new PoolingDataSource();
        pds.setUniqueName("java:jboss/datasources/ExampleDS");
        pds.setClassName("bitronix.tm.resource.jdbc.lrc.LrcXADataSource");
        pds.setMaxPoolSize(5);
        pds.setAllowLocalTransactions(true);
        pds.getDriverProperties().put("user", DB_USER);
        pds.getDriverProperties().put("password", DB_PASSWD);
        pds.getDriverProperties().put("url", DB_URL);
        pds.getDriverProperties().put("driverClassName", DB_DRIVER);
        pds.init();
        return pds;
        
    }
	
	
	private Map<String, Object> send(WorkItem workItem) throws URISyntaxException {
		if (context == null) {
			context = CamelContextService.getInstance();
		}
		ProducerTemplate template = context.createProducerTemplate(); 
		
		Map<String, Object> params = new HashMap<String, Object>(workItem.getParameters());
		// filtering out TaskName 
		params.remove("TaskName");
		Processor processor = requestMapper.mapToRequest(params);
		URI uri = uriConverter.toURI(params);
		String s;
		try {
			s = URLDecoder.decode(uri.toString(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			s = uri.toString();
		}
		Endpoint endpoint = context.getEndpoint(s);
		 
		Exchange exchange = template.send(endpoint, processor);
		return this.responseMapper.mapFromResponse(exchange);
	}
	
	@Override
	public void executeWorkItem(org.kie.api.runtime.process.WorkItem workItem, org.kie.api.runtime.process.WorkItemManager workItemManager) {
		
		Map<String, Object> results = new HashMap<String, Object>();
		try {
			results = this.send(workItem);
			System.out.println("Resultado "+results);
			System.out.println("DataSource es "+ds);
      			
			
		} catch(Exception e) {
			handleException(e);
		}
		workItemManager.completeWorkItem(workItem.getId(), results);
	}
	
	@Override
	public void abortWorkItem(WorkItem workItem, WorkItemManager manager) {
		manager.abortWorkItem(workItem.getId());
	}
	
	
	
}
