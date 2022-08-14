package com.mongodb.app;
import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.Attribute;

import java.text.SimpleDateFormat;
import java.util.*;

import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.result.InsertOneResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class jmx2mongo {

    public static void main(String[] args) throws Exception{

    final Map<String, List<String>> params = new HashMap<>();
    List<String> options = null;

    //Define default values
    String ServiceURL="service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi";
    String MongoDBCXN = "mongodb://localhost";
    Boolean bWriteToConsole=false;
    int iSampleMS=5000;
    String sDatabaseName="jmx2mongo";
    String sCollectionName="metrics_ts";
    String sObjectName=""; 

    //Sample object names
    //sObjectName="com.mongodb.kafka.connect:type=source-task-metrics,task=*";

    try{
        if (args.length>0)
        {
        
            //Parse arguments
            for (int i = 0; i < args.length; i++) {
                final String a = args[i];
            
                if (a.charAt(0) == '-') {
                    if (a.length() < 2) {
                        System.err.println("Error at argument " + a);
                        return;
                    }
            
                    options = new ArrayList<>();
                    params.put(a.substring(1), options);
                }
                else if (options != null) {
                    options.add(a);
                }
                else {
                    System.err.println("Illegal parameter usage");
                    return;
                }
            }
            if (params.get("h")!=null || params.get("help")!=null) {
                System.out.println(("\nJmxMongo - Stream JMX events to MongoDB time-series collection\n\n\nOptions:\n\n -service [JMX Service Url]  (default: service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi)\n -objectname [Name of MBean, or a pattern matching the names of many MBeans] (REQUIRED)\n -mongo [MongoDB Connection String] (default: mongodb://localhost)\n -database [Destination database name] (default: jmx2mongo)\n -collection [Destination collection name] (default: metrics_ts)\n -console (Logs metrics to console)\n\n"));
                System.exit(0);
            }
            if (params.get("console")!=null) {
                System.out.println(("\nLog to console\n\n"));
                bWriteToConsole=true;
            }
            
            if (params.get("mongo")!=null) { MongoDBCXN=params.get("mongo").get(0); }
            if (params.get("service")!=null) { ServiceURL=params.get("service").get(0); }
            if (params.get("console")!=null) { bWriteToConsole=true;}
            if (params.get("database")!=null) { sDatabaseName=params.get("database").get(0); }
            if (params.get("collection")!=null) { sCollectionName=params.get("collection").get(0); }
            if (params.get("objectname")!=null) { sObjectName=params.get("objectname").get(0);}
            
        }

        if (sObjectName.length()==0) { System.out.println("\n\nJMX2MONGO - Missing ObjectName\n\nExample Usage:\n\njava -jar jmx2mongo.jar -objectname \"com.mongodb:name=*,type=MongoDBKafkaConnector\"\n\n"); System.exit(0); }
        System.out.println("\nJMX2MONGO - MBean attribute value copy tool\n\nConnecting to JMX Service URL - " + ServiceURL );
        JMXServiceURL url=new JMXServiceURL(ServiceURL); //"service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url,null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        System.out.println("\nConnecting to MongoDB - " + MongoDBCXN + "\n\n" );
      
        //Open MongoDB Connection
       try (MongoClient mongoClient = MongoClients.create(MongoDBCXN)) {

        MongoDatabase database = mongoClient.getDatabase(sDatabaseName);
        boolean collectionExists = database.listCollectionNames().into(new ArrayList<String>()).contains(sCollectionName);
      
        //If collection doesn't exist, create it as a time-series collection
        if (!collectionExists)
        {
            System.out.println("\"" + sCollectionName + "\" collection not found, creating new time series collection");
            TimeSeriesOptions tsOptions = new TimeSeriesOptions("sample_time");
            tsOptions.metaField("mbean");
            CreateCollectionOptions collOptions = new CreateCollectionOptions().timeSeriesOptions(tsOptions);
            database.createCollection(sCollectionName, collOptions);
        }
        else{
            System.out.println("\n\"" + sCollectionName + "\" collection exists!  For best perfomanceuse a time series collection.");
        }

        MongoCollection<Document> collection = database.getCollection(sCollectionName);

        System.out.println("\n\nQuerying " + sObjectName + "\n\n");

       Set<ObjectInstance> objectInstanceNames=mbsc.queryMBeans(new ObjectName(sObjectName),null); 
       
       //TEMP
       System.out.println("\n\nFound the following MBeans:\n");
       Set<ObjectName> objectIN=mbsc.queryNames(new ObjectName(sObjectName),null);
       for (ObjectName c : objectIN) {
        
        System.out.println(c.getKeyPropertyListString());
       }


        Date dLastWritten=new Date();
        SimpleDateFormat sdfLastWritten = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println("\n\nStarted " +sdfLastWritten.format(dLastWritten.getTime()) + "\nMBean queried every " + (double)iSampleMS/1000 + "s\nPress Control-C to cancel\n\n");

        int i=0; // Counter for total data points written

        while (true) {
        for (ObjectInstance c : objectInstanceNames) {
        
            ObjectName x = c.getObjectName();
       //     System.out.println(x.getKeyPropertyListString());
            //Future note: May need to tweak this getKeyProperty as the Kafka Connector exposes "type" but not all MBeans use that some use "name"
            String mbean_name=x.getKeyProperty("type");
            MBeanInfo info= mbsc.getMBeanInfo(x);
            MBeanAttributeInfo[] attribute=info.getAttributes();
            Document new_doc = new Document().append("_id", new ObjectId()).append("sample_time", new Date()).append("mbean", mbean_name);
           // .append("attributes", Arrays.asList("value1", "value2")));

            if (bWriteToConsole) System.out.println("\n\nMBean = " + mbean_name + "\n\n");

            for(MBeanAttributeInfo attr : attribute){
                
                Object attributeValue = mbsc.getAttribute(x, attr.getName()).toString();

                if (attr.isReadable())
                {
                    if (bWriteToConsole) System.out.println(attr.getName() + " = " + attributeValue.toString()); //mbsc.getAttributes(x, new String[]{attr.getName()}));
             
                    switch(attr.getType()) {
                        case "long":
                            new_doc.append(attr.getName(), Long.parseLong(attributeValue.toString()));
                            break;
                        case "integer":
                            new_doc.append(attr.getName(),Integer.parseInt(attributeValue.toString()));
                            break;
                        default:
                            System.out.println("Type of Value = " + attr.getType());
                            new_doc.append(attr.getName(),attributeValue.toString());
                    }
            
                 } // isreadable
                
            }
            InsertOneResult result = collection.insertOne(new_doc);
            i=i+1;
            dLastWritten=new Date();
            System.out.print("\rTotal data points written " + i + " (last written: " + sdfLastWritten.format(dLastWritten.getTime()) + ")");

        }
        Thread.sleep(iSampleMS);

        }
        
        } catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        }

        jmxc.close();
     }catch (Exception e){
        System.out.println(e.getMessage());
        System.exit(0);
     }

    
}
}

     //If the user wants to enumerate Domains

        /* 
        ObjectName mbeanName = new ObjectName("com.mongodb:type=MongoDBKafkaConnector,name=\"CacheSize\"");
Set<ObjectName> objectInstanceNames = mBeanServer.queryNames(objn, null);
for (ObjectName on : objectInstanceNames) {
    // query a number of attributes at once
    AttributeList attrs = mBeanServer.getAttributes(on, new String[] {"ExchangesCompleted","ExchangesFailed"});
    // process attribute values (beware of nulls...)
    // ... attrs.get(0) ... attrs.get(1) ...
}*/

        //working - ObjectName mbeanName = new ObjectName("com.mongodb:name=SourceTask0,type=MongoDBKafkaConnector"); //com.mongodb:type=MongoDBKafkaConnector"); //kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount");
    //    ObjectName mbeanName = new ObjectName("com.mongodb:type=MongoDBKafkaConnector"); //com.mongodb:type=MongoDBKafkaConnector"); //kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount");
     //   ObjectName mbeanName = new ObjectName("com.mongodb:name=SourceTask0,type=MongoDBKafkaConnector");
    /*
    public static void main(String[] args) throws Exception {
// set a self JMX connection
MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();


// set the object name(s) you are willing to query, here a CAMEL JMX object
ObjectName objn = new ObjectName("com.example:type=Int,name=\"CacheSize\"");
Set<ObjectName> objectInstanceNames = mBeanServer.queryNames(objn, null);
for (ObjectName on : objectInstanceNames) {
    // query a number of attributes at once
    AttributeList attrs = mBeanServer.getAttributes(on, new String[] {"ExchangesCompleted","ExchangesFailed"});
    // process attribute values (beware of nulls...)
    // ... attrs.get(0) ... attrs.get(1) ...
}


        System.out.println("Hello, World!");
    }
    */