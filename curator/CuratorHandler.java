package edu.illinois.cs.cogcomp.curator;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.cs.cogcomp.archive.Archive;
import edu.illinois.cs.cogcomp.archive.ArchiveException;
import edu.illinois.cs.cogcomp.archive.Identifier;
import edu.illinois.cs.cogcomp.legacy.SRLHandler;
import edu.illinois.cs.cogcomp.thrift.base.AnnotationFailedException;
import edu.illinois.cs.cogcomp.thrift.base.BaseService;
import edu.illinois.cs.cogcomp.thrift.base.Clustering;
import edu.illinois.cs.cogcomp.thrift.base.Forest;
import edu.illinois.cs.cogcomp.thrift.base.Labeling;
import edu.illinois.cs.cogcomp.thrift.base.ServiceSecurityException;
import edu.illinois.cs.cogcomp.thrift.base.ServiceUnavailableException;
import edu.illinois.cs.cogcomp.thrift.base.View;
import edu.illinois.cs.cogcomp.thrift.cluster.ClusterGenerator;
import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import edu.illinois.cs.cogcomp.thrift.curator.MultiRecord;
import edu.illinois.cs.cogcomp.thrift.curator.Record;
import edu.illinois.cs.cogcomp.thrift.labeler.Labeler;
import edu.illinois.cs.cogcomp.thrift.labeler.MultiLabeler;
import edu.illinois.cs.cogcomp.thrift.parser.MultiParser;
import edu.illinois.cs.cogcomp.thrift.parser.Parser;
import edu.illinois.cs.cogcomp.util.Pair;
import edu.illinois.cs.cogcomp.util.StringUtil;

/**
 * Curator
 * 
 * Implements the Curator.
 * 
 * @author James Clarke
 * 
 */
public class CuratorHandler implements Curator.Iface {

	private final Logger logger = LoggerFactory.getLogger(CuratorHandler.class);
	private final String SHORTNAME = "curator";
	private final String VERSION = "0.6";
	
	private final int CLIENTTIMEOUT;

	private final Map<String, AtomicInteger> counters = new HashMap<String, AtomicInteger>();
	private final Map<String, AtomicInteger> timers = new HashMap<String, AtomicInteger>();

	private final boolean slave;
	private final boolean writeaccess;
	private final String masterHost;
	private final int masterPort;

	private final Map<String, Pool> pools = new HashMap<String, Pool>();
	private final Map<String, String> clientSourceIdentifiers = new ConcurrentHashMap<String, String>();
	private final Map<String, String> clientNames = new ConcurrentHashMap<String, String>();
	private final Map<String, List<String>> multiFields = new HashMap<String, List<String>>();
	private final Map<String, List<String>> requirements = new HashMap<String, List<String>>();
	private final Map<String, List<String>> invertedRequirements = new HashMap<String, List<String>>();

	private Archive archive;

	public CuratorHandler() {
		this("", "", "");
	}

	public CuratorHandler(String propfn, String annotatorsfn) {
		this(propfn, annotatorsfn, "");
	}

    /**
     * Constructs a CuratorHandler
     * @param propfn A string containing a path (relative to the current working
     *               directory) to the Curator configuration files. If this is
     *               blank, we use the default.
     * @param annotatorsfn A path to the annotator configuration file. If blank,
     *                     we use the default.
     * @param archivefn A path to the archive handler's configuration file. If
     *                  blank, we use no archive handler.
     */
	public CuratorHandler(String propfn, String annotatorsfn, String archivefn) {
		// Set the configuration based on our inputs
        if (propfn.trim().equals("")) {
			propfn = "configs/curator.properties";
		}
		if (annotatorsfn.trim().equals("")) {
			annotatorsfn = "configs/annotators.xml";
		}
		Configuration config = null;
		XMLConfiguration annotatorscfg = null;
		try {
			config = new PropertiesConfiguration(propfn);
			annotatorscfg = new XMLConfiguration(annotatorsfn);
		} catch (ConfigurationException e) {
			logger.error("Error reading configuration file. {}", e);
			System.exit(1);
		}
		String archiveClassname = config.getString("archive",
				"edu.illinois.cs.cogcomp.archive.DatabaseArchive");

		// initialize the archive class
		try {
			if (!archivefn.trim().equals("")) {
				init(archiveClassname, new PropertiesConfiguration(archivefn));
			} else {
				init(archiveClassname, null);
			}
		} catch (ConfigurationException e) {
			logger.error("Error reading archive configuration file. {}", e);
			System.exit(1);
		}

		// details if we are a slave
        // TODO add hadoop bool toggle functionality, slave = local?
		slave = config.getBoolean("curator.slave", false);

		writeaccess = config.getBoolean("curator.writeaccess", false);

		if (slave) {
			String[] split = config.getString("servers.master").split(":");
			masterHost = split[0];
			masterPort = Integer.parseInt(split[1]);
			logger.info("Curator is a slave to {}",
					config.getString("servers.master"));
		} else {
			masterHost = "";
			masterPort = 0;
		}

		CLIENTTIMEOUT = config.getInt("client.timeout", 45) * 1000;

		int CLIENTCOUNT = 1; // how many clients per host:port combination

		// let's create the client pools
		if (!slave) {
			
			List<HierarchicalConfiguration> annotators =
					annotatorscfg.configurationsAt("annotator");

			for (HierarchicalConfiguration annotator : annotators) {
			
				ClientPool cpool = new ClientPool(CLIENTTIMEOUT);
				Pool pool = null;
				
				String type = annotator.getString("type");
				
				String[] hosts = annotator.getStringArray("host");
				String[] fields = annotator.getStringArray("field");
				
				String[] requirements = annotator.getStringArray("requirement");
				String local = annotator.getString("local", "");

				
				logger.debug( "creating new annotator client: type is '" + type +
						", first host " + ( hosts.length > 0 ? ( "'" + hosts[0] + "'" ) : "NULL" ) +
						", first field " + ( fields.length > 0 ? ( "'" + fields[0] + "'" ) : "NONE" ) +
						", local: '" + ( local == null ? "NULL" : local ) );
				
				if (!local.equals("")) {
					BaseService.Iface service = initClassInstance(local);
					pool = new MockPool(service);
				} else if (type.equals("labeler")) {
					cpool.addClients(hosts, CLIENTCOUNT, Labeler.Client.class);
					pool = cpool;
				} else if (type.equals("multilabeler")) {
					cpool.addClients(hosts, CLIENTCOUNT,
							MultiLabeler.Client.class);
					pool = cpool;
				} else if (type.equals("clustergenerator")) {
					cpool.addClients(hosts, CLIENTCOUNT,
							ClusterGenerator.Client.class);
					pool = cpool;
				} else if (type.equals("parser")) {
					cpool.addClients(hosts, CLIENTCOUNT, Parser.Client.class);
					pool = cpool;
				} else if (type.equals("multiparser")) {
					cpool.addClients(hosts, CLIENTCOUNT,
							MultiParser.Client.class);
					pool = cpool;
				} else if (type.equals("legacySRL")) {
					cpool.addLegacyClients(hosts, 1, SRLHandler.class);
					pool = cpool;
				} else {
					logger.error("Unknown annotator type: {}", type);
					logger.error("Exiting...");
					System.exit(-1);
				}// end if conditions

				for (int i = 0; i < fields.length; i++) {
					String field = fields[i];
					pools.put(field, pool);
					counters.put(field, new AtomicInteger());
					timers.put(field, new AtomicInteger());
					if (requirements.length > 0) {
						// store requirements
						this.requirements.put(field,Arrays.asList(requirements));
						logger.info(field + " requires "
								+ this.requirements.get(field));
						for (String dependency : this.requirements.get(field)) {
							if (!invertedRequirements.containsKey(dependency)) {
								invertedRequirements.put(dependency, new ArrayList<String>());
							}
							invertedRequirements.get(dependency).add(field);
						}
					}
					// store information about fields when multiple annotations
					// are returned by
					// annotators
					if (fields.length > 1) {
						multiFields.put(field, Arrays.asList(fields));
					}
				}// end for fields
			}
		}

		// setup reporter thread
		final long reportTime = config.getLong("curator.reporttime", 5) * 60 * 1000;
		startReporter(reportTime);

		// setup version thread
		final long versionTime = config.getLong("curator.versiontime", 30) * 60 * 1000;
		startVersionUpdater(versionTime);
	}

	
	private String getPoolReport() {
		StringBuffer poolreport = new StringBuffer();
		poolreport.append("pool status: ");
		for (String name : pools.keySet()) {
			poolreport.append(name);
			poolreport.append("[");
			poolreport.append(pools.get(name).getStatusReport());
			poolreport.append("]");
			poolreport.append(" ");
		}
		return poolreport.toString();
	}
	
	/**
	 * @param interval
	 */
	private void startReporter(final long interval) {
		Thread reporter = new Thread("Curator Report") {
			public void run() {
				for (;;) {
					logger.info(getStatusReport());
					logger.info(getPoolReport());
					try {
						Thread.sleep(interval);
					} catch (InterruptedException e) {
					}
				}
			}
		};
		reporter.start();
	}

	/**
	 * @param interval
	 */
	private void startVersionUpdater(final long interval) {
		Thread versionUpdater = new Thread("Client Version Updater") {
			public void run() {
				for (;;) {
					logger.debug("Attempting version refresh");
					for (String name : pools.keySet()) {
						BaseService.Iface c = null;
						String ident = null;
						String serviceName = null;
						try {
							c = (BaseService.Iface) pools.get(name).getClient();
							ident = c.getSourceIdentifier();
							serviceName = c.getName();
							pools.get(name).releaseClient(c);
						} catch (TException e) {
							logger.error(
									"Error talking with client named '" + name 
									+ "' with ident '" + ident + "', service name '{}': {}",
									serviceName, e.getMessage());
							if (c != null) {
								pools.get(name).releaseClient(c);
							}
							continue;
						}
						if (!clientSourceIdentifiers.containsKey(name)
								|| !clientSourceIdentifiers.get(name).equals(
										ident)) {
							clientSourceIdentifiers.put(name, ident);
						}
						if (!clientNames.containsKey(name)
								|| !clientNames.get(name).equals(serviceName)) {
							clientNames.put(name, serviceName);
						}

					}
					logger.info("{}", clientSourceIdentifiers);
					try {
						Thread.sleep(interval);
					} catch (InterruptedException e) {
					}
				}
			}
		};
		versionUpdater.start();
	}

	private BaseService.Iface initClassInstance(String classname) {
		Class<?> clazz = null;
		BaseService.Iface result = null;
		try {
			clazz = Class.forName(classname);
		} catch (ClassNotFoundException e2) {
			logger.error("Couldn't find class: {}", e2);
			System.exit(1);
		}
		Constructor<?> constr = null;
		try {
			constr = clazz.getConstructor(new Class[] {});
		} catch (SecurityException e1) {
			logger.error("Error finding class constructor: {}", e1);
			System.exit(1);
		} catch (NoSuchMethodException e1) {
			logger.error("Error finding class constructor: {}", e1);
			System.exit(1);
		}
		try {
			result = (BaseService.Iface) constr.newInstance(new Object[] {});
		} catch (IllegalArgumentException e) {
			logger.error("Error creating instance of class: {}", e);
			System.exit(1);
		} catch (InstantiationException e) {
			logger.error("Error creating instance of class: {}", e);
			System.exit(1);
		} catch (IllegalAccessException e) {
			logger.error("Error creating instance of class: {}", e);
			System.exit(1);
		} catch (InvocationTargetException e) {
			logger.error("Error creating instance of class: {}", e);
			System.exit(1);
		}
		return result;
	}

	/**
	 * Initializes the archive class
	 * 
	 * @param archiveClassname
	 */
	private void init(String archiveClassname, Configuration archiveConfig) {
		Class<?> archiveClass = null;
		try {
			archiveClass = Class.forName(archiveClassname);
		} catch (ClassNotFoundException e2) {
			logger.error("Couldn't find archive class: {}", e2);
			System.exit(1);
		}
		Constructor<?> constr = null;
		try {
			if (archiveConfig != null) {
				constr = archiveClass.getConstructor(new Class[] { Configuration.class });
			} else {
				constr = archiveClass.getConstructor(new Class[] {});
			}
		} catch (SecurityException e1) {
			logger.error("Error finding archive class constructor: {}", e1);
			System.exit(1);
		} catch (NoSuchMethodException e1) {
			logger.error("Error finding archive class constructor: {}", e1);
			System.exit(1);
		}
		try {
			if (archiveConfig != null) {
				archive = (Archive) constr.newInstance(new Object[] { archiveConfig });
			} else {
				archive = (Archive) constr.newInstance(new Object[] {});
			}
		} catch (IllegalArgumentException e) {
			logger.error("Error creating instance of archive class: {}", e);
			System.exit(1);
		} catch (InstantiationException e) {
			logger.error("Error creating instance of archive class: {}", e);
			System.exit(1);
		} catch (IllegalAccessException e) {
			logger.error("Error creating instance of archive class: {}", e);
			System.exit(1);
		} catch (InvocationTargetException e) {
			logger.error("Error creating instance of archive class: {}", e);
			System.exit(1);
		}
		
	}

	/**
	 * Creates a new client on the fly. Only used by slave curators to connect
	 * to master
	 * 
	 * @param hostname
	 * @param port
	 * @param clientClass
	 * @return
	 */
	private Pair<TTransport, Object> createClient(String hostname, int port,
			Class<?> clientClass) {

		logger.info( "Creating client with host '" + hostname + "', port '" + port + "'." );
		
		TTransport transport = new TSocket(hostname, port, CLIENTTIMEOUT);
		transport = new TFramedTransport(transport);

		TProtocol protocol = new TBinaryProtocol(transport);
		try {
			Constructor<?> c = clientClass.getConstructor(TProtocol.class);
			return new Pair<TTransport, Object>(transport,
					c.newInstance(protocol));
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	public boolean ping() throws TException {
		logger.debug("PONG!");
		return true;
	}

	public boolean isCacheAvailable() throws TException {
		return true;
	}

	public String getName() throws TException {
		return "Curator";
	}

	public String getVersion() throws TException {
		return VERSION;
	}

	public String getSourceIdentifier() throws TException {
		return SHORTNAME + getVersion();
	}

	private Record getRecord(String text, boolean whitespaced)
			throws ServiceUnavailableException, TException,
			AnnotationFailedException {
		if (text.trim().equals("")) {
			throw new AnnotationFailedException(
					"Cannot annotate the empty string");
		}
		logger.debug("Asking archive for record");
		Record record = null;
		try {
			record = archive.get(text, whitespaced, Record.class);
		} catch (ArchiveException e1) {
			logger.error("Error retrieving from Archive.", e1);
		}
		if (record == null) {
			if (slave) {
				Pair<TTransport, Object> tc = createClient(masterHost,
						masterPort, Curator.Client.class);
				TTransport transport = tc.first;
				Curator.Client client = (Curator.Client) tc.second;
				try {
					transport.open();
					record = client.getRecord(text);
					transport.close();
				} catch (TException e) {
					if (transport.isOpen())
						transport.close();
					logger.warn("Problem communicating with master curator");
					if (e.getCause() != null) {
						logger.warn(e.getCause().toString());
					} else {
						logger.warn(e.toString());
					}
				}
				// salves should also store records when they request from
				// master (cut down on comms)
				try {
					archive.store(record, Record.class);
				} catch (ArchiveException e) {
					logger.error("Error writing to Archive.", e);
				}
			} else if (record == null) {
				logger.debug("Created new record");
				record = new Record();
				record.setRawText(text);
				record.setWhitespaced(whitespaced);
				record.setLabelViews(new HashMap<String, Labeling>());
				record.setClusterViews(new HashMap<String, Clustering>());
				record.setParseViews(new HashMap<String, Forest>());
				record.setViews(new HashMap<String, View>());
				record.setIdentifier(Identifier.getId(text, whitespaced));
			}
		} else {
			logger.debug("Record provided by archive");
		}
		removeStaleFields(record);
		return record;
	}

	public Record getRecordById(String identifier)
			throws ServiceUnavailableException, AnnotationFailedException,
			TException {
		Record record = null;
		try {
			record = archive.getById(identifier, Record.class);
		} catch (ArchiveException e1) {
			logger.error("Error retrieving from Archive.", e1);
		}
		if (record == null && slave) {
			Pair<TTransport, Object> tc = createClient(masterHost, masterPort,
					Curator.Client.class);
			TTransport transport = tc.first;
			Curator.Client client = (Curator.Client) tc.second;
			try {
				transport.open();
				record = client.getRecordById(identifier);
				transport.close();
			} catch (TException e) {
				if (transport.isOpen())
					transport.close();
				logger.warn("Problem communicating with master curator");
				if (e.getCause() != null) {
					logger.warn(e.getCause().toString());
				} else {
					logger.warn(e.toString());
				}
			}
		}
		if (record == null) {
			throw new AnnotationFailedException(
					"Unable to locate record with identifier: " + identifier);
		}
		removeStaleFields(record);
		return record;
	}

	/**
	 * Removes any field that was produced by an old annotator.
	 * 
	 * @param record
	 */
	private void removeStaleFields(MultiRecord record) {
		//TODO: Implement!
	}

	/**
	 * Removes any field that was produced by an old annotator.
	 * 
	 * @param record
	 */
	private void removeStaleFields(Record record) {
		removeStaleFields(record, record.getLabelViews());
		removeStaleFields(record, record.getClusterViews());
		removeStaleFields(record, record.getParseViews());
		removeStaleFields(record, record.getViews());
	}

    
// 	private void removeStaleFields(Record record, Map<String, ?> views) {
// 		for (Iterator<String> it = views.keySet().iterator(); it.hasNext();) {
// 			String view_name = it.next();
// 			if (updateRequired(view_name, record)) {
// 				logger.debug("Removing stale annotation {}", view_name);
// 				it.remove();
// 				cascadeRemoveStaleFields(view_name, record);
// 			}
// 		}
// 	}


    private void removeStaleFields(Record record, Map<String, ?> views) {
	//this will track what we have removed
	Set<String> removals = new HashSet<String>();
	
	for (Iterator<String> it = views.keySet().iterator(); it.hasNext();) {
	    String view_name = it.next();
	    if (updateRequired(view_name, record)) {
		logger.debug("Removing stale annotation {}", view_name);
		it.remove();
		removals.add(view_name);
	    }
	}
	//now we cascade the removals
	for (String view_name : removals) {
	    cascadeRemoveStaleFields(view_name, record);
	}
    }


	/**
	 * Removes any field that depends on the view_dependency field.
	 * This is useful for removing all fields that require another field that has 
	 * recently been updated.
	 * 
	 * @param view_dependency
	 * @param record
	 */
	private void cascadeRemoveStaleFields(String view_dependency, Record record) {
		cascadeRemoveStaleFields(view_dependency, record, record.getLabelViews());
		cascadeRemoveStaleFields(view_dependency, record, record.getClusterViews());
		cascadeRemoveStaleFields(view_dependency, record, record.getParseViews());
		cascadeRemoveStaleFields(view_dependency, record, record.getViews());
	}
	
	private void cascadeRemoveStaleFields(String view_name, Record record, Map<String, ?> views) {
		if (!invertedRequirements.containsKey(view_name)) {
			return;
		}
		for (String dependent : invertedRequirements.get(view_name)) {
			if (views.containsKey(dependent)) {
				logger.debug("Removing {} annotation because it depends on {}", dependent, view_name);
				views.remove(dependent);
			}
		}
	}
	
	public void storeRecord(Record record) throws ServiceSecurityException,
			TException {
		if (slave || writeaccess) {
			try {
				archive.store(record, Record.class);
			} catch (ArchiveException e) {
				logger.error("Error storing to Archive.", e);
			}
		} else {
			throw new ServiceSecurityException(
					"Curator does not support storeRecord");
		}

	}

	/**
	 * This does all the work. The other provide* methods just call this with
	 * the appropriate transformer, counter and timer.
	 * 
	 * @param record
	 * @param store
	 * @param forceUpdate
	 * @param transformer
	 * @throws ServiceUnavailableException
	 * @throws TException
	 * @throws AnnotationFailedException
	 */
	public void performAnnotation(Record record, String view_name,
			boolean forceUpdate) throws ServiceUnavailableException,
			AnnotationFailedException, TException {

		long startTime = System.currentTimeMillis();
		// if not slave....
		if (!slave) {
			logger.debug("Finding annotator for {}", view_name);
			Pool pool = pools.get(view_name);

			// we need to check we can provide this annotation
			if (!pools.containsKey(view_name)) {
				logger.info("Couldn't find an annotator for {}", view_name);
				throw new ServiceUnavailableException(
						"The Curator does not know of any annotators for "
								+ view_name
								+ ". Check the annotators.xml config file or call describeAnnotations().");
			}
			Object client = null;
			try {
				client = pool.getClient();
				transform(record, view_name, client);
				if (client != null)
					pool.releaseClient(client);
			} catch (AnnotationFailedException fail) {
				if (client != null)
					pool.releaseClient(client);
				throw fail;
			} catch (TException e) {
				// check that we've released the client
				if (client != null)
					pool.releaseClient(client);
				// propagate error to calling client
				logger.warn("Unhandled TException (probably a problem in the underlying annotator");
				logger.warn("Input sentence: {}", record.getRawText());
				if (e.getCause() != null) {
					logger.warn("{} : {}", view_name, e.getCause().toString());
					throw new ServiceUnavailableException(view_name
							+ " unavailable:" + e.getCause().toString());
				} else {
					logger.warn("{} : {}", view_name, e.toString());
					throw new ServiceUnavailableException(view_name
							+ " unavailable:" + e.toString());
				}
			} catch (Exception e) {
				if (client != null)
					pool.releaseClient(client);
				logger.error("Unexpected Exception!", e);
			}
			// else if slave
		} else {
			logger.debug("Calling master Curator for {}", view_name);
			Pair<TTransport, Object> tc = createClient(masterHost, masterPort,
					Curator.Client.class);
			TTransport transport = tc.first;
			try {
				transport.open();
				transformMaster(record, view_name, forceUpdate, tc.second);
				transport.close();
			} catch (ServiceUnavailableException e) {
				if (transport.isOpen())
					transport.close();
				throw e;
			} catch (TException e) {
				if (transport.isOpen())
					transport.close();
				logger.warn("Problem communicating with master curator");
				if (e.getCause() != null) {
					logger.warn(e.getCause().toString());
					throw new ServiceUnavailableException(e.getCause()
							.toString());
				} else {
					logger.warn(e.toString());
					throw new ServiceUnavailableException(e.toString());
				}
			}
		}
		long endTime = System.currentTimeMillis();
		// update count and time for status reports
		counters.get(view_name).incrementAndGet();
		timers.get(view_name).addAndGet((int) (endTime - startTime));
		// finally store record
		
		try {
			archive.store(record, Record.class);
		} catch (ArchiveException e) {
			logger.error("Error storing the record.", e);
		}
	}

	/**
	 * Calls the master curator and adds the requested field.
	 * 
	 * @param record
	 * @param view_name
	 * @param forceUpdate
	 * @param client
	 * @throws ServiceUnavailableException
	 * @throws AnnotationFailedException
	 * @throws TException
	 */
	private void transformMaster(Record record, String view_name,
			boolean forceUpdate, Object client)
			throws ServiceUnavailableException, AnnotationFailedException,
			TException {
		Curator.Client c = (Curator.Client) client;
		Record masterRecord = c.provide(view_name, record.getRawText(),
				forceUpdate);
		if (masterRecord.getLabelViews().containsKey(view_name)) {
			record.getLabelViews().put(view_name,
					masterRecord.getLabelViews().get(view_name));
		} else if (masterRecord.getClusterViews().containsKey(view_name)) {
			record.getClusterViews().put(view_name,
					masterRecord.getClusterViews().get(view_name));
		} else if (masterRecord.getParseViews().containsKey(view_name)) {
			record.getParseViews().put(view_name,
					masterRecord.getParseViews().get(view_name));
		} else if (masterRecord.getViews().containsKey(view_name)) {
			record.getViews().put(view_name,
					masterRecord.getViews().get(view_name));
		}
	}

	private boolean containsView(String view_name, Record record) {
		return record.getLabelViews().containsKey(view_name)
				|| record.getClusterViews().containsKey(view_name)
				|| record.getParseViews().containsKey(view_name)
				|| record.getViews().containsKey(view_name);
	}

	/**
	 * Is an annotation out of date with respect to the current service?
	 * 
	 * @param view_name
	 * @param record
	 * @return
	 */
	private boolean updateRequired(String view_name, Record record) {
		String clientSource = clientSourceIdentifiers.get(view_name);
		String annotationSource = null;

		// special case for whitespace tokenizer!
		if (view_name.equals("tokens") && record.isWhitespaced()) {
			if (!record.getLabelViews().get("tokens").getSource()
					.equals(Whitespacer.getSourceIdentifier())) {
				return true;
			} else {
				return false;
			}
		}
		if (view_name.equals("sentences") && record.isWhitespaced()) {
			if (!record.getLabelViews().get("sentences").getSource()
					.equals(Whitespacer.getSourceIdentifier())) {
				return true;
			} else {
				return false;
			}
		}
		// end special case for whitespace tokenizer

		if (record.getLabelViews().containsKey(view_name)) {
			Labeling labeling = record.getLabelViews().get(view_name);
			annotationSource = labeling.getSource();
		} else if (record.getParseViews().containsKey(view_name)) {
			Forest forest = record.getParseViews().get(view_name);
			annotationSource = forest.getSource();
		} else if (record.getClusterViews().containsKey(view_name)) {
			Clustering cluster = record.getClusterViews().get(view_name);
			annotationSource = cluster.getSource();
		} else if (record.getViews().containsKey(view_name)) {
			View view = record.getViews().get(view_name);
			annotationSource = view.getSource();
		}

		if (annotationSource == null) {
			return true;
		}
		
		logger.debug(view_name
				+ " has service source: {} current annotation source: {}",
				clientSource, annotationSource);
		
		if (clientSource == null) {
			// we don't have any information about the service so we assume it
			// is ok!
			return false;
		}

		if (annotationSource.equals(clientSource)) {
			return false;
		}

		int osplit = annotationSource.lastIndexOf("-");
		int csplit = clientSource.lastIndexOf("-");
		if (osplit == -1)
			return true;

		if (!annotationSource.substring(0, osplit).equals(
				clientSource.substring(0, csplit)))
			return true;

		double oversion = 0.0;
		double cversion = 0.0;

		try {
			oversion = Double.valueOf(annotationSource.substring(osplit + 1));
		} catch (NumberFormatException e) {
			return true;
		}

		try {
			cversion = Double.valueOf(clientSource.substring(csplit + 1));
		} catch (NumberFormatException e) {
			logger.warn("Unable to parse version number in {}", clientSource);
		}

		return cversion > oversion;
	}

	/**
	 * Calls the underlying annotation server and populates the appropriate
	 * views
	 * 
	 * @param record
	 *            Record to populate
	 * @param view_name
	 *            view name for the annotation
	 * @param client
	 *            client to the annotation server
	 * @throws AnnotationFailedException
	 * @throws TException
	 */
	private void transform(Record record, String view_name, Object client)
			throws AnnotationFailedException, TException {
		if (client instanceof Labeler.Iface) {
			// Handle Labelers
			Labeler.Iface c = (Labeler.Iface) client;
			Labeling labeling = c.labelRecord(record);
			record.getLabelViews().put(view_name, labeling);
		} else if (client instanceof MultiLabeler.Iface) {
			// Handle MultiLabelers
			MultiLabeler.Iface c = (MultiLabeler.Iface) client;
			List<Labeling> labelings = c.labelRecord(record);
			List<String> fields = multiFields.get(view_name);
			Map<String, Labeling> labelViews = record.getLabelViews();
			for (int i = 0; i < labelings.size(); i++) {
				if (i >= fields.size()) {
					logger.warn(
							"Too many labelings returned by {} annotator; maybe not enough fields specified in annotators.xml?",
							view_name);
					break;
				}
				labelViews.put(fields.get(i), labelings.get(i));
			}
		} else if (client instanceof ClusterGenerator.Iface) {
			// Handle ClusterGenerators
			ClusterGenerator.Iface c = (ClusterGenerator.Iface) client;
			Clustering clustering = c.clusterRecord(record);
			record.getClusterViews().put(view_name, clustering);
		} else if (client instanceof Parser.Iface) {
			// Handle Parsers
			Parser.Iface c = (Parser.Iface) client;
			Forest parse = c.parseRecord(record);
			record.getParseViews().put(view_name, parse);
		} else if (client instanceof MultiParser.Iface) {
			// Handle MultiParsers
			MultiParser.Iface c = (MultiParser.Iface) client;
			List<Forest> forests = c.parseRecord(record);
			List<String> fields = multiFields.get(view_name);
			Map<String, Forest> parseViews = record.getParseViews();
			for (int i = 0; i < forests.size(); i++) {
				if (i >= fields.size()) {
					logger.error(
							"Too many forests returned by {} annotator; maybe not enough fields specified in annotators.xml?",
							view_name);
					break;
				}
				parseViews.put(fields.get(i), forests.get(i));
			}
		} else {
			logger.error("Unknown client type!");
		}
	}

	public MultiRecord getMultiRecord(List<String> texts)
			throws ServiceUnavailableException, TException,
			AnnotationFailedException {
		MultiRecord multiRec = null;
		try {
			multiRec = archive.get(texts, MultiRecord.class);
		} catch (ArchiveException e) {
			logger.error("Error getting MultiRecord from Archive.", e);
		}
		if (multiRec == null) {
			multiRec = new MultiRecord();
			List<String> records = new ArrayList<String>();
			for (String text : texts) {
				Record record = getRecord(text);
				records.add(record.getIdentifier());
			}
			multiRec.setRecords(records);
			multiRec.setIdentifier(Identifier.getId(texts));
			try {
				archive.store(multiRec, MultiRecord.class);
			} catch (ArchiveException e) {
				logger.error("Error storing MultiRecord in Archive.", e);
			}
		}
		removeStaleFields(multiRec);
		return multiRec;
	}

	/**
	 * Get the status report.
	 * 
	 * @return
	 */
	private String getStatusReport() {
		StringBuffer result = new StringBuffer();
		for (String name : counters.keySet()) {
			result.append(getStatusItem(name, counters.get(name),
					timers.get(name)));
			result.append(" | ");
		}
		return result.toString();
	}

	private String getStatusItem(String name, AtomicInteger count,
			AtomicInteger time) {
		int c = count.getAndSet(0);
		int t = time.getAndSet(0);
		int a = c == 0 ? 0 : t / c;
		return String.format("%s: %d %dms", name, c, a);
	}

	public void storeMultiRecord(MultiRecord record)
			throws ServiceSecurityException, TException {
		if (slave || writeaccess) {
			try {
				archive.store(record, MultiRecord.class);
			} catch (ArchiveException e) {
				logger.error("Error storing MultiRecord in Archive");
			}
		} else {
			throw new ServiceSecurityException(
					"Curator does not support storeMultiRecord");
		}

	}

	public Record provide(String view_name, String text, boolean forceUpdate)
			throws ServiceUnavailableException, AnnotationFailedException,
			TException {
		logger.debug(getPoolReport());
		logger.debug("Annotation requested: {}", view_name);
		Record record = getRecord(text);
		// check requirements
		if (requirements.containsKey(view_name)) {
			for (String requirement : requirements.get(view_name)) {
				forceUpdate = forceUpdate
						|| !containsView(requirement, record);
				if (forceUpdate) {
					record = provide(requirement, record.getRawText(),
							forceUpdate);
				}
			}
		}
		forceUpdate = forceUpdate || updateRequired(view_name, record);
		if (forceUpdate) {
			logger.debug("Annotation {} is going to be performed.", view_name);
			performAnnotation(record, view_name, forceUpdate);
		}
		return record;
	}

	public Record wsgetRecord(List<String> sentences)
			throws ServiceUnavailableException, AnnotationFailedException,
			TException {
		logger.debug(getPoolReport());
		String rawText = StringUtil.join(sentences, " ");
		Record record = getRecord(rawText, true);
		// if record has no sentences annotation
		// or the sentence annotation is stale update!
		if (!record.getLabelViews().containsKey("sentences")
				|| !record.getLabelViews().get("sentences").getSource()
						.equals(Whitespacer.getSourceIdentifier())) {
			Labeling sents = Whitespacer.sentences(sentences);
			record.getLabelViews().put("sentences", sents);

		}
		// ditto for tokens
		if (!record.getLabelViews().containsKey("tokens")) {
			Labeling tokens = Whitespacer.tokenize(sentences);
			record.getLabelViews().put("tokens", tokens);
		}
		return record;
	}

	public Map<String, String> describeAnnotations() throws TException {
		Map<String, String> result = new HashMap<String, String>();
		for (String view_name : pools.keySet()) {
			StringBuilder sb = new StringBuilder();
			sb.append(clientNames.get(view_name));
			sb.append(" identifies as ");
			sb.append(clientSourceIdentifiers.get(view_name));
			String description = sb.toString();
			if (multiFields.containsKey(view_name)) {
				for (String v : multiFields.get(view_name)) {
					result.put(v, description);
				}
			} else {
				result.put(view_name, description);
			}
		}
		return result;
	}

	public Record wsprovide(String view_name, List<String> sentences,
			boolean forceUpdate) throws ServiceUnavailableException,
			AnnotationFailedException, TException {
		logger.debug("Annotation requested: {}", view_name);
		Record record = wsgetRecord(sentences);
		// check requirements
		if (requirements.containsKey(view_name)) {
			for (String requirement : requirements.get(view_name)) {
				forceUpdate = forceUpdate
						|| !containsView(requirement, record);
				if (forceUpdate) {
					record = wsprovide(requirement, sentences, forceUpdate);
				}
			}
		}
		forceUpdate = forceUpdate || updateRequired(view_name, record);
		if (forceUpdate) {
			// special case for whitespace tokenizer!
			if (view_name.equals("sentences") || view_name.equals("tokens")) {
				Labeling sents = Whitespacer.sentences(sentences);
				record.getLabelViews().put("sentences", sents);
				Labeling tokens = Whitespacer.tokenize(sentences);
				record.getLabelViews().put("tokens", tokens);
				// end special case
			} else {
				logger.debug("Annotation {} is going to be performed.",
						view_name);
				performAnnotation(record, view_name, forceUpdate);
			}
		}
		return record;
	}

	public MultiRecord provideMulti(String view_name, List<String> texts,
			boolean forceUpdate) throws ServiceUnavailableException,
			AnnotationFailedException, TException {
		throw new AnnotationFailedException("provideMulti not yet implemented!");
	}

	public Record getRecord(String text) throws ServiceUnavailableException,
			AnnotationFailedException, TException {
		return getRecord(text, false);
	}
}
