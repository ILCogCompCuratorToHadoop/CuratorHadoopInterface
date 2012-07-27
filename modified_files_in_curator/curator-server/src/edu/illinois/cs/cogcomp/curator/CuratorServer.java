package edu.illinois.cs.cogcomp.curator;

import edu.illinois.cs.cogcomp.thrift.curator.Curator;
import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* 
* @author James Clarke
* 
*/
public class CuratorServer {

	private static Logger logger = LoggerFactory.getLogger(CuratorServer.class);

	/*
	 * most this code is boiler plate. All you need to modify is createOptions
	 * and main to get a server working!
	 */
	public static Options createOptions() {

		Option port = OptionBuilder.withLongOpt("port").withArgName("PORT")
				.hasArg().withDescription("port to open server on").create("p");

		Option threads = OptionBuilder.withLongOpt("threads")
				.withArgName("THREADS").hasArg()
				.withDescription("number of threads to run").create("t");

		Option config = OptionBuilder.withLongOpt("config")
				.withArgName("CONFIG").hasArg()
				.withDescription("configuration file (curator.properties)")
				.create("c");

		Option annotators = OptionBuilder
				.withLongOpt("annotators")
				.withArgName("ANNOTATORS")
				.hasArg()
				.withDescription(
						"annotators configuration file (annotators.xml)")
				.create("a");

		Option archive = OptionBuilder
				.withLongOpt("archiveconfig").withArgName("ARCHIVECONFIG").hasArg()
				.withDescription("archiver configuration file (configs/database.properties)")
				.create("r");

		Option help = new Option("h", "help", false, "print this message");

		Options options = new Options();

		options.addOption(port);
		options.addOption(threads);
		options.addOption(config);
		options.addOption(annotators);
		options.addOption(archive);
		options.addOption(help);

		return options;
	}

	public static void main(String[] args) {

		int threads = 1;
		int port = 9090;

		String configFile = "";
		String annotatorsFile = "";
		String archiveConfigFile = "";

		CommandLineParser parser = new GnuParser();
		Options options = createOptions();

		HelpFormatter hformat = new HelpFormatter();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			logger.error(e.getMessage());
			hformat.printHelp("java " + CuratorServer.class.getName(), options,
					true);
			System.exit(1);
		}
		if (line.hasOption("help")) {
			hformat.printHelp("java " + CuratorServer.class.getName(), options,
					true);
			System.exit(1);
		}

		port = Integer.parseInt(line.getOptionValue("port", "9090"));

		try {
			threads = Integer.parseInt(line.getOptionValue("threads", "25"));
		} catch (NumberFormatException e) {
			logger.warn("Couldn't interpret {} as a number.",
					line.getOptionValue("threads"));
		}
		if (threads < 0) {
			threads = 1;
		} else if (threads == 0) {
			threads = 25;
		}

		configFile = line.getOptionValue("config", "");
		annotatorsFile = line.getOptionValue("annotators", "");
		archiveConfigFile = line.getOptionValue("archiveconfig", "");

		Curator.Iface handler = new CuratorHandler(configFile, annotatorsFile, archiveConfigFile);
		Curator.Processor processor = new Curator.Processor(handler);

		runServer(processor, port, threads, handler );
	}

    /**
     * Starts the Thrift server.
     *
     * @param processor The Curator processor (constructed from a CuratorHandler)
     * @param port
     * @param threads
     */
	public static void runServer( TProcessor processor, int port, int threads,
                                  Curator.Iface handler ) {

		TNonblockingServerTransport serverTransport;
		TServer server;
		try {
			serverTransport = new TNonblockingServerSocket(port);

			if (threads == 1) {
				server = new TNonblockingServer( processor, serverTransport );
			} else {
				THsHaServer.Options serverOptions = new THsHaServer.Options();
				serverOptions.workerThreads = threads;
				server = new THsHaServer(new TProcessorFactory(processor),
						serverTransport, new TFramedTransport.Factory(),
						new TBinaryProtocol.Factory(), serverOptions);
			}
			Runtime.getRuntime().addShutdownHook(
					new Thread(new ShutdownListener(server, serverTransport),
							"Server Shutdown Listener"));
			logger.info("Starting the server on port {} with {} threads", port,
					threads);
            Thread inactivityMonitor = new Thread( new InactiveCuratorKiller( handler ) );
            inactivityMonitor.start();
			server.serve();
		} catch (TTransportException e) {
			logger.error("Thrift Transport error");
			logger.error(e.toString());
			System.exit(1);
		}
    }

    /**
     * Starts a thread that monitors the activity of the CuratorHandler.
     * If the Curator is ever inactive for too long, this thread will shut down
     * the server.
     */
    private static class InactiveCuratorKiller implements Runnable {

        private Curator.Iface handler;
        private static long MAX_INACTIVITY_TIME = 1000 * 60 * 5; // 5 mins
        private static long timeBetweenChecks = 1000 * 60 * 1; // 1 min

        private InactiveCuratorKiller( Curator.Iface handler ) {
            this.handler = handler;
        }

        /**
         * Returns true if the CuratorHandler has not performed an annotation in
         * a long time. This is important on Hadoop, where the Curator has to
         * kill its own process after a period of inactivity (otherwise, we could
         * accidentally leave many Curators running after we're done).
         * @return True if the last annotation performed by the Curator took place
         *         a long time ago.
         */
        private boolean serverNeedsToDie() throws TException {
            logger.info( "Checking time of Curator's last annotation operation." );
            long lastAnnoTime = handler.getTimeOfLastAnnotation();
            long now = System.currentTimeMillis();
            long diff = now - lastAnnoTime;
            logger.info( "Last annotation performed " + ((double)(diff/1000))/60
                         + " minutes ago." );
            return diff >= MAX_INACTIVITY_TIME;
        }

        @Override
        public void run() {
            try {
                while( !serverNeedsToDie() ) {
                    try {
                        Thread.sleep( timeBetweenChecks );
                    } catch ( InterruptedException ignored ) { }
                }
            } catch ( TException e ) {
                logger.error( "Thrift exception while checking the time "
                              + "of last annotation!" );
            }

            double inactivityTimeInMins = ((double)(MAX_INACTIVITY_TIME/1000))/60;
            logger.info( "The Curator Server was inactive for at least "
                         + inactivityTimeInMins
                         + " minutes. Shutting down . . ." );

            // Server needs to be shut down. Let's kill the whole runtime
            // environment.
            Runtime.getRuntime().exit( 0 );
        }
    }

	private static class ShutdownListener implements Runnable {

		private final TServer server;
		private final TServerTransport transport;

		public ShutdownListener(TServer server, TServerTransport transport) {
			this.server = server;
			this.transport = transport;
		}

		public void run() {
			if (server != null) {
				server.stop();
			}
			if (transport != null) {
				transport.interrupt();
				transport.close();
			}
		}
	}
}
