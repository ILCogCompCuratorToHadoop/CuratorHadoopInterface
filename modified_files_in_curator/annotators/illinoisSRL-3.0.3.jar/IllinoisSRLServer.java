package edu.illinois.cs.cogcomp.srl.curator;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;

import edu.illinois.cs.cogcomp.srl.main.SRLSystem;
import edu.illinois.cs.cogcomp.thrift.parser.Parser;

public class IllinoisSRLServer {

	private static Logger log = LoggerFactory
			.getLogger(IllinoisSRLHandler.class);
	private final boolean beamSearch;
	private final SRLSystem srlSystem;

	public IllinoisSRLServer(SRLSystem srlSystem, boolean beamSearch) {
		this.srlSystem = srlSystem;
		this.beamSearch = beamSearch;

	}

	public void runServer(int port, int numThreads) {
		Parser.Iface handler = new IllinoisSRLHandler(this.srlSystem, this.beamSearch);
		Parser.Processor processor = new Parser.Processor(handler);

		TNonblockingServerTransport serverTransport;
		TServer server;

		try {
			serverTransport = new TNonblockingServerSocket(port);

			if (numThreads == 1) {
				server = new TNonblockingServer(processor, serverTransport);
			} else {
				THsHaServer.Options serverOptions = new THsHaServer.Options();
				serverOptions.workerThreads = numThreads;
				server = new THsHaServer(new TProcessorFactory(processor),
						serverTransport, new TFramedTransport.Factory(),
						new TBinaryProtocol.Factory(), serverOptions);

			}

			Runtime.getRuntime().addShutdownHook(
					new Thread(new ShutdownListener(server, serverTransport),
							"Server shutdown listener"));

			log.info("Starting the server on port {} with {} threads", port,
					numThreads);

            // Monitor the annotator's activity and kill it if it's inactive
            // for too long.
            Thread inactivityMonitor =
                    new Thread( new InactiveAnnotatorKiller( handler ) );
            inactivityMonitor.start();

			server.serve();

		} catch (TTransportException e) {
			log.error("Thrift transport error", e);
			System.exit(-1);
		}
	}

    /**
     * Starts a thread that monitors the activity of the annotator.
     * If the annotator is ever inactive for too long, this thread will shut down
     * the server.
     *
     * Note that this is copied, with minor modifications, from CuratorServer.
     */
    private static class InactiveAnnotatorKiller implements Runnable {

        private Parser.Iface handler;
        private static long MAX_INACTIVITY_TIME = 1000 * 60 * 5; // 5 mins
        private static long timeBetweenChecks = 1000 * 60 * 1; // 1 min

        private InactiveAnnotatorKiller( Parser.Iface handler ) {
            this.handler = handler;
        }

        /**
         * Returns true if the handler has not performed an annotation in
         * a long time. This is important on Hadoop, where the annotators have to
         * kill their own processes after a period of inactivity (otherwise, we
         * could accidentally leave many annotators running after we're done).
         * @return True if the last annotation performed by the annotator took
         *         place a long time ago.
         */
        private boolean serverNeedsToDie() throws TException {
            log.info( "Checking time of last annotation operation." );
            long lastAnnoTime = handler.getTimeOfLastAnnotation();
            long now = System.currentTimeMillis();
            long diff = now - lastAnnoTime;
            log.info( "Last annotation performed " + ((double)(diff/1000))/60
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
                log.error( "Thrift exception while checking the time "
                        + "of last annotation!" );
            }

            double inactivityTimeInMins = ((double)(MAX_INACTIVITY_TIME/1000))/60;
            log.info( "The annotator was inactive for at least "
                    + inactivityTimeInMins
                    + " minutes. Shutting down . . ." );

            // Server needs to be shut down. Let's kill the whole runtime
            // environment.
            Runtime.getRuntime().exit( 0 );
        }
    }

	private static class ShutdownListener implements Runnable {
        private final TServer server;

		private final TNonblockingServerTransport serverTransport;

		public ShutdownListener(TServer server,
				TNonblockingServerTransport serverTransport) {
			this.server = server;
			this.serverTransport = serverTransport;
		}

		@Override
		public void run() {
			if (server != null) {
				server.stop();
			}

			if (serverTransport != null) {
				serverTransport.interrupt();
				serverTransport.close();
			}
		}


	}
}
