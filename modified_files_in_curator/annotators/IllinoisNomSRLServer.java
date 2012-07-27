package edu.illinois.cs.cogcomp.annotation.server;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.illinois.cs.cogcomp.srl.curator.IllinoisSRLServer;
import edu.illinois.cs.cogcomp.srl.main.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IllinoisNomSRLServer {
    private static Logger logger = LoggerFactory.getLogger(IllinoisNomSRLServer.class);
    
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
	    .withDescription("configuration file").create("c");
		
	Option help = new Option("h", "help", false, "print this message");
	Options options = new Options();
	options.addOption(port);
	options.addOption(threads);
	options.addOption(config);
	options.addOption(help);
	return options;
    }


    public static void main(String[] args) {
	int threads = 1;
	int port = 9390;
	String configFile = "";

	CommandLineParser parser = new GnuParser();
	Options options = createOptions();
	HelpFormatter hformat = new HelpFormatter();
	CommandLine line = null;
	try {
	    line = parser.parse(options, args);
	} catch (ParseException e) {
	    logger.error(e.getMessage());
	    hformat.printHelp("java " + IllinoisNomSRLServer.class.getName(),
			      options, true);
	    System.exit(1);
	}
	if (line.hasOption("help")) {
	    hformat.printHelp("java " + IllinoisNomSRLServer.class.getName(),
			      options, true);
	    System.exit(1);
	}

	port = Integer.parseInt(line.getOptionValue("port", "9090"));

	try {
	    threads = Integer.parseInt(line.getOptionValue("threads", "2"));
	} catch (NumberFormatException e) {
	    logger.warn("Couldn't interpret {} as a number.",
			line.getOptionValue("threads"));
	}
	if (threads < 0) {
	    threads = 1;
	} else if (threads == 0) {
	    threads = 2;
	}

	configFile = line.getOptionValue("config", "");
	
	// initialize the configuration 
	SRLConfig.getInstance(configFile);

	// create the curator server
	IllinoisSRLServer server = new IllinoisSRLServer(NomSRLSystem.getInstance(), true);


        // Monitor the annotator's activity and kill it if it's inactive
        // for too long.
        Thread inactivityMonitor =
                new Thread( new InactiveAnnotatorKiller( handler ) );
        inactivityMonitor.start();

	// run the curator server
	server.runServer(port, threads);
    }

    /**
     * Starts a thread that monitors the activity of the annotator.
     * If the annotator is ever inactive for too long, this thread will shut down
     * the server.
     *
     * Note that this is copied, with minor modifications, from CuratorServer.
     */
    private static class InactiveAnnotatorKiller implements Runnable {

        private ClusterGenerator.Iface handler;
        private static long MAX_INACTIVITY_TIME = 1000 * 60 * 5; // 5 mins
        private static long timeBetweenChecks = 1000 * 60 * 1; // 1 min

        private InactiveAnnotatorKiller( ClusterGenerator.Iface handler ) {
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
            logger.info( "Checking time of last annotation operation." );
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
            logger.info( "The annotator was inactive for at least "
                    + inactivityTimeInMins
                    + " minutes. Shutting down . . ." );

            // Server needs to be shut down. Let's kill the whole runtime
            // environment.
            Runtime.getRuntime().exit( 0 );
        }
    }
}
