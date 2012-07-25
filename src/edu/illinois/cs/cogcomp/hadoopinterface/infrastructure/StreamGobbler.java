package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.*;

/**
 * @author Vivek Srikumar and Ming-Wei Chang
 */
public class StreamGobbler extends Thread {
    InputStream is;
    String prefix;
    private StringBuilder sb;
    private boolean print;
    private File logFile;
    private boolean hasLogFile;
    private static final String NL = System.getProperty("line.separator");


    public StreamGobbler( InputStream is, String prefix ) {
        this(is, prefix, true);
    }

    public StreamGobbler( InputStream inputStream,
                          String prefix, boolean alsoPrintToStdOut ) {
        this.is = inputStream;
        this.prefix = prefix;
        this.sb = new StringBuilder();
        this.print = alsoPrintToStdOut;
    }

    /**
     * Constructs a StreamGobbler that writes to the indicated file. If the
     * @param inputStream
     * @param prefix
     * @param alsoPrintToStdOut
     * @param logFile
     * @TODO: Document!
     */
    public StreamGobbler( InputStream inputStream,
                          String prefix, boolean alsoPrintToStdOut,
                          File logFile ) {
        this( inputStream, prefix, alsoPrintToStdOut );
        if( logFile != null && logFile.isFile() ) {
            this.logFile = logFile;
            hasLogFile = true;
        }
    }

    public void run() {
        try {
            BufferedWriter writer = null;
            if( hasLogFile ) {
                writer = new BufferedWriter( new FileWriter( logFile, true ) );
            }

            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                // if it's not a "comment"
                // (Charniak outputs a few hundred thousand lines of utterly
                // useless diagnostics that begin with ##, but it
                // occassionally has a useful message as well)
                if( !line.contains("##") ) {
                    line = prefix + " " + line + NL;
                    if( print ) {
                        System.out.print( line );
                    }

                    if( hasLogFile ) {
                        writer.write( line );
                    }

                    sb.append(line);
                }
            }

            if( hasLogFile ) {
                writer.close();
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    public String getOutput() {
        return sb.toString();
    }
}
