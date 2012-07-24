package edu.illinois.cs.cogcomp.hadoopinterface.infrastructure;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Vivek Srikumar and Ming-Wei Chang
 */
public class StreamGobbler extends Thread {
    InputStream is;
    String prefix;
    private StringBuilder sb;
    private boolean print;


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

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                // if it's not a "comment"
                // (Charniak outputs a few hundred thousand lines of utterly
                // useless diagnostics that begin with ##, but it
                // occassionally has a useful message as well)
                if( !line.contains("##") ) {
                    if( print ) {
                        System.out.println(prefix + " " + line);
                    }
                    sb.append(line);
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String getOutput() {
        return sb.toString();
    }
}
