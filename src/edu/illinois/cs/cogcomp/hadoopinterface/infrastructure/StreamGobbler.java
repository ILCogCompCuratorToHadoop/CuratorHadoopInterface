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
        this.is = is;
        this.prefix = prefix;
        this.sb = new StringBuilder();
        this.print = false;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(prefix + " " + line);
                sb.append(line);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String getOutput() {
        return sb.toString();
    }
}
