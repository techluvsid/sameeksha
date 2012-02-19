/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author Administrator
 */
public class PMREPWrapper {

    static OutputStream stdin = null;
    static InputStream stderr = null;
    static InputStream stdout = null;

    public PMREPWrapper() throws Exception{
        String[] env={"INFA_DOMAINS_FILE=C:\\Informatica\\PowerCenterClient\\domains.infa"};
        Process process = Runtime.getRuntime().exec("C:\\Informatica\\PowerCenterClient\\client\\bin\\pmrep.exe",env,new File("C:\\Informatica\\PowerCenterClient\\client\\bin\\temp_cache"));
        stdin = process.getOutputStream();
        stderr = process.getErrorStream();
        stdout = process.getInputStream();
    }
    
    public static void connect () throws Exception{
        String line = "connect -r infa_rep -d Domain_Dev -n admin -x admin" + "\n";
        stdin.write(line.getBytes());
        stdin.flush();
    }
    
    
    
}
