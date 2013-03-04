/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 *
 * @author Administrator
 */
public class Pmrep {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        String line;
        OutputStream stdin = null;
        InputStream stderr = null;
        InputStream stdout = null;
        //Runtime.getRuntime().exec(line, args, null) 
        // launch EXE and grab stdin/stdout and stderr
        String[] env={"INFA_DOMAINS_FILE=C:\\Informatica\\9.1.0\\domains.infa"};
        Process process = Runtime.getRuntime().exec("C:\\Informatica\\9.1.0\\clients\\PowerCenterClient\\client\\bin\\pmrep.exe",env,new File("C:\\Informatica\\temp_cache"));
        stdin = process.getOutputStream();
        stderr = process.getErrorStream();
        stdout = process.getInputStream();
        System.out.println("Connect now");
        // "write" the parms into stdin
        
        line = "\n";
        stdin.write(line.getBytes());
        stdin.flush();
        
        line = "connect -r infa_rep -d Domain_envy -n rishav -x admin" + "\n";
        stdin.write(line.getBytes());
        stdin.flush();
        
        System.out.println("connect ");

        line = "listobjects -o folder" + "\n";
        stdin.write(line.getBytes());
        stdin.flush();

        // clean up if any output in stdout
        BufferedReader brCleanUp =
                new BufferedReader(new InputStreamReader(stdout));
        while ((line = brCleanUp.readLine()) != null) {
            System.out.println ("[Stdout] " + line);
        }
        brCleanUp.close();

        // clean up if any output in stderr
        brCleanUp =
                new BufferedReader(new InputStreamReader(stderr));
        while ((line = brCleanUp.readLine()) != null) {
            System.out.println ("[Stderr] " + line);
        }
        brCleanUp.close();
        stdin.close();
    }
}
