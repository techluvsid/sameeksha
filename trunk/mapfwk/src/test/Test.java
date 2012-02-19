/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

/**
 *
 * @author Administrator
 */
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkRetrieveContext;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;
import util.MapHelper;

public class Test {

    protected Repository rep;
    protected String mapFileName;
    protected Transformation rank;

    protected void createRepository() {
        rep = new Repository("PowerCenter", "PowerCenter", "This repository contains API test samples");
    }

    protected void init() throws IOException {
        createRepository();
        Properties properties = new Properties();
        String filename = "pcconfig.properties";
        InputStream propStream = getClass().getClassLoader().getResourceAsStream(filename);

        if (propStream != null) {
            properties.load(propStream);
            rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, "C:\\Informatica\\PowerCenterClient\\client\\bin");
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME, "infa_rep");
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_DOMAIN_NAME, "Domain_Dev");
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD, "admin");
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME, "admin");
        } else {
            throw new IOException("pcconfig.properties file not found.");
        }
    }

    public void execute() throws Exception {
        // initialise the repository configurations.
        init();
        RepositoryConnectionManager repmgr = new PmrepRepositoryConnectionManager();
        //CachedRepositoryConnectionManager cmgr = new CachedRepositoryConnectionManager(repmgr);
        rep.setRepositoryConnectionManager(repmgr);
        rep.getRepositoryConnectionManager().connect();
    }
    
    public void listmappings() throws Exception{
        Vector folders = rep.getFolder();
        int folderSize = folders.size();

        long begin = System.currentTimeMillis();
        for (int i = 0; i < folderSize; i++) {
            Vector listOfMappings = ((Folder) folders.get(i)).getMappings();
        }
                
        //Vector maps= rep.getRepositoryConnectionManager().listMappings((Folder)folders.get(0));
    }

    public void runme() throws Exception{

        // get the list of folder names which satisfies filter condition
        Vector folders = rep.getFolder(new NameFilter() {

            public boolean accept(String name) {
                return name.equals("Dev");
            }
        });

        
        //folder count - in this case it is always 1
        int folderSize = folders.size();

        long begin = System.currentTimeMillis();
        for (int i = 0; i < folderSize; i++) {
            Vector listOfMappings = ((Folder) folders.get(i)).getMappings(new NameFilter() {
                public boolean accept(String name) {
                    return name.contains("m_test");
                }
            }); //get the list of mappings
           
            Mapping m = (Mapping) listOfMappings.get(0);
            TransformHelper helper = new TransformHelper(m);
            MapHelper mh=new MapHelper(m);
            //mh.getChild("COUNTRIES1.REGION_ID");
            //OutputSet outSet = helper.sourceQualifier( (Source)m.getSource().get(0) );
            //RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
            //System.out.println(m.getName());
            
        }
        //System.out.println(System.currentTimeMillis() - begin);
    }

    /**
     * not expecting any arguments
     */
    public static void main(String[] args) {
        try {
            Test repo = new Test();
            repo.execute();
            repo.runme();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception is: " + e.getMessage());
        }

    }
}
