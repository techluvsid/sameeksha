/**
 * ListObjects.java Created on Jan, 2008.
 * Copyright (c) 2008 Informatica Corporation.  All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * This example is to list all source, target and mapping names in a given folder of a repository 
 * @author kbhat
 */

public class ListObjects {
	// Instance variables	
    protected Repository rep;
    protected String mapFileName;
    protected Transformation rank;

    protected void createRepository() {
        rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );
    }

    /**
     * Initialise the repository configurations.
     */
    protected void init() throws IOException {
        createRepository();
        Properties properties = new Properties();
        String filename = "pcconfig.properties";
        InputStream propStream = getClass().getClassLoader().getResourceAsStream( filename);
        
        if ( propStream != null ) {
            properties.load( propStream );
			
            rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, "C:\\Informatica\\PowerCenterClient\\client\\bin");
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME, "infa_rep");
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_DOMAIN_NAME, "Domain_Dev");
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD,"admin");
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME, "admin");
        }
        else {
            throw new IOException( "pcconfig.properties file not found.");
        }
    }

    /**
     * Initialize the method
     */    
    public void execute() throws Exception {
    	// initialise the repository configurations.
        init();
        RepositoryConnectionManager repmgr = new PmrepRepositoryConnectionManager();
        rep.setRepositoryConnectionManager(repmgr);

        // get the list of folder names which satisfies filter condition
        Vector folders = rep.getFolder(new NameFilter() {
            public boolean accept(String name) {
            	return name.equals("Dev");
            }
        });
        
        //folder count - in this case it is always 1
        int folderSize = folders.size();
        
        for(int i=0 ; i < folderSize; i++){
        	Vector listOfSources = ((Folder)folders.get(i)).getSource(); //ge tthe list of sources
        	int listSize = listOfSources.size();
			System.out.println(" ***** List of Sources ******");
        	for(int j=0; j < listSize; j++){
        		System.out.println(((Source)listOfSources.get(j)).getName());
        	}
        }
        
        for(int i=0 ; i < folderSize; i++){
        	Vector listOfTargets = ((Folder)folders.get(i)).getTarget(); //get the list of targets
        	int listSize = listOfTargets.size();
			System.out.println(" ***** List of Targets ******");
        	for(int j=0; j < listSize; j++){
        		System.out.println(((Target)listOfTargets.get(j)).getName());
        	}
        }
        
        for(int i=0 ; i < folderSize; i++){
            Vector listOfMapplets = ((Folder)folders.get(i)).getMapplets(); //get the list of mapplets
            int listSize = listOfMapplets.size();
            System.out.println(" ***** List of Mapplets ******");
            for(int j=0; j < listSize; j++){
                System.out.println(((Mapplet)listOfMapplets.get(j)).getName());
            }
        }
        
        for(int i=0 ; i < folderSize; i++){
        	Vector listOfMappings = ((Folder)folders.get(i)).getMappings(); //get the list of mappings
        	int listSize = listOfMappings.size();
			System.out.println(" ***** List of Mappings ******");
        	for(int j=0; j < listSize; j++){
        		System.out.println(((Mapping)listOfMappings.get(j)).getName());
        	}
        }
    }


    /**
     * not expecting any arguments
     */
    public static void main(String[] args) {
        try {
        	ListObjects repo = new ListObjects();
            repo.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }

    }

}