/**
 * ListFolders.java Created on Jan, 2008.
 * Copyright (c) 2008 Informatica Corporation.  All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * This example is to list all folder names in a given repository 
 * @author kbhat
 */

public class ListFolders {
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
            
//            rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, properties
//                    .getProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH));
//            rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME, properties
//                    .getProperty(RepoPropsConstant.TARGET_REPO_NAME));
//            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_DOMAIN_NAME, properties
//                    .getProperty(RepoPropsConstant.REPO_SERVER_DOMAIN_NAME));
//            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD, properties
//                    .getProperty(RepoPropsConstant.ADMIN_PASSWORD));
//            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME, properties
//                    .getProperty(RepoPropsConstant.ADMIN_USERNAME));
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
        
        Vector folders = rep.getFolder();
        
        //gets folder count from repository
        int folderSize = folders.size();
        
        System.out.println("List of folder present in repository: " + rep.getProperties().getProperty(RepoPropsConstant.TARGET_REPO_NAME));
        for(int i=0 ; i < folderSize; i++){
            String folderName = ((Folder)folders.get(i)).getName();
            System.out.println(folderName);
        }
    }

    /**
     * Not expecting any arguments
     */
    public static void main(String[] args) {
        try {
            ListFolders listFolders = new ListFolders();
            listFolders.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}