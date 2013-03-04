/**
 * ListObjectNames.java Created on Jan, 2010.
 * Copyright (c) 2010 Informatica Corporation.  All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionObject;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.repository.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.repository.ISrcTgtField.SrcTgtDataType;

/**
 * This example is to list all source and target names at repository level
 * Listing mapplet/mapping/workflow names is yet to be added.
 * 
 * @author araju
 */

public class ListObjectNames {
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
	        rep.getRepoConnectionInfo().setPcClientInstallPath(properties.getProperty(RepoPropsConstants.PC_CLIENT_INSTALL_PATH));
	        rep.getRepoConnectionInfo().setTargetFolderName(properties.getProperty(RepoPropsConstants.TARGET_FOLDER_NAME));
	        rep.getRepoConnectionInfo().setTargetRepoName(properties.getProperty(RepoPropsConstants.TARGET_REPO_NAME));
	        rep.getRepoConnectionInfo().setRepoServerHost(properties.getProperty(RepoPropsConstants.REPO_SERVER_HOST));
	        rep.getRepoConnectionInfo().setAdminPassword(properties.getProperty(RepoPropsConstants.ADMIN_PASSWORD));
	        rep.getRepoConnectionInfo().setAdminUsername(properties.getProperty(RepoPropsConstants.ADMIN_USERNAME));
	        rep.getRepoConnectionInfo().setRepoServerPort(properties.getProperty(RepoPropsConstants.REPO_SERVER_PORT));
	        rep.getRepoConnectionInfo().setServerPort(properties.getProperty(RepoPropsConstants.SERVER_PORT));
	        rep.getRepoConnectionInfo().setDatabaseType(properties.getProperty(RepoPropsConstants.DATABASETYPE));
	        
	        if(properties.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER) != null)
	        	rep.getRepoConnectionInfo().setPmrepCacheFolder(properties.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER));	        	
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

   
        //Fetching Source Names
        Map<Folder, List> srcNames = rep.getAllSourceNames();
        Set<Folder> flds1 = (Set<Folder>) srcNames.keySet();
        Iterator itr1 = flds1.iterator();
        while(itr1.hasNext()){
        	Folder fld = (Folder)itr1.next();
        	List<String> srcs = srcNames.get(fld);
        	System.out.println("\nList of Sources in Folder: "+fld.getName());
        	for(int j=0 ; j<srcs.size(); ++j){
        		System.out.println(srcs.get(j));        		
        	}
        }
        
        //Fetching Target Names        
        Map<Folder, List> tgtNames = rep.getAllTargetNames();
        Set<Folder> flds = (Set<Folder>) tgtNames.keySet();
        Iterator itr = flds.iterator();
        while(itr.hasNext()){
        	Folder fld = (Folder)itr.next();
        	List<String> tgts = tgtNames.get(fld);
        	System.out.println("\nList of Targets in Folder: "+fld.getName());
        	for(int j=0 ; j<tgts.size(); ++j){
        		System.out.println(tgts.get(j));        		
        	}
        }
    }

    public static void main(String[] args) {
        try {
        	ListObjectNames repo = new ListObjectNames();
            repo.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
