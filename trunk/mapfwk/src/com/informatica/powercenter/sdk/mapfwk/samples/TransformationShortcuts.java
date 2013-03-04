/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.repository.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;

/**
 * This sample program performs the following
 * <br>
 * 1. Create a mapping as follows
 *
 *    Shortcut of source --> DSQ --> shortcut of Expression Transform --> shortcut of target
 *
 *   <br> Expression Transform concatenates the first name and last name to get full name<br>
 * 
 * 2. Create a session using this mapping<br> 
 * 
 * 3. Create workflow using the session created in step 2.
 *
 *
 * @author asingla
 * @since 9.0.0
 */
public class TransformationShortcuts extends Base {
    protected Source employeeSrc;
    protected Target outputTarget;
    protected Transformation trans;
    RepositoryConnectionManager repmgr;
    
    /**
     * Create sources
     */
    protected void createSources() {
    }

    /**
     * Create targets
     */
    protected void createTargets() {
    }
    
    /**
     * Intializes the properties of the repository
     * 
     * @throws <code>IOException</code>
     * 				If default properties file is not found
     */
    protected void intializeLocalProps() throws IOException {
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
     * Get the specified source from the given folder from the repository using
     * pmrep<br>
     * 
     * @return <code>Source</code> Instance of the source object fetched from repository 
     * @throws Exception
     */
    protected Source getSource() throws Exception {

        intializeLocalProps();
        rep.setRepositoryConnectionManager(repmgr);
        RepositoryConnectionManager repMgr = new PmrepRepositoryConnectionManager();            

        setRepositoryConnectionManager(repMgr);

        
        List<Folder> folders = rep.getFolders(new INameFilter() {
            public boolean accept(String name) {
                return name.equals("test_folder");
                }
            });
        Folder temp = folders.get(0);
        
        List<Source> vSources = temp.fetchSourcesFromRepository(new INameFilter()
        {
            public boolean accept(String name)
            {
                return name.equals("source_test");
            }
        });
       Source mySource=vSources.get(0);
       return mySource;
    }
    
    /**
     * Get the specified target from the specified folder from the
     * repository using pmrep 
     *
     * @return <code>Target</code>
     * 				 Instance of the target fetched from repository
     * @throws Exception
     */
    protected Target getTarget() throws Exception 
    {
    
        List<Folder> folders1 = rep.getFolders(new INameFilter() {
              public boolean accept(String name) {
              return name.equals("test_folder");
               }
        });
        Folder temp =  folders1.get(0);
        List<Target> targets = temp.fetchTargetsFromRepository(new INameFilter()
        {
           public boolean accept(String name)
              {
                 return name.equals("target_test");
              }
           });
        Target myTarget=targets.get(0);
        return myTarget;
     }
    
    /**
     * Get the specified transformation from the specified folder from the repository
     * 
     * @return <code>Transformation</code>
     * 				 Instance of the transformation fetched from repository
     * @throws Exception
     */
    protected Transformation getTransformation() throws Exception {
    	
        List<Folder> folders = rep.getFolders(new INameFilter() {
            public boolean accept(String name) {
                return name.equals("test_folder");
                }
            });
        Folder temp = folders.get(0);
        List<Transformation> transformations = temp.fetchTransformationsFromRepository(new INameFilter()
        {
            public boolean accept(String name)
            {
                return name.equals("trans_expression");
            }
        });
       Transformation myTrans=transformations.get(0);
       return myTrans;
    }
    
    // To set the repository connection manager
    synchronized public void setRepositoryConnectionManager(
            RepositoryConnectionManager repMgr) {
        if (repMgr == null)
            repmgr = new PmrepRepositoryConnectionManager();
        else {
            repmgr = repMgr;
        }
        rep.setRepositoryConnectionManager(repmgr);
    }
    
    /**
     * Creates the Expression mapping using shortcuts
     * <br>
     * This function performs the following
     * <ul>
     * <li> Creates the shortcut of the <code>Source</code></li>
     * <li> Creates the shortcut of the <code>Target</code></li>
     * <li> Creates the shortcut of the <code>transformation</code></li>
     * <li> Updates the <code>InputSet</code> of the transformation and writes the output to the Target</li>
     * </ul>
     * Mapping generated is:<br>
     * Shortcut of source --> DSQ --> shortcut of Expression Transform --> shortcut of target
     * <br>
     * 
     */  
    protected void createMappings() throws Exception {
        mapping = new Mapping( "shortCutSampleForTrans", "shortCutSampleForTrans", "This is shortCutSample for Transformation" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        Source mySource =getSource();
  
        // create shortcut for source
        
        ShortCut scSrc=new ShortCut("Shortcut_to_source_test","shortcut to source","asingla_repo_861","test_folder","source_test",RepositoryObjectConstants.OBJTYPE_SOURCE,TransformationConstants.STR_SOURCE,ShortCut.LOCAL);
        folder.addShortCut(scSrc);
        mapping.addShortCut(scSrc);
        scSrc.setRefObject(mySource);
        OutputSet outSet = helper.sourceQualifier(scSrc);
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        InputSet inSet= new InputSet(dsqRS);
        List<InputSet> vinputSet= new ArrayList<InputSet>();
        vinputSet.add(inSet);

        // create shortcut for target
    
        outputTarget=getTarget();
        ShortCut scTrg=new ShortCut("Shortcut_to_target_test","shortcut to Target","asingla_repo_861","test_folder","target_test",RepositoryObjectConstants.OBJTYPE_TARGET,TransformationConstants.STR_TARGET,ShortCut.LOCAL);
        folder.addShortCut(scTrg);
        mapping.addShortCut(scTrg);
        scTrg.setRefObject(outputTarget);
        outputTarget.setName(scTrg.getName());
        outputTarget.setInstanceName(scTrg.getName());

        // create the shortcut of the transformation
        
        trans=getTransformation(); 
        // while creating shortcut, it is necessary to mention the subtype
        // of the transformation whether it is expression, filter or any other transformation
        ShortCut scTrans=new ShortCut("Shortcut_to_trans_expression","shortcut to Trans","asingla_repo_861","test_folder","trans_expression",RepositoryObjectConstants.OBJTYPE_TRANSFORMATION,TransformationConstants.STR_EXPRESSION ,ShortCut.LOCAL);
        folder.addShortCut(scTrans);
        mapping.addShortCut(scTrans);
        // set the reference object of the transformation shortcut
        scTrans.setRefObject(trans);

        // Update the input set the transformation shortcut 

        /*
         * Helper class can also be used to update the InputSet of the transformation
         * and to get the OutputSet from transformation using the following code:
         * OutputSet outset = helper.useTransformationShortcut(vinputSet, scTrans);
         */
        OutputSet outset =  ((Transformation)scTrans.getRefObject()).getTransContext().updateInputSetOfShorcutTrans(vinputSet, scTrans);
        
        // write to target
        mapping.writeTarget((RowSet)outset.getRowSets().get(0) , (Target)scTrg.getRefObject());
        mapping.removeTarget(outputTarget);
        folder.addMapping( mapping );
        
      }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_shortCutSampleForTrans", "Session_For_shortCutSampleForTrans",
                "This is session for shortCutSampleFor Trans" );
        session.setMapping( this.mapping );
    }

    /**
     * Create workflow using session
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_shortCutSampleForTrans", "Workflow_for_shortCutSampleForTrans",
                "This workflow for TransformationReadAndUse" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
    
    /**
     * Sets the path of the XML file to be generated
     * @param filename
     * 				Name of XML file 
     */
    protected void setMapFileName(String filename) {
        StringBuffer buff = new StringBuffer();
        buff.append(System.getProperty("user.dir"));
        buff.append(java.io.File.separatorChar);
        buff.append(filename);
        buff.append(".xml");
        mapFileName = buff.toString();
    }

    /*
	 * Main function to execute this sample program
	 */
    public static void main( String args[] ) {
        try {
            TransformationShortcuts shortCut = new TransformationShortcuts();
            
            
            if (args.length > 0) {
                if (shortCut.validateRunMode( args[0] )) {
                    shortCut.execute();
                }
            } else {
                shortCut.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
