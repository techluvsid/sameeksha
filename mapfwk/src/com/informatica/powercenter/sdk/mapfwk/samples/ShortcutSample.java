/*
 * Expression.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryObjectConstants;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.ExpTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.FilterTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationContext;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;


/**
 * 
 * 
 */
public class ShortcutSample extends Base {
    protected Source employeeSrc;
    protected Target outputTarget;
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

    protected void intializeLocalProps() throws IOException {

        Properties properties = new Properties();
       
        //InputStream propStream = new FileInputStream(filename);
        String filename = "pcconfig.properties";
        InputStream propStream = getClass().getClassLoader().getResourceAsStream( filename);
        
        if ( propStream != null ) {
            properties.load( propStream );

            rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, properties.getProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH));
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_FOLDER_NAME, properties.getProperty(RepoPropsConstant.TARGET_FOLDER_NAME));
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME, properties.getProperty(RepoPropsConstant.TARGET_REPO_NAME));
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_HOST, properties.getProperty(RepoPropsConstant.REPO_SERVER_HOST));
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD, properties.getProperty(RepoPropsConstant.ADMIN_PASSWORD));
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME, properties.getProperty(RepoPropsConstant.ADMIN_USERNAME));
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_PORT, properties.getProperty(RepoPropsConstant.REPO_SERVER_PORT));
            rep.getProperties().setProperty(RepoPropsConstant.SERVER_PORT, properties.getProperty(RepoPropsConstant.SERVER_PORT));
            rep.getProperties().setProperty(RepoPropsConstant.DATABASETYPE, properties.getProperty(RepoPropsConstant.DATABASETYPE));
            if(properties.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER) != null)
                rep.getProperties().setProperty(RepoPropsConstant.PMREP_CACHE_FOLDER, properties.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER));
        }
        else {
            throw new IOException( "pcconfig.properties file not found.");
        }
    }
    
    protected Source getSource() throws Exception {

        intializeLocalProps();
        rep.setRepositoryConnectionManager(repmgr);
        RepositoryConnectionManager repMgr = new CachedRepositoryConnectionManager(
                new PmrepRepositoryConnectionManager());            

        setRepositoryConnectionManager(repMgr);

        
        Vector folders = rep.getFolder(new NameFilter() {
            public boolean accept(String name) {
                return name.equals("temp");
            }
        });

        Folder temp = (Folder) folders.elementAt(0);
        
        Vector vSources = temp.getSource(
            new NameFilter()
        {
            public boolean accept(String name)
            {
                return name.equals("age");
            }
        });
        
        Source mySource=(Source)vSources.get(0);
               
        return mySource;
    }
    
    protected Target getTarget() throws Exception 
    {
            Vector folders = rep.getFolder(new NameFilter() {
            public boolean accept(String name) {
                return name.equals("temp");
                }
            });

            Folder temp = (Folder) folders.elementAt(0);
        
            Vector vTargets = temp.getTarget(
                    new NameFilter()
                    {
                        public boolean accept(String name)
                        {
                            return name.equals("trg_int");
                        }
                    });
        
         Target myTarget=(Target)vTargets.get(0);
         return myTarget;
               
         }
    
    
    synchronized public void setRepositoryConnectionManager(
            RepositoryConnectionManager repMgr) {
        if (repMgr == null)
            repmgr = new CachedRepositoryConnectionManager(
                    new PmrepRepositoryConnectionManager());
        else {
            repmgr = repMgr;
        }
        rep.setRepositoryConnectionManager(repmgr);
    }
    

    protected void createMappings() throws Exception {
        mapping = new Mapping( "shortCutSample", "shortCutSample", "This is shortCutSample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );

        Source mySource =getSource();
        Vector vFields=mySource.getFields();
        Iterator iterFld=vFields.iterator();
        while(iterFld.hasNext())
        {
            Field fld=(Field)iterFld.next();
            fld.setDataType(DataTypeConstants.INTEGER);
            fld.setDataTypeFlag(Field.DATATYPE_FLAG_NOTPRESERVE);
        }
        
        
        ShortCut scSrc=new ShortCut("sc_src_age","shortcut to source","Repo_rjain","temp","age",RepositoryObjectConstants.OBJTYPE_SOURCE,ShortCut.LOCAL);
        folder.addShortCut(scSrc);
        mapping.addShortCut(scSrc);
        
        scSrc.setRefObject(mySource);
        OutputSet outSet = helper.sourceQualifier(scSrc);
        
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );

                
        outputTarget=getTarget();
          
        ShortCut scTrg=new ShortCut("sc_trg_int","shortcut to Target","Repo_rjain","temp","trg_int",RepositoryObjectConstants.OBJTYPE_TARGET,ShortCut.LOCAL);
        folder.addShortCut(scTrg);
       mapping.addShortCut(scTrg);
       scTrg.setRefObject(outputTarget);
       
       outputTarget.setName(scTrg.getName());
       outputTarget.setInstanceName(scTrg.getName());
       
        mapping.writeTarget(dsqRS, (Target)scTrg.getRefObject());
        
        mapping.removeTarget(outputTarget);
        folder.addMapping( mapping );
        
         
        }

        /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_shortCutSample", "Session_For_shortCutSample",
                "This is session for shortCutSample" );
        session.setMapping( this.mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_shortCutSample", "Workflow_for_shortCutSample",
                "This workflow for SourceReadAndUse" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
    
    protected void setMapFileName(String filename) {
        StringBuffer buff = new StringBuffer();
        buff.append(System.getProperty("user.dir"));
        buff.append(java.io.File.separatorChar);
        buff.append(filename);
        buff.append(".xml");
        mapFileName = buff.toString();
    }

    public static void main( String args[] ) {
        try {
            ShortcutSample shortCut = new ShortcutSample();
            
            
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
