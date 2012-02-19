/*
 * 
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
 * Sample for creating multiple mappings using same shortcut
 * @author rjain
 *
 */
public class ShortcutMassMapping extends Base {
    protected Source employeeSrc1;
    protected Target outputTarget1;
    
    protected Source employeeSrc2;
    protected Target outputTarget2;
    
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
        outputTarget1 = this.createFlatFileTarget("trg1");
        outputTarget2 = this.createFlatFileTarget("trg2");
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
    
    protected Source getSource1() throws Exception {

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
    
    
    protected Source getSource2() throws Exception {

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
                return name.equals("ID");
            }
        });
        
        Source mySource=(Source)vSources.get(0);
               
        return mySource;
    }
    
    protected Target getTarget1() throws Exception 
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
    
    protected Target getTarget2() throws Exception 
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
                            return name.equals("ID");
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
        Mapping mapping1 = new Mapping( "shortCutSample1", "shortCutSample1", "This is shortCutSample" );
        Mapping mapping2 = new Mapping( "shortCutSample2", "shortCutSample2", "This is shortCutSample" );
        setMapFileName( mapping1 );        
        
        TransformHelper helper1 = new TransformHelper( mapping1 );
        TransformHelper helper2 = new TransformHelper( mapping2);

        Source mySource1 =getSource1();
        Vector vFields1=mySource1.getFields();
        Iterator iterFld1=vFields1.iterator();
        while(iterFld1.hasNext())
        {
            Field fld=(Field)iterFld1.next();
            fld.setDataType(DataTypeConstants.INTEGER);
            fld.setDataTypeFlag(Field.DATATYPE_FLAG_NOTPRESERVE);
        }

        ShortCut scSrc1=new ShortCut("shortcut_src_age1","shortcut to source","Repo_rjain","temp","age",RepositoryObjectConstants.OBJTYPE_SOURCE,ShortCut.LOCAL);
        folder.addShortCut(scSrc1);
        mapping1.addShortCut(scSrc1);
                     
        mapping2.addShortCut(scSrc1);              
        
        scSrc1.setRefObject(mySource1);
                        
        OutputSet outSet1 = helper1.sourceQualifier(scSrc1);
        RowSet dsqRS1 = (RowSet) outSet1.getRowSets().get( 0 );
        
        OutputSet outSet2 = helper2.sourceQualifier(scSrc1);
        RowSet dsqRS2 = (RowSet) outSet2.getRowSets().get( 0 );

                
               
       mapping1.writeTarget(dsqRS1, this.outputTarget1);
       mapping2.writeTarget(dsqRS2, this.outputTarget1);
        
       folder.addMapping( mapping1 );
       folder.addMapping( mapping2 );
                 
        }

        /**
     * Create session
     */
    protected void createSession() throws Exception {       
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {       
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
            ShortcutMassMapping shortCut = new ShortcutMassMapping();
            
            
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
