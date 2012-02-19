/*
 * Copyright (c) 2005 Informatica Corporation.  All rights reserved.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.OutputField;
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TaskTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.WorkflowVariable;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;



/**
 * @author rtumruko
 * @since Hercules
 */
public class DeleteAndReplaceTarget {
    protected Repository rep;
    protected String mapFileName;
    protected Transformation rank;

    protected void createRepository() {
        rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );
    }

    protected void init() throws IOException {
        createRepository();
        Properties properties = new Properties();
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
        }
        else {
            throw new IOException( "pcconfig.properties file not found.");
        }
    }

    public void execute() throws Exception {
    	// initialise the repository configurations.
        init();
        RepositoryConnectionManager repmgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
        rep.setRepositoryConnectionManager(repmgr);
        /*
         *  retrieve a folder. In this case example I have a folder called w1 in my repository that I am retrieving
         *  Change the condition appropriately to retrieve any other folder
         */
        Vector folders = rep.getFolder(new NameFilter() {
            public boolean accept(String name) {
            	return name.equals("delMe");
            }
        });
        /*
         * printing the details of the folder contents retrieved.
         */
        for (int i = 0; i < folders.size(); i++) {
            Folder folder = (Folder)folders.get(i);

            Vector workflows = folder.getWorkFlows();
//            folder.addWorkFlow(workflows);
            System.out.println("Number of workflows retreived: " + workflows.size());
            for(int m = 0; m < workflows.size(); m++) {
                Vector vecSessions = ((Workflow)workflows.get(m)).getSessions();
                //Vector vecSessions = workflow.getSessions();
                for(int n = 0; n<vecSessions.size(); n++){
                    Session sess = (Session)vecSessions.get(n);
                    System.out.println("================= Session: " + sess.getName());
                    System.out.println(sess.getProperties().toString());
                    if(sess.getType() == TaskTypes.SESSION) {
                      Mapping curMmapping = ((Session)vecSessions.get(n)).getMapping();
                      System.out.println("====================");
                      System.out.println(curMmapping.getName());
                      /*
                       * print sources in mapping
                       */
                      Vector sources = curMmapping.getSource();                      
                      for (int k = 0; k < sources.size(); k++) {
                          Source source = (Source)sources.get(k);
                          System.out.println("==================");
                          System.out.println(source);
                          System.out.println(source.getConnInfo().getConnProps().toString());
                          System.out.println("==================");
                      }
                      /*
                       * print target in mapping
                       */
                      Vector targets = curMmapping.getTarget();
                      for (int k = 0; k < targets.size(); k++) {
                          Target target = (Target)targets.get(k);
                          System.out.println("==================");
                          System.out.println(target);
                          System.out.println(target.getConnInfo().getConnProps().toString());
                          System.out.println("=====Target PortDef: ");
                          Vector portDef = target.getPortDef();
                          Iterator portIter = portDef.iterator();
                          while(portIter.hasNext()) {
                              PortDef curDef = (PortDef)portIter.next();
                              System.out.println("From Field: " + curDef.getInputField().getName() + " from: " + curDef.getFromInstanceName() + " of type: " + curDef.getFromInstanceType());
                              System.out.println("To Field: " + curDef.getOutputField().getName() + " from: " + curDef.getToInstanceName() + " of type: " + curDef.getToInstanceType());
                          }
                      }
                      System.out.println("cout of portdefs for target: " + ((Target)targets.get(0)).getPortDef().size());
                      /*
                       * print transformations.
                       */
                      Vector transformations = curMmapping.getTransformation();
                      for (int k = 0; k < transformations.size(); k++) {
                          Transformation trans = (Transformation)transformations.get(k);
                          System.out.println("==================");
                          System.out.println(trans.getName() + " of type " + trans.getTransformationType());
                          System.out.println(trans.getProperties().toString());
                          System.out.println("=======Transformation Fields");
                          Vector vInpSets = trans.getTransContext().getInputSet();
                          Iterator inpIter = vInpSets.iterator();
                          while(inpIter.hasNext()) {
                              InputSet curInpSet = (InputSet)inpIter.next();
                              Vector vFlds = curInpSet.getOutputField();
                              Iterator fldIter = vFlds.iterator();
                              while(fldIter.hasNext()) {
                                  OutputField fld = (OutputField)fldIter.next();
                                  System.out.println(" Field: " + fld.getField().getName() + " of GroupName: " + fld.getGroupName());
                                  //System.out.println(" sortkey: " + fld.getSortKey() + " sort dir: " + fld.getSortDirection());
                                  System.out.println(" fldkey: " + fld.getField().getDataType() + " = " + fld.getDataTypeFlag() + " >> " + fld.getField().getDataTypeFlag());
                              }
                              System.out.println("=====Transformation PortDef: ");
                              Vector portDef = curInpSet.getPortDef();
                              Iterator portIter = portDef.iterator();
                              while(portIter.hasNext()) {
                                  PortDef curDef = (PortDef)portIter.next();
                                  System.out.println("From Field: " + curDef.getInputField().getName() + " from: " + curDef.getFromInstanceName() + " of type: " + curDef.getFromInstanceType());
                                  System.out.println("To Field: " + curDef.getOutputField().getName() + " from: " + curDef.getToInstanceName() + " of type: " + curDef.getToInstanceType());
                              }
                          }
                          System.out.println("==================");
                      }
                    }
                }
                /*
                 * print workflow variables
                 */
                Vector wfVars = ((Workflow)workflows.get(m)).getWorkflowVariables();
                Iterator wfVarsIter = wfVars.iterator();
                while(wfVarsIter.hasNext())
                {
                	WorkflowVariable wfVar = (WorkflowVariable) wfVarsIter.next();
                	System.out.println("name = " + wfVar.getName());
                	System.out.println("Datatype = " + wfVar.getDataType());
                	System.out.println("Defaultvalue = " + wfVar.getDefaultValue());
                	System.out.println("Desc = " + wfVar.getDescription());                	
                	System.out.println("-----------------------------------------");
                }
                /*
                 * function to change the target.
                 */
                this.changeTargetForWorkflow(((Workflow)workflows.get(m)),folder);
            }
            /*
             * set repository connection manager to null to generate xml
             */
//            rep.setRepositoryConnectionManager(null);
//            folder.setRepositoryConnectionManager(null);            
            /*
             * add folder to repository.
             */
//            rep.addFolder(folder);
        }
        /*
         * generate xml
         */
        generateOutput();
    }
    
    private void changeTargetForWorkflow(Workflow workflow, Folder folder)
    {
    	Vector vSessions = workflow.getSessions();
    	Iterator vSessIter = vSessions.iterator();
    	Target newTarget;
    	while(vSessIter.hasNext())
    	{
    		Session session = (Session)vSessIter.next();
    		if(session.getType() == TaskTypes.START)
    			continue;
    		Mapping mapping = session.getMapping();
    		 /*
             * print target in mapping
             */
    		mapping.setModified(true);
            Vector targets = mapping.getTarget();
            for (int k = 0; k < targets.size(); k++) {
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("BEFORE change of Target");
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    
                Target target = (Target)targets.get(k);
                System.out.println("==================");
                System.out.println(target);
                System.out.println(target.getConnInfo().getConnProps().toString());
                System.out.println("=====Target PortDef: ");
                Vector portDef = target.getPortDef();
                Iterator portIter = portDef.iterator();
                while(portIter.hasNext()) {
                    PortDef curDef = (PortDef)portIter.next();
                    System.out.println("From Field: " + curDef.getInputField().getName() + " from: " + curDef.getFromInstanceName() + " of type: " + curDef.getFromInstanceType());
                    System.out.println("To Field: " + curDef.getOutputField().getName() + " from: " + curDef.getToInstanceName() + " of type: " + curDef.getToInstanceType());
                }
                /*
                 * Case 1: Just change the name of the target. The field names will not be changed
                 */
                // new target name
                String newTarget1 = "newTarget";
                // change the name of the target
                target.setName(newTarget1);
                target.setBusinessName(newTarget1);
                // set its instance name
                target.setInstanceName(newTarget1);
                // change the output file name if required
                target.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.OUTPUT_FILENAME, newTarget1+".out");
                portIter = portDef.iterator();
                while(portIter.hasNext()) 
                {
                    PortDef curDef = (PortDef)portIter.next();
                    // change the port def to point to the new name.                    
                    curDef.setToInstanceName(newTarget1);
                } 
                /*
                 * End case 1
                 */
                
                /*
                 * Case 2: changing the target object. The field object will be retained, but the field name 
                 * can be changed. Comment till end of this block to see the effects of Case 1. Else changes
                 * in case 1 will be overwritten. 
                 */
                             
                // changing target
                 // create a new target
                newTarget = this.createFlatFileTarget("newTarget_replace");
                // set its instance name
                if (newTarget.getInstanceName() == null || newTarget.getInstanceName().equals(""))
                	newTarget.setInstanceName(newTarget.getName());
                
                portIter = portDef.iterator();
                int count = 0;
                // update port defs
                while(portIter.hasNext()) 
                {
                    PortDef curDef = (PortDef)portIter.next();
                    // change the Instance name in the port def
                    curDef.setToInstanceName(newTarget.getName());
                    // new field name if required
                    String fieldName = "field"+count;
                    Field oldField = curDef.getOutputField();
                    oldField.setName(fieldName);
                    oldField.setBusinessName(fieldName);
                    // if new field object has to be set, care has to be taken to ensure that they datatypes are 
                    // compatible.
                    
                    // if the field doesnot exist in the target, add it to the target
                    if(!checkIfFieldExists(newTarget,oldField))
                    	newTarget.addField(oldField);                    
                    count++;
                } 
                // set the portDef of he new target
                newTarget.getPortDef().addAll(portDef);
                // remove the previous target from the mapping
                targets.remove(k);
                // add the new target
                mapping.addTarget(newTarget);
                /*
                 * End case 2
                 */
                // print the changed Target details.
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("AFTER change of Target");
        	    System.out.println("==================");
        	    System.out.println("==================");
        	    System.out.println("==================");                
        	    portIter = portDef.iterator();
                while(portIter.hasNext()) {
                    PortDef curDef = (PortDef)portIter.next();
                    System.out.println("From Field: " + curDef.getInputField().getName() + " from: " + curDef.getFromInstanceName() + " of type: " + curDef.getFromInstanceType());
                    System.out.println("To Field: " + curDef.getOutputField().getName() + " from: " + curDef.getToInstanceName() + " of type: " + curDef.getToInstanceType());
                }                
            }
            System.out.println("cout of portdefs for target: " + ((Target)targets.get(0)).getPortDef().size());
            this.setMapFileName(mapping);
//            folder.addMapping(mapping);
    	}
    }
    private boolean checkIfFieldExists(Target target, Field field)
    {
    	Vector vFields = target.getFields();
    	Iterator vFieldsIter = vFields.iterator();
    	while(vFieldsIter.hasNext())
    	{
    		Field fieldInst = (Field) vFieldsIter.next();
    		if(fieldInst.getName().equals(field.getName()))
    			return true;
    	}
    	return false;
    }

    /**
     * This method generates the output xml
     * @throws Exception exception
     */
    public void generateOutput() throws Exception {
        MapFwkOutputContext outputContext = new MapFwkOutputContext(
                MapFwkOutputContext.OUTPUT_FORMAT_XML, MapFwkOutputContext.OUTPUT_TARGET_FILE,
				mapFileName);
      
        rep.saveAndImportModifiedFolders(outputContext);
        }
    
    protected void setMapFileName( Mapping mapping ) {
        StringBuffer buff = new StringBuffer();
        buff.append( System.getProperty( "user.dir" ) );
        buff.append( java.io.File.separatorChar );
        buff.append( mapping.getName() );
        buff.append( ".xml" );
        mapFileName = buff.toString();
    }

    public Target createFlatFileTarget( String name ) {
        Target tgt = new Target(name, name, "", "",new ConnectionInfo( SourceTargetTypes.FLATFILE_TYPE ));
        tgt.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_CODEPAGE, "MS1252");
        tgt.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.OUTPUT_FILENAME, name+".out");
        return tgt;
    }

    protected ConnectionInfo getFlatFileConnectionInfo() {

        ConnectionInfo infoProps = new ConnectionInfo( SourceTargetTypes.FLATFILE_TYPE );
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_SKIPROWS,"1");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_DELIMITERS,";");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.DATETIME_FORMAT,"A  21 yyyy/mm/dd hh24:mi:ss");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_QUOTE_CHARACTER,"DOUBLE");

        return infoProps;
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
        	DeleteAndReplaceTarget repo = new DeleteAndReplaceTarget();
            repo.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }

    }

}
