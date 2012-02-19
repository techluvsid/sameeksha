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
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.PowerCenterCompatibilityFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;



/**
 * @author rtumruko
 * @since Hercules
 */
public class ModifyObjectsSample {
    protected Repository rep;
    protected String mapFileName;
    protected Transformation rank;
    private static final int INSERT = 0;
    private static final int UPDATE = 1;
    private static final int DELETE = 2;
    RepositoryConnectionManager repmgr;
    public ModifyObjectsSample() throws IOException
    {
    	init();
//    	this.setRepositoryConnectionManager(null);
//    	 initialise the repository configurations.                
        rep.setRepositoryConnectionManager(repmgr);   
        setMapFileName("ModiyObjectsSample");
    }
    protected void createRepository() {
        rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );        
    }
    public void setRepository(Repository rep)
    {
    	this.rep = rep;
    }
    protected void init() throws IOException {
        createRepository();
        Properties properties = new Properties();
//        String filename = "C:\\OUTPUT\\eBiz\\javamappingsdk_Output\\pcconfig.properties";
//        InputStream propStream = new FileInputStream(filename);
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
    /*
     * INSERT - Insert a new link between fromField(in source) and toField(in Transformation)
     * UPDATE - change the link in connecting to fromField to now connect to toField
     * DELETE - delete the link connection fromField and toField
     */
    synchronized public void LinkBetweenSourceAndTransformation(Mapping map,String transformationName, Field fromField,Field toField, int option)
    {
    	Vector vtrans = map.getTransformation();
			Iterator vTransIter = vtrans.iterator();
			while(vTransIter.hasNext())
			{
				Transformation trans = (Transformation) vTransIter.next();
				/*
				 * get the required transformation
				 */
				if(trans.getName().equals(transformationName)) // source qualifer
				{
					// get the input set from the transformation context
					Vector ipSet = trans.getTransContext().getInputSet();
					// pick the required Input set. In this case, this is a single
					// inputset transformation. So pick the first (only) Inputsey
					InputSet ip = (InputSet) ipSet.elementAt(0);
					// get the port definitions from the inputset
					Vector vportDefs = ip.getPortDef();
					// select a port to model the instance name and type after.
					this.performOperation(vportDefs, option, fromField, toField);					
				}
			}
    }
    /*
     * source => ( tranformation1 => transformation2 )=> target 
     * manipulate links between transformation1 and transformation2
     */
    /*
     * INSERT - Insert a new link between fromField(in source) and toField(in Transformation)
     * UPDATE - change the link in connecting to fromField to now connect to toField
     * DELETE - delete the link connection fromField and toField
     */
    synchronized public void LinkBetweenTransformationAndTransformation(Mapping map,String transformation2_name, Field fromField,Field toField, int option)
    {
    	Vector vtrans = map.getTransformation();
		Iterator vTransIter = vtrans.iterator();
		while(vTransIter.hasNext())
		{
			Transformation trans = (Transformation) vTransIter.next();
			/*
			 * get the required transformation
			 */
			if(trans.getName().equals(transformation2_name))
			{
				// get the input set from the transformation context
				Vector ipSet = trans.getTransContext().getInputSet();
				// pick the required Input set. In this case, this is a single
				// inputset transformation. So pick the first (only) Inputsey
				InputSet ip = (InputSet) ipSet.elementAt(0);
				// get the port definitions from the inputset
				Vector vportDefs = ip.getPortDef();
				// select a port to model the instance name and type after.
				this.performOperation(vportDefs, option, fromField, toField);					
			}
		}
    }
    /*
     *source => tranformation1 => (transformation2 => target)
     *Manipulate links between transformation 2 and target
     */ 
    /*
     * INSERT - Insert a new link between fromField(in source) and toField(in Transformation)
     * UPDATE - change the link in connecting to fromField to now connect to toField
     * DELETE - delete the link connection fromField and toField
     */
    synchronized public void LinkBetweenTransformationAndTarget(Mapping map,String target_name, Field fromField,Field toField, int option)
    {
    	Vector vtargets = map.getTarget();
		Iterator vtargetsIter = vtargets.iterator();
		while(vtargetsIter.hasNext())
		{
			Target target = (Target) vtargetsIter.next();
			/*
			 * get the required transformation
			 */
			if(target.getName().equals(target_name))
			{
				// get the port definitions from the target
				Vector vportDefs = target.getPortDef();
				// select a port to model the instance name and type after.
				this.performOperation(vportDefs, option, fromField, toField);				
			}
		}
    }
    private void performOperation(Vector vportDefs, int option, Field fromField,Field toField)
    {    	    	
    	synchronized (vportDefs)
    	{
    	switch (option)
		{
			case INSERT: 
				// create the new port definition. The 2 fields passed are the
				// fields that should be linked. -
				// fields with the same name should exists in the source and the transformation, else the 
				// this will fail while importing into the repository.
				Iterator myiter = vportDefs.iterator();
				boolean found = false;
				while(myiter.hasNext())
				{
					PortDef pd = (PortDef) myiter.next();
					if(pd.getInputField().getName().equals(fromField.getName()) && pd.getOutputField().getName().equals(toField.getName()))
						found = true;
				}
				if(!found)
				{
					PortDef newPortDef = new PortDef(fromField,toField);
					PortDef modelPortDef = (PortDef)vportDefs.elementAt(0);
					// set the instance properties using the model port definition
					newPortDef.setFromInstanceName(modelPortDef.getFromInstanceName());
					newPortDef.setFromInstanceType(modelPortDef.getFromInstanceType());
					newPortDef.setToInstanceName(modelPortDef.getToInstanceName());
					newPortDef.setToInstanceType(modelPortDef.getToInstanceType());
					vportDefs.add(newPortDef);
				}
				break;
			case UPDATE:
				Iterator iter = vportDefs.iterator();
				while(iter.hasNext())
				{
					PortDef pd = (PortDef) iter.next();
					if(pd.getOutputField().getName().equals(fromField.getName()))
					{
						pd.getOutputField().setName(toField.getName());
						pd.getOutputField().setBusinessName(toField.getBusinessName());
					}
				}							
				break;
			case DELETE:
				Iterator iter1 = vportDefs.iterator();
				
				while(iter1.hasNext())
				{
						PortDef pd = (PortDef) iter1.next();
						if(pd.getInputField().getName().equals(fromField.getName()) && pd.getOutputField().getName().equals(toField.getName()))
							iter1.remove();	
				}
				break;
		}
    	}
    }
    synchronized public void setRepositoryConnectionManager(RepositoryConnectionManager repMgr)
    {
    	if(repMgr == null)
    		this.repmgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
    	else
    	{
    		this.repmgr = repMgr;
    	}
    	rep.setRepositoryConnectionManager(repmgr);
    }
    synchronized public void execute() throws Exception {
    	
        /*
         *  retrieve a folder. In this case example I have a folder called w1 in my repository that I am retrieving
         *  Change the condition appropriately to retrieve any other folder
         */
		PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
		compFactory.setCompatibilityVersion(8, 5,1);
        Vector folders = rep.getFolder(new NameFilter(){
			public boolean accept(String name)
			{
				return name.equals("w1");
			}        	
        });
        
        /*
         * The cache is updated with the first call to CacheRepositoryConnectionManager. The list of folders are retrieved from
         * from the cache. Subsequent filtered calls to getFolder will retrieve the folder from the cache without making a call to the repository. 
		 * 
         */
        Folder w1 = (Folder)folders.elementAt(0);
        Vector workflows = w1.getWorkFlows();

       	Vector vMappings = w1.getMappings();  	
       	Iterator iter = vMappings.iterator();
       	while(iter.hasNext())
       	{
       		/*
           	 * Use case 1: add a new link between source and transformation (source qualifier for instance)
           	 */ 
       		Mapping map = (Mapping) iter.next();
       		/*
       		 * get the required mapping
       		 */
       		if(map.getName().equals("testMapping"))
       		{
       			/*this.LinkBetweenSourceAndTransformation(map, "SQ_OrderDetail",
       				new Field("Varchar2Fld","Varchar2Fld","",DataTypeConstants.STRING,"5","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_SOURCE,false)
       				,new Field("Varchar2Fld","Varchar2Fld","",DataTypeConstants.STRING,"5","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false),
       				ModifyObjectsSample.INSERT);*/
       			map.setModified(true);
       		}
       		/*
           	 * end use case 1
           	 */
       		
       		/*
           	 * Use case 2: add a new link between transformation(tx1) and transformation (tx2). This is the same as earlier
           	 * case. Use the transformation context in tranformation tx2 to get the inputset and get the port definitions
           	 * from this inputset. From the vector of port definitions, pick a model port. 
           	 * create a new port definition object and use the model port to set the instance properties of this port def as
           	 * shown above. Then add the port definition to the vector of port definitions.
           	 */
       		if(map.getName().equals("ValidationMapping"))
       		{
       			this.LinkBetweenTransformationAndTransformation(map, "agg_transform",
       				new Field("UnitPrice","UnitPrice","",DataTypeConstants.DECIMAL,"15","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false)
       				,new Field("total_cost","total_cost","",DataTypeConstants.DECIMAL,"15","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false),
       				ModifyObjectsSample.UPDATE);
       			//map.setModified(true);
       		}
           	/*
           	 * end use case 2
           	 */
       		/*
           	 * use case 3. add a new link between transformation(tx1) and target(tgt1). This is the same as earlier
           	 * case. Use the target to get the inputset and get the port definitions from this inputset. From the vector of 
           	 * port definitions, pick a model port. 
           	 * create a new port definition object and use the model port to set the instance properties of this port def as
           	 * shown above. Then add the port definition to the vector of port definitions.
           	 */
       		if(map.getName().equals("ValidationMapping"))
       		{
       			this.LinkBetweenTransformationAndTarget(map, "Validation_Output",
       				new Field("Discount","Discount","",DataTypeConstants.INTEGER,"15","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false)
       				,new Field("Discount","Discount","",DataTypeConstants.INTEGER,"15","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TARGET,false),
       				ModifyObjectsSample.DELETE);
       			//map.setModified(true);
       		}
           	/*
           	 * end use case 3
           	 */
       		/*
       		 * Add/remove/modify fields in source/target/transformation.
       		 * 1. Vector vSource = map.getSource(); 
       		 * 							// same for target
       		 * 							// use Transformation.getOutRowSet() and Transformation.getOutputFields()
       		 * 2. Source  source = vSource.elementAt(0);//pick the source 
       		 * 3. Vector vfields = source.getFields();  // same for target. For transformations, both the OutRowSet
       		 *											// and OutputFields would need to be modified
       		 * 4. vfields.remove(1); 
       		 * 			// remove the selected field. Same for target.
       		 *			// For transformations and tarets, the port defs would need to be updated to remove links
       		 *			// to the field just removed. Refer the use cases above to modify port definitions.
       		 *			// For transformations, use 
       		 * 5. vfields.add(new Field()); // add the necessary field. The port definitions would need to be updated
       		 *								// to link to/from the new field.
       		 * 6. Field field = vfields.elementAt(2);
       		 * 7. field.setName("newName"); // modify field. The port definitions need to be updated to reflect the 
       		 * 								// change in name of the field.	
       		 */       		
       		/*
       		 * change data type
       		 */
       		
       		/*
       		 * If the datatype needs to be changes across multiple transformations, then port defs would have to be used
       		 * to link the port that links to the port whose datatype needs to be changed.
       		 * Source => transformation1 => transformation2 => target.
       		 * 
       		 * e.g. suppose myField in the target is to be changed to, say int.
       		 * 1. Iterate through the fields in target and find myField.
       		 * 2. Change the datatype to DataTypeConstants.INTEGER.
       		 * 3. If the target was retrieved from the repository, set the dataTypeFlag to Field.DATATYPE_FLAG_NOTPRESERVE
       		 * 4. Iterate through the port defs in the target and find the field/instance that link to the myField/target.
       		 * 		Look at the OutputField of the port definition to identify the field.
       		 * 5. Once the instance name,type and field name has been found, find the instance in mapping depending
       		 * on the instance type and name. 
       		 * 6. After changing the field datatype in the instance, get the port definitions and repeat step 4-5
       		 * for the instance.
       		 */
       		if(map.getName().equals("AggregateMappingForParameter"))
        	{
       			Vector vSources = map.getSource();
       			Source source = (Source)vSources.elementAt(0);
       			if(source != null && source.getName().equals("OrderDetail"))
       			{
	       			Vector vFields = source.getFields();
	       			Iterator vFieldsIter = vFields.iterator();
	       			while(vFieldsIter.hasNext())
	       			{
	       				Field field = (Field)vFieldsIter.next();
	       				if(field.getName().equals("UnitPrice"))
	       				{
	       					field.setDataType(DataTypeConstants.INTEGER);
	       					field.setScale("0");
	       					field.setPrecision("7");
	       					field.setDataTypeFlag(Field.DATATYPE_FLAG_MODIFIED);
	       				}       				       				
	       			}
       			}
       			//map.setModified(true);
        	}       		
       	}
       	       	       	
        Vector modFolders = rep.getModifiedFolders();
        System.out.println(modFolders.toString());       
        generateOutput();
    }
    /**
     * This method generates the output xml
     * @throws Exception exception
     */
    synchronized public void generateOutput() throws Exception {
        MapFwkOutputContext outputContext = new MapFwkOutputContext(
                MapFwkOutputContext.OUTPUT_FORMAT_XML, MapFwkOutputContext.OUTPUT_TARGET_FILE,
				mapFileName);
      
        //rep.saveAndImportModifiedFolders(outputContext);
        rep.save(outputContext, false);
        }
    
    protected void setMapFileName( Mapping mapping ) {
        StringBuffer buff = new StringBuffer();
        buff.append( System.getProperty( "user.dir" ) );
        buff.append( java.io.File.separatorChar );
        buff.append( mapping.getName() );
        buff.append( ".xml" );
        mapFileName = buff.toString();
    }
    protected void setMapFileName( String filename) {
        StringBuffer buff = new StringBuffer();
        buff.append( System.getProperty( "user.dir" ) );
        buff.append( java.io.File.separatorChar );
        buff.append( filename );
        buff.append( ".xml" );
        mapFileName = buff.toString();
    }
    synchronized public Target createFlatFileTarget( String name ) {
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
        	ModifyObjectsSample repo = new ModifyObjectsSample();
        	RepositoryConnectionManager repMgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
        	repo.setMapFileName("ModifyObjectsSample");
        	repo.setRepositoryConnectionManager(repMgr);
            repo.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }

    }

}