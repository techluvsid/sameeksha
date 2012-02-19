/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputException;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionFailedException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ValidationFailedException;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryObjectConstants;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * @author sramamoo
 *
 */
public class ValidateSample extends Base
{
	protected Source orderDetailSource;

	protected Target outputTarget;

	/**
	 * Creates a folder
	 */
	protected void createFolder()
	{
		folder = new Folder("ValidateSample", "ValidateSample",
				"This is a folder containing java mapping samples for valiation");
		rep.addFolder(folder);
	}

	protected void createMappings() throws Exception
	{
		// create a mapping
		mapping = new Mapping("ValidationMapping", "ValidationMapping",
				"This is sample for validation");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// creating DSQ Transformation
		RowSet dsqRS = (RowSet) helper.sourceQualifier(this.orderDetailSource)
				.getRowSets().get(0);

		// create an aggregator Transformation
		// calculate cost per order using the formula
		// SUM((UnitPrice * Quantity) * (100 - Discount1) / 100) grouped by
		// OrderId
		TransformField cost = new TransformField(
				"number(15,0) total_cost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))");
		RowSet aggRS = (RowSet) helper.aggregate(dsqRS, cost,
				new String[] { "OrderID" }, "agg_transform").getRowSets()
				.get(0);
		// write to target
		mapping.writeTarget(aggRS, this.outputTarget);

		folder.addMapping(mapping);
	}

	protected void createSession() throws Exception
	{
		session = new Session("Session_For_Validation",
				"Session_For_Validation", "This is session for Validation");
		session.setMapping(mapping);
	}

	protected void createSources()
	{
		orderDetailSource = this.createOrderDetailSource();
		orderDetailSource.getConnInfo().getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILE_DIRECTORY, "C:\\Temp");
		orderDetailSource.getConnInfo().getConnProps().setProperty(
				ConnectionPropsConstants.SOURCE_FILENAME, "Order_Details.csv");
		folder.addSource(orderDetailSource);
	}

	protected void createTargets()
	{
		outputTarget = this.createFlatFileTarget("Validation_Output");
		outputTarget.getConnInfo().getConnProps().setProperty(
				ConnectionPropsConstants.OUTPUT_FILE_DIRECTORY, "C:\\Temp");
	}

	protected void createWorkflow() throws Exception
	{
		workflow = new Workflow("Workflow_for_Validation",
				"Workflow_for_Aggregate", "This workflow for Validation");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}
	public void changeTargetName(String name)
	{
		outputTarget.setName(name);
		outputTarget.setBusinessName(name);
        // set its instance name
		outputTarget.setInstanceName(name);
        // change the output file name if required
		outputTarget.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.OUTPUT_FILENAME, name+".out");
		Vector portDef = outputTarget.getPortDef();
        Iterator portIter = portDef.iterator();
        while(portIter.hasNext()) 
        {
            PortDef curDef = (PortDef)portIter.next();
            // change the port def to point to the new name.                    
            curDef.setToInstanceName(name);
        } 
	}
	public void changeTargetInstance()
	{
		Target newTarget = this.createFlatFileTarget("newTarget_replace");
        // set its instance name
        if (newTarget.getInstanceName() == null || newTarget.getInstanceName().equals(""))
        	newTarget.setInstanceName(newTarget.getName());
        
        Vector portDef = outputTarget.getPortDef();
        Iterator portIter = portDef.iterator();
        
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
        /*
         * Check if removing a link invalidates the mapping
         */
//        portDef.remove(0);
        // set the portDef of he new target
        newTarget.getPortDef().addAll(portDef);
        // remove the previous target from the mapping
        folder.getTarget().remove(outputTarget);
        mapping.getTarget().remove(outputTarget);
        
        folder.addTarget(newTarget);
        // add the new target
        mapping.addTarget(newTarget);
        /*
         * End case 2
         */
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
	public void validate()
	{		
		CachedRepositoryConnectionManager connMgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
		rep.setRepositoryConnectionManager(connMgr);
		
		StringWriter outputSummary = new StringWriter();
		// validate mapping
		try
		{
			connMgr.validate(mapping.getName(), RepositoryObjectConstants.OBJTYPE_MAPPING, folder.getName(), true,outputSummary);
		} catch (ConnectionFailedException e)
		{			
			e.printStackTrace();
		} catch (ValidationFailedException e)
		{
		
			e.printStackTrace();
		} catch (IOException e)
		{		
			e.printStackTrace();
		}
		System.out.println(outputSummary.toString());
		outputSummary.flush();
		// validate session
		try
		{
			connMgr.validate(session.getName(), RepositoryObjectConstants.OBJTYPE_SESSION, folder.getName(), true,outputSummary);
		} catch (ConnectionFailedException e)
		{			
			e.printStackTrace();
		} catch (ValidationFailedException e)
		{
		
			e.printStackTrace();
		} catch (IOException e)
		{		
			e.printStackTrace();
		}
		System.out.println(outputSummary.toString());
		outputSummary.flush();
		// validate workflow
		try
		{
			connMgr.validate(workflow.getName(), RepositoryObjectConstants.OBJTYPE_WORKFLOW, folder.getName(), true,outputSummary);
		} catch (ConnectionFailedException e)
		{			
			e.printStackTrace();
		} catch (ValidationFailedException e)
		{
		
			e.printStackTrace();
		} catch (IOException e)
		{		
			e.printStackTrace();
		}
		System.out.println(outputSummary.toString());
		outputSummary.flush();
		try
		{
			outputSummary.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void initLocalProps() throws IOException
	{
		Properties properties = new Properties();
        String filename = "pcconfig.properties";
        InputStream propStream = getClass().getClassLoader().getResourceAsStream(filename);
        
        if ( propStream != null ) {
	        properties.load( propStream );

	        rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, properties.getProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH));
	     rep.getProperties().setProperty(RepoPropsConstant.TARGET_FOLDER_NAME,folder.getName() );
	     rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME,rep.getName() );
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
	public void saveToRepository()
	{
		try
		{
		initLocalProps();
		} catch (IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        MapFwkOutputContext outputContext = new MapFwkOutputContext(
                MapFwkOutputContext.OUTPUT_FORMAT_XML, MapFwkOutputContext.OUTPUT_TARGET_FILE,
				mapFileName);
        try
		{
			rep.save(outputContext, true);
		}
        catch (MapFwkOutputException e)
		{			
			e.printStackTrace();
		}
	}
	public static void main(String args[])
	{
		ValidateSample sample = null;
		try
		{
			sample = new ValidateSample();
			if (args.length > 0)
			{
				if (sample.validateRunMode(args[0]))
				{					
					sample.execute();
				}
			} else
			{
				sample.printUsage();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
		sample.changeTargetName("newTarget");
		//sample.changeTargetInstance();
		sample.saveToRepository();
		sample.validate();
	}
}
