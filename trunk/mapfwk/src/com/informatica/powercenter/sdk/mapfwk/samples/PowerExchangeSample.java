/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import javax.xml.bind.JAXBException;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkException;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PowerCenterCompatibilityFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectSource;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectTarget;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SourceGroup;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TargetGroup;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.Factory.PowerConnectSourceFactory;
import com.informatica.powercenter.sdk.mapfwk.core.Factory.PowerConnectTargetFactory;
import com.informatica.powercenter.sdk.mapfwk.exceptions.SourceTargetDefinitionNotFoundException;

/**
 * @author sramamoo 
 *
 */
public class PowerExchangeSample extends Base
{
	PowerConnectSource source;
    PowerConnectTarget outputTarget;
	/* (non-Javadoc)
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
	 */
	protected void createMappings() throws Exception
	{
		mapping = new Mapping("myMapping", "myMapping",
				"Testing sample with myMapping");
		TransformHelper helper = new TransformHelper(mapping);
		RowSet asqRS = (RowSet) helper.sourceQualifier(source)
				.getRowSets().get(0);
		// creating ASQ Transformation
		/*
		 * Transformation asqTrans = employeeSource.createASQTransform(); RowSet
		 * asqRS = (RowSet) asqTrans.apply().getRowSets().get(0);
		 * mapping.addTransformation( asqTrans );
		 */
		// write to target
		InputSet asqIS = new InputSet(asqRS);
		Vector vin = new Vector();
		vin.add(asqIS);
		mapping.writeTarget(vin, outputTarget);
		folder.addMapping(mapping);

	}

	/* (non-Javadoc)
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception
	{
        session = new Session("Session_For_PowerExchange",
                "Session_For_PowerExcange", "This is session for PowerExcange");
        session.setMapping(this.mapping);		
	}

	/* (non-Javadoc)
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
	 */
	protected void createSources()
	{
		PowerConnectSourceFactory sourceFactory = PowerConnectSourceFactory.getInstance();
		try
		{
			source = sourceFactory.getPowerConnectSourceInstance("PWX_DB2UDB_CDC", "mySource", "mySourceDBD", "mySource", "mySource");
			SourceGroup srcGrp = new SourceGroup("ct_ALLDTYPES_SRC",null);
			source.createField("DTL__CAPXRESTART1",srcGrp,"","","PACKED","25","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
			source.createField("DTL__CAPXRESTART2",srcGrp,"","",DataTypeConstants.STRING,"10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
			
			source.setMetaExtensionValue("Access Method", "V");
			source.setMetaExtensionValue("Map Name", "ct_ALLDTYPES_SRC");
			source.setMetaExtensionValue("Original Name" , "ALLDTYPES_SRC");
			source.setMetaExtensionValue("Original Schema", "PWXUDB");
			Vector connInfos = source.getConnInfos();
			for (int i=0;i<connInfos.size();i++)
			{
				ConnectionInfo connInfo = (ConnectionInfo) connInfos.elementAt(i);
				connInfo.getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, "myTestConnection");
				connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME,"myDBName");
			}
		} catch (SourceTargetDefinitionNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MapFwkException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		folder.addSource(source);
		this.mapFileName = "PowerExchangeSource.xml";
	}

	/* (non-Javadoc)
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
	 */
	protected void createTargets()
	{
        PowerConnectTargetFactory targetFactory = PowerConnectTargetFactory.getInstance();
        try
        {
            outputTarget = targetFactory.getPowerConnectTargetInstance("PWX_SEQ_NRDB2", "myTargetDBD",  "myTargetDBD",  "myTargetDBD",  "myTargetDBD");
            TargetGroup trgGrp = new TargetGroup("stqa",null);
            outputTarget.createField("DTL__CAPXRESTART1",trgGrp,"","","PACKED","25","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);
            outputTarget.createField("DTL__CAPXRESTART2",trgGrp,"","",DataTypeConstants.STRING,"10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);
            
            
            outputTarget.setMetaExtensionValue("Access Method", "S");            
            outputTarget.setMetaExtensionValue("Original Schema", "PWXUDB");
            Vector connInfos = outputTarget.getConnInfos();
            for (int i=0;i<connInfos.size();i++)
            {
                ConnectionInfo connInfo = (ConnectionInfo) connInfos.elementAt(i);
                connInfo.getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, "myTestConnection");
                connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME,"myDBName");
            }
            
        } catch (SourceTargetDefinitionNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MapFwkException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        folder.addTarget(outputTarget);
	}

	/* (non-Javadoc)
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
	 */
	protected void createWorkflow() throws Exception
	{
        workflow = new Workflow("Workflow_for_PowerExcange",
                "Workflow_for_PowerExcange", "This workflow for PowerExcange");
        workflow.addSession(session);
        folder.addWorkFlow(workflow);
	}
	
	public static void main(String args[]) {
		try {
			PowerExchangeSample sample = new PowerExchangeSample();
			if (args.length > 0) {
				if (sample.validateRunMode(args[0])) {
					PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
					compFactory.setCompatibilityVersion(8, 5, 0);
					sample.execute();
				}
			} else {
				sample.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
	
}
