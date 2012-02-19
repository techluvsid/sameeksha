/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Iterator;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionProperties;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariable;
import com.informatica.powercenter.sdk.mapfwk.core.ParameterFile;
import com.informatica.powercenter.sdk.mapfwk.core.ParameterFileIterator;
import com.informatica.powercenter.sdk.mapfwk.core.PowerMartDataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.WorkflowVariable;

/**
 * @author sramamoo
 *
 */
public class ParameterFileSample extends Base
{
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source orderDetailSource;

	protected Target outputTarget;

	/**
	 * Create sources
	 */
	protected void createSources() {
		orderDetailSource = this.createOrderDetailSource();
		ConnectionInfo connInfo = orderDetailSource.getConnInfo();
		ConnectionProperties connProperties = connInfo.getConnProps();
//		connInfo.setConnType(SourceTargetTypes.RELATIONAL_TYPE_ORACLE);
		connProperties.setProperty(ConnectionPropsConstants.SOURCE_FILENAME, "$InputFileMyinputfilename");
		
		connProperties.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "$DBConnectionMyconnection");
		folder.addSource(orderDetailSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Parameter_Output");
		ConnectionInfo connInfo = outputTarget.getConnInfo();
		ConnectionProperties connProperties = connInfo.getConnProps();
		connProperties.setProperty(ConnectionPropsConstants.OUTPUT_FILENAME, "$OutputFileMyoutputfilename");
		connProperties.setProperty(ConnectionPropsConstants.REJECT_FILENAME, "$BadFileMybadfilename");
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping("AggregateMappingForParameter", "AggregateMappingFOrParameter",
				"This is sample for parameter file usage");
		Vector vMappingVariables = getMappingVariables();
		
		Iterator mappingVarIter = vMappingVariables.iterator();		
		while(mappingVarIter.hasNext())
		{
			mapping.addMappingVariable((MappingVariable) mappingVarIter.next());
		}
		
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

	/**
	 * @return
	 */
	private Vector getMappingVariables()
	{
		Vector vMappingVars = new Vector();
		
		MappingVariable mpVar1 = new MappingVariable("MAX",DataTypeConstants.STRING,"default value1","description",true,"$$var1","20","0",true);
		MappingVariable mpVar2 = new MappingVariable("MIN",DataTypeConstants.INTEGER,"100","description",false,"$$var2","10","0",true);
		MappingVariable mpVar3 = new MappingVariable("MAX",DataTypeConstants.NSTRING,"default value3","description",true,"$$var3","10","0",true);
		MappingVariable mpVar4 = new MappingVariable("MAX",DataTypeConstants.INTEGER,"101","description",false,"$$var4","10","0",true);
		MappingVariable mpVar5 = new MappingVariable("MIN",DataTypeConstants.STRING,"default value5","description",true,"$$var5","15","0",true);
		MappingVariable mpVar6 = new MappingVariable("MAX",DataTypeConstants.NSTRING,"default value6","description",false,"$$var6","20","0",true);
		
		vMappingVars.add(mpVar1);
		vMappingVars.add(mpVar2);
		vMappingVars.add(mpVar3);
		vMappingVars.add(mpVar4);
		vMappingVars.add(mpVar5);
		vMappingVars.add(mpVar6);
		
		return vMappingVars;
	}

	/**
	 * Create session
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_parameter_file", "Session_For_parameter_file",
				"This is session for parameter file");
		session.setMapping(mapping);
		
	}

	/**
	 * Create workflow
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_parameter_file",
				"Workflow_for_parameter_file", "This workflow for parameter file");
		workflow.addSession(session);
		workflow.setParentFolder(folder);
		workflow.addWorkflowVariables(getWorkflowVariables());
		folder.addWorkFlow(workflow);
		
		Vector listOfParams = workflow.getListOfParameters();
		
		ParameterFile pmFile = new ParameterFile("C:\\work\\param.save");
		
		Iterator listOfParamsIter = listOfParams.iterator();
		int i=0;
		while(listOfParamsIter.hasNext())
		{
			pmFile.setParameterValue((String) listOfParamsIter.next(), new Integer(i).toString());
			i++;
		}
		pmFile.save();
		ParameterFileIterator iter = pmFile.iterator();
		
		while(iter.hasNext())
		{
			Vector nextRow = (Vector)iter.next();
			System.out.println(nextRow.elementAt(0));
			System.out.println("Scope>>"+ iter.getScope());
			System.out.println(nextRow.elementAt(1));
			System.out.println("ParamName>>" + iter.getParameterName());
			System.out.println(nextRow.elementAt(2));
			System.out.println("value>>" + iter.getValue());
			System.out.println("----------------------");
		}		
	}
	
	/**
	 * @return
	 */
	private Vector getWorkflowVariables()
	{
		Vector vWFVars = new Vector();
		
		WorkflowVariable wfVar1 = new WorkflowVariable("$$wfVar1",PowerMartDataTypeConstants.DOUBLE,"100.00","WF var desc",true,false);
		WorkflowVariable wfVar2 = new WorkflowVariable("$$wfVar2",PowerMartDataTypeConstants.INTEGER,"100","WF var desc",true,true);
		WorkflowVariable wfVar3 = new WorkflowVariable("$$wfVar3",PowerMartDataTypeConstants.NSTRING,"WF defaultValue3","WF var desc",false,false);
		WorkflowVariable wfVar4 = new WorkflowVariable("$$wfVar4",PowerMartDataTypeConstants.INTEGER,"102","WF var desc",false,true);
		WorkflowVariable wfVar5 = new WorkflowVariable("$$wfVar5",PowerMartDataTypeConstants.TIMESTAMP,"10-31-2006","WF var desc",true,true);
		WorkflowVariable wfVar6 = new WorkflowVariable("$$wfVar6",PowerMartDataTypeConstants.DOUBLE,"101.00","WF var desc",true,false);
		
		vWFVars.add(wfVar1);
		vWFVars.add(wfVar2);
		vWFVars.add(wfVar3);
		vWFVars.add(wfVar4);
		vWFVars.add(wfVar5);
		vWFVars.add(wfVar6);
		
		return vWFVars;
	}
	
	public static void main(String args[]) {
		try {
			ParameterFileSample pmFileSample = new ParameterFileSample();
			if (args.length > 0) {
				if (pmFileSample.validateRunMode(args[0])) {
					pmFileSample.execute();
				}
			} else {
				pmFileSample.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
