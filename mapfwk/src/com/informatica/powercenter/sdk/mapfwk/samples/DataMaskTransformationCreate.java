package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.CodePageConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionProperties;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.DataMask;
import com.informatica.powercenter.sdk.mapfwk.core.DataMaskTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MaskConstants;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.StringConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;


/**
 *
 *
 */
public class DataMaskTransformationCreate extends Base {

	// ////////////////////////////////////////////////////////////////
	// instance variables
	// ////////////////////////////////////////////////////////////////
	protected Mapping mapping = null;
	protected Source jobSourceObj = null;
	protected Target targetObj = null;
	protected DataMaskTransformation dmo;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
	 */
	protected void createSources() {

		jobSourceObj = this.createOracleJobSource("Oracle_Source002");
		folder.addSource(jobSourceObj);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
	 */
	protected void createTargets() {

		targetObj = this.createRelationalTarget(SourceTargetType.Oracle, "Target002");

	}

	protected void setDataToTransformation() throws Exception {

		dmo.addPortwithSeed(MaskConstants.DATA_TYPE_STRING, "Key", "JOB_ID", "10", "0", "190", "", "3");
		dmo.addPortwithSeed(MaskConstants.DATA_TYPE_STRING, "SSN", "JOB_TITLE", "35", "0", "", "", "10");
		dmo.addPort(MaskConstants.DATA_TYPE_DECIMAL, "Random", "MIN_SALARY", "6", "0", "", "1");

		dmo.addKeyMaskForStringByPortName("JOB_ID", "FALSE", "Mask only", "FALSE", "Use only", "FALSE", "", "", "");
		dmo.addSSNMaskForStringByPortName("JOB_TITLE", "FALSe", "FALSE");

		DataMask.MaskNumeric maskNumeric = new DataMask.MaskNumeric("Fixed", "FALSE", "TRUE", "", "", "", "1000000.000000", "0");
		dmo.addRandomMaskForDecimalByPortName("MIN_SALARY", maskNumeric);

		dmo.setMetaExtensionValue(StringConstants.META_EXTENTION_MASKING_RULES, dmo.getPortinfo().getXml());

	}

	protected void createMappings() throws Exception {

		mapping = new Mapping("DMO003", "DataMaskTransformationTest", "Mapping for DataMaskTransformationTest");
		setMapFileName(mapping);

		RowSet datamaskRS = createTransformation();

		mapping.writeTarget(datamaskRS, this.targetObj);
		folder.addMapping(mapping);

	}

	protected RowSet createTransformation() throws Exception {

		TransformHelper helper = new TransformHelper(mapping);

		RowSet dsqRowSet1 = (RowSet) helper.sourceQualifier(this.jobSourceObj).getRowSets().get(0);
		InputSet ip = new InputSet(dsqRowSet1, PortPropagationContextFactory.getContextForAllIncludeCols(),
			PortLinkContextFactory.getPortLinkContextByName());
		RowSet datamaskRS = (RowSet) helper.datamaskTransformation(ip, null, "DMO003", null,
			CodePageConstants.META_EXTENTION_CODE_PAGE_EN).getRowSets().get(0);
		dmo = (DataMaskTransformation) mapping.getTransformation("DMO003");
		setDataToTransformation();
		return datamaskRS;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_CustomACTIVE", "Session_For_CustomACTIVE",
			"This is session for Custom Transformation");
		session.setMapping(mapping);
		
		//Adding Connection Objects for substitution mask option
		ConnectionInfo info = new ConnectionInfo(SourceTargetType.Oracle);
		ConnectionProperties cprops = info.getConnProps();
		cprops.setProperty(ConnectionPropsConstants.CONNECTION_REFERENCE_NAME, "IDM_Dictionary");
		cprops.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "ILM_Connection");
		cprops.setProperty(ConnectionPropsConstants.CONNECTIONNUMBER, "1");
		
		ConnectionInfo info2 = new ConnectionInfo(SourceTargetType.Oracle);
		ConnectionProperties cprops2 = info2.getConnProps();
		cprops2.setProperty(ConnectionPropsConstants.CONNECTION_REFERENCE_NAME, "IDM_Storage");
		cprops2.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "ILM_Connection");
		cprops2.setProperty(ConnectionPropsConstants.CONNECTIONNUMBER, "2");
		List<ConnectionInfo> cons = new ArrayList<ConnectionInfo>();
		cons.add(info);
		cons.add(info2);
		session.addConnectionInfosObject(dmo, cons);
		
		//Overriding source connection in Seesion level
		ConnectionInfo newSrcCon = new ConnectionInfo(SourceTargetType.Oracle);
		ConnectionProperties newSrcConprops = newSrcCon.getConnProps();
		newSrcConprops.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "NewSrcCon");
		DSQTransformation dsq = (DSQTransformation)mapping.getTransformation("SQ_JOBS");
		session.addConnectionInfoObject(dsq, newSrcCon);
		//session.addConnectionInfoObject(jobSourceObj, newSrcCon);
		
		//Overriding target connection in Seesion level
		ConnectionInfo newTgtCon = new ConnectionInfo(SourceTargetType.Oracle);
		ConnectionProperties newTgtConprops = newTgtCon.getConnProps();
		newTgtConprops.setProperty(ConnectionPropsConstants.CONNECTIONNAME, "newTgtCon");
		session.addConnectionInfoObject(targetObj, newTgtCon);
		
		//Setting session level property.
		Properties props = new Properties();
		props.setProperty(StringConstants.META_EXTENTION_SSN_HIGH, StringConstants.META_EXTENTION_SSN_HIGH_DEFAULT_VAL);
		session.addSessionTransformInstanceProperties(dmo, props);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_CustomTransformation", "Workflow_for_CustomTransformation",
			"This workflow for Custom Transformation");
		workflow.assignIntegrationService("Rep_IT", "Domain_HYW172973");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			DataMaskTransformationCreate dmTransformation = new DataMaskTransformationCreate();
			if (args.length > 0) {
				if (dmTransformation.validateRunMode(args[0])) {
					dmTransformation.execute();
				}
			} else {
				dmTransformation.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
