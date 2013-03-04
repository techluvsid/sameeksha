/*
 * VSAMSample.java Created on Jan 07, 2010.
 *
 * Copyright 2009 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerField;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerRecord;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerTransformDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortType;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationContext;
import com.informatica.powercenter.sdk.mapfwk.core.VSAMField;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.powercentercompatibility.PowerCenterCompatibilityFactory;

/**
 * @since JMF 9.0.1
 * @author araju
 *
 */

public class VSAMSample extends Base
{
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source orderDetailSource;

	protected Target outputTarget1;

	/**
	 * Create source
	 */
	protected void createSources() {
		
        List<VSAMField> fields = new ArrayList<VSAMField>();
		Field field1 = new Field("field1","","",NormalizerTransformDataTypes.NUMBER,"10","0",FieldKeyType.NOT_A_KEY,FieldType.SOURCE,false);
		VSAMField vsamField1=new VSAMField("field1","","","1",field1);
		fields.add(vsamField1);
		
		Field field2 = new Field("field2","","",NormalizerTransformDataTypes.NUMBER,"10","0",FieldKeyType.NOT_A_KEY,FieldType.SOURCE,false);
		VSAMField vsamField2=new VSAMField("field2","","","1",field2,true,true,true);
		
		Field field3 = new Field("field3","","",NormalizerTransformDataTypes.STRING,"10","0",FieldKeyType.NOT_A_KEY,FieldType.SOURCE,false);
		VSAMField vsamField3=new VSAMField("field3","","","1",field3);
		
		Field field4 = new Field("field4","","",NormalizerTransformDataTypes.NSTRING,"10","0",FieldKeyType.NOT_A_KEY,FieldType.SOURCE,false);
		VSAMField vsamField4=new VSAMField("field4","","","3",field4);
		
        List<VSAMField> fields1 = new ArrayList<VSAMField>();
		fields1.add(vsamField2);
		fields1.add(vsamField3);
		fields1.add(vsamField4);
		NormalizerRecord record = new NormalizerRecord("record2","record2","record2","1", null,fields);
        List<NormalizerRecord> records=new ArrayList<NormalizerRecord>();
        records.add(record);  
        
        ConnectionInfo info = new ConnectionInfo( SourceTargetType.VSAM);
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Order_Details.csv");
        info.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,"VSAMdb");		
        orderDetailSource = new Source( "OrderDetail", "OrderDetail", "This is Order Detail Table", "OrderDetail", info );
        orderDetailSource.setVSAMFields(fields1);
        orderDetailSource.setNormalizerRecords(records);
		folder.addSource(orderDetailSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget1 = this.createFlatFileTarget("VSAM_Output1");		
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping("VSAMMapping", "VSAMMapping",
				"This is sample for VSAMMapping");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);
		RowSet dsqRS = (RowSet) helper.VSAMNormalizerTransform(this.orderDetailSource)
		.getRowSets().get(0);
		
		/*
		 * 
		 * Using Transformation Context
                
        List<InputSet> inputSets = new ArrayList<InputSet>();
        InputSet itemIS = new InputSet( orderDetailSource );
        inputSets.add( itemIS );
        TransformationContext tc = new TransformationContext( inputSets );
        Transformation VSAMNorm = tc.createTransform( TransformationConstants.NORMALIZER, "Norm_OrderDetail" );
        // RowSet of combined transformation
        RowSet dsqRS = (RowSet) VSAMNorm.apply().getRowSets().get( 0 );
        mapping.addTransformation( VSAMNorm );
		*/
		
		mapping.writeTarget( dsqRS, outputTarget1 );
		folder.addMapping(mapping);
	}

	/**
	 * Create session
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_VSAMMapping", "Session_For_VSAMMapping",
				"This is session for VSAMMapping");
		session.setMapping(mapping);
	}

	/**
	 * Create workflow
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_VSAMMapping",
				"Workflow_for_VSAMMapping", "This workflow for VSAMMapping");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	public static void main(String args[]) {
		try {
			VSAMSample sample = new VSAMSample();
			if (args.length > 0) {
				if (sample.validateRunMode(args[0])) {
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
