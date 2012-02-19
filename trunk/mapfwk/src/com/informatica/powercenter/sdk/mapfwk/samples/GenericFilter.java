/*
 * filter.java Created on May 13, 2005.
 * 
 * Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.GenericTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PowerCenterCompatibilityFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationProperties;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * 
 */
public class GenericFilter extends Base {
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
		folder.addSource(orderDetailSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Filter_Output");
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping("FilterMapping", "FilterMapping",
				"This is sample for Filter");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// creating DSQ Transformation
		RowSet dsqRS = (RowSet) helper.sourceQualifier(this.orderDetailSource)
				.getRowSets().get(0);
		InputSet inset = new InputSet(dsqRS);
		Vector vInset = new Vector();
		vInset.add(inset);
		TransformationProperties props = new TransformationProperties();
		props.setProperty("Filter Condition", "TRUE");
				
		GenericTransformation trans = new GenericTransformation("filter","","","filter","Filter",vInset,null,props);
		mapping.addTransformation(trans.getTransContext().getTransformObj());
		RowSet rs = (RowSet)trans.apply().getRowSets().get(0);
		mapping.writeTarget(rs, this.outputTarget);

		folder.addMapping(mapping);
	}

	/**
	 * Create session
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_Filter", "Session_For_Filter",
				"This is session for Filter");
		session.setMapping(mapping);
	}

	/**
	 * Create workflow
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_Filter",
				"Workflow_for_Filter", "This workflow for Filter");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	public static void main(String args[]) {
		try {
			GenericFilter genAgg = new GenericFilter();
			if (args.length > 0) {
				if (genAgg.validateRunMode(args[0])) {
					PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
					compFactory.setCompatibilityVersion(8, 5,1);
					genAgg.execute();
				}
			} else {
				genAgg.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}

}
