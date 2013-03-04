/*
 * Aggregator.java Created on May 13, 2005.
 * 
 * Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.powercentercompatibility.PowerCenterCompatibilityFactory;

/**
 * 
 * 
 */
public class AggregatorForFlatFile extends Base {
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
		outputTarget = this.createFlatFileTarget("Aggregate_Output");
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping("AggregateMapping", "AggregateMapping",
				"This is sample for aggregate");
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
				"decimal(15,0) total_cost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))");
		RowSet aggRS = (RowSet) helper.aggregate(dsqRS, cost,
				new String[] { "OrderID" }, "agg_transform").getRowSets()
				.get(0);
		// write to target
		mapping.writeTarget(aggRS, this.outputTarget);

		folder.addMapping(mapping);
	}

	/**
	 * Create session
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_Aggregate", "Session_For_Aggregate",
				"This is session for Aggregate");
		session.setMapping(mapping);
	}

	/**
	 * Create workflow
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_Aggregate",
				"Workflow_for_Aggregate", "This workflow for aggregate");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	public static void main(String args[]) {
		try {
			AggregatorForFlatFile aggregator = new AggregatorForFlatFile();
			if (args.length > 0) {
				if (aggregator.validateRunMode(args[0])) {
					PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
					compFactory.setCompatibilityVersion(8, 5,1);
					aggregator.execute();
				}
			} else {
				aggregator.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}

}
