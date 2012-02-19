/*
 * Joiner.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * 
 */
public class Joiner extends Base {
	protected Target outputTarget;

	protected Source ordersSource;

	protected Source orderDetailsSource;

	/**
	 * Create sources
	 */
	protected void createSources() {
		ordersSource = this.createOrdersSource();
		folder.addSource(ordersSource);
		orderDetailsSource = this.createOrderDetailSource();
		folder.addSource(orderDetailsSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Joiner_Output");
	}

	protected void createMappings() throws Exception {
		mapping = new Mapping("JoinMapping", "JoinMapping",
				"This is join sample");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// Pipeline - 1
		// create DSQ for Order_Details
		RowSet ordDetDSQ = (RowSet) helper.sourceQualifier(orderDetailsSource)
				.getRowSets().get(0);

		// calculate order cost using the formula
		// SUM((UnitPrice * Quantity) * (100 - Discount1) / 100) grouped by
		// OrderId
		TransformField orderCost = new TransformField(
				"number(24,0) OrderCost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))");
		RowSet ordDetAGG = (RowSet) helper.aggregate(ordDetDSQ, orderCost,
				new String[] { "OrderID" }, "Calculate_Order_Cost")
				.getRowSets().get(0);
		PortPropagationContext orderCostContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "OrderCost", "OrderID" });
		InputSet ordDetInputSet = new InputSet(ordDetAGG, orderCostContext); // propage
																				// only
																				// OrderCost

		// Pipeline - 2
		// create DSQ for Order
		RowSet ordDSQ = (RowSet) helper.sourceQualifier(ordersSource)
				.getRowSets().get(0);

		// Join Pipeline-1 to Pipeline-2
		Vector vInputSets = new Vector();
		vInputSets.add(ordDetInputSet); // collection includes only the detail

		RowSet joinRowSet = (RowSet) helper.join(vInputSets,
				new InputSet(ordDSQ), "OrderID = IN_OrderID",
				"Join_Order_And_Details").getRowSets().get(0);

		PortPropagationContext exclOrderID = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "IN_OrderID" }); // exclude
																					// OrderCost
																					// while
																					// writing
																					// to
																					// target

		InputSet joinInputSet = new InputSet(joinRowSet, exclOrderID);

		// Apply expression to calculate TotalOrderCost
		TransformField totalOrderCost = new TransformField(
				"number(24,0) TotalOrderCost = OrderCost + Freight");
		RowSet expRowSet = (RowSet) helper.expression(joinInputSet,
				totalOrderCost, "Expression_Total_Order_Cost").getRowSets()
				.get(0);
		PortPropagationContext exclOrderCost = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "OrderCost" }); // exclude
																				// OrderCost
																				// while
																				// writing
																				// to
																				// target

		// write to target
		mapping.writeTarget(new InputSet(expRowSet, exclOrderCost),
				outputTarget);

		// add mapping to folder
		folder.addMapping(mapping);
	}

	/**
	 * Create workflow method
	 */
	protected void createWorkflow() throws Exception {

		workflow = new Workflow("Workflow_for_Joiner", "Workflow_for_joiner",
				"This workflow for joiner");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);

	}

	public static void main(String args[]) {
		try {
			Joiner joinerTrans = new Joiner();
			if (args.length > 0) {
				if (joinerTrans.validateRunMode(args[0])) {
					joinerTrans.execute();
				}
			} else {
				joinerTrans.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_Joiner", "Session_For_Joiner",
				"This is session for joiner");
		session.setMapping(this.mapping);

	}
}
