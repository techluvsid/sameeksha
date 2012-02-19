/*
 * Lookup.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * 
 */
public class Lookup extends Base {
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source itemsSrc;

	protected Target outputTarget;

	protected Source manufacturerSrc;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
	 */
	protected void createMappings() throws Exception {
		mapping = new Mapping("LookupMapping", "LookupMapping",
				"This is a test for Lookup mapping");
		setMapFileName(mapping);

		// create helper
		TransformHelper helper = new TransformHelper(mapping);

		// create dsq transformation
		OutputSet outputSet = helper.sourceQualifier(itemsSrc);
		RowSet dsqRS = (RowSet) outputSet.getRowSets().get(0);
		PortPropagationContext dsqRSContext = PortPropagationContextFactory
				.getContextForExcludeColsFromAll(new String[] { "Manufacturer_Id" });

		// create a lookup transformation
		outputSet = helper.lookup(dsqRS, manufacturerSrc,
				"manufacturer_id = in_manufacturer_id",
				"Lookup_Manufacturer_Table");
		RowSet lookupRS = (RowSet) outputSet.getRowSets().get(0);
		PortPropagationContext lkpRSContext = PortPropagationContextFactory
				.getContextForIncludeCols(new String[] { "Manufacturer_Name" });

		Vector vInputSets = new Vector();
		vInputSets.add(new InputSet(dsqRS, dsqRSContext)); // remove
															// Manufacturer_id
		vInputSets.add(new InputSet(lookupRS, lkpRSContext)); // propagate
																// only
																// Manufacturer_Name

		// write to target
		mapping.writeTarget(vInputSets, outputTarget);
		folder.addMapping(mapping);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
	 */
	protected void createSession() throws Exception {
		session = new Session("Session_For_Lookup", "Session_For_Lookup",
				"This is session for Lookup");
		session.setMapping(mapping);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
	 */
	protected void createSources() {
		manufacturerSrc = this.createManufacturersSource();
		folder.addSource(manufacturerSrc);
		itemsSrc = this.createItemsSource();
		folder.addSource(itemsSrc);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Lookup_Items_Target");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("Workflow_for_lookup", "Workflow_for_lookup",
				"This workflow for lookup");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);

	}

	public static void main(String args[]) {
		try {
			Lookup lookup = new Lookup();
			if (args.length > 0) {
				if (lookup.validateRunMode(args[0])) {
					lookup.execute();
				}
			} else {
				lookup.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}

}
