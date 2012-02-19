/*
 * /*
 * Rank.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * This example illustrates how to use a rank transformation. This
 * transformation selects the number of rows specified by rank and
 * based on the group by condition that has been applied. 
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * @author asingh
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
public class Rank extends Base {
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
        folder.addSource( orderDetailSource );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Rank_Output" );
    }

    /**
     * This method creates a simple mapping required for Rank Transformation.
     */
    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "Rank_Mapping", "Rank_Mapping", "This is sample for rank" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        RowSet dsqRS = (RowSet) helper.sourceQualifier( this.orderDetailSource ).getRowSets().get(
                0 );
        // create a rank Transformation
        // set the rank and rank port
        RowSet rankRS = (RowSet) helper.rank( dsqRS, 3, "UnitPrice", new String[] { "ProductID" },
                "rank_transform" ).getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( rankRS, this.outputTarget );
        folder.addMapping( mapping );
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Rank", "Session_For_Rank", "This is session for Rank" );
        session.setMapping( mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Rank", "Workflow_for_Rank", "This workflow for rank" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            Rank rank = new Rank();
            if (args.length > 0) {
                if (rank.validateRunMode( args[0] )) {
                    rank.execute();
                }
            } else {
                rank.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
