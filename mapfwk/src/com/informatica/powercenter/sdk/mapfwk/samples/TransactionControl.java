/*
 * TestTransactionControl.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This example applies a simple Transaction Control transformation on the
 * Employee table and writes to a target.
 * 
 * 
 */
public class TransactionControl extends Base {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source employeeSrc;
    protected Target outputTarget;

    /**
     * Create sources
     */
    protected void createSources() {
        employeeSrc = this.createEmployeeSource();
        folder.addSource( employeeSrc );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "TC_Output" );
    }

    /**
     * This method creates the mapping with a Transaction Control
     * Transformation.
     */
    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "TCMapping", "mapping", "Testing Transaction Sample sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create an Transaction Control Transformation
        String condition = "IIF(EmployeeID>10,TC_COMMIT_AFTER,TC_CONTINUE_TRANSACTION)";
        RowSet tcRS = (RowSet) helper.transactionControl( dsqRS, null, "tc_transform", condition,
                null ).getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( tcRS, outputTarget );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            TransactionControl tcTrans = new TransactionControl();
            if (args.length > 0) {
                if (tcTrans.validateRunMode( args[0] )) {
                    tcTrans.execute();
                }
            } else {
                tcTrans.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_TC", "Session_For_TC",
                "This is session for transaction control" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_TC", "Workflow_for_TC", "This workflow for TC" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
