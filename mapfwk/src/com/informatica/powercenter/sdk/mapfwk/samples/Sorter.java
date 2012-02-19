/*
 * Sorter.java Created on Nov 4, 2005.
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
 * This example applies a simple sorter transformation on the Employee table
 * sorts the employee by Firstname in ascending and LastName in descending and
 * writes to a targe
 * 
 */
public class Sorter extends Base {
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
        outputTarget = this.createFlatFileTarget( "Sorter_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "SorterMapping", "mapping", "Testing Sorter sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create a sorter Transformation
        RowSet sorterRS = (RowSet) helper.sorter( dsqRS, new String[] { "FirstName", "LastName" },
                new boolean[] { true, false }, "sorter_transform" ).getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( sorterRS, outputTarget );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            Sorter expressionTrans = new Sorter();
            if (args.length > 0) {
                if (expressionTrans.validateRunMode( args[0] )) {
                    expressionTrans.execute();
                }
            } else {
                expressionTrans.printUsage();
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
        session = new Session( "Session_For_Sorter", "Session_For_Sorter",
                "This is session for Sorter" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Sorter", "Workflow_for_Sorter",
                "This workflow for Sorter" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
