/*
 * SourceClone.java Created on Nov 4, 2005.
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
 * This example illustrates the use of cloning sources and target.
 * 
 */
public class SourceClone extends Base {
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
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Expression_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "SourceCloneMapping", "mapping", "Testing SourceClone sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( dsqRS, outputTarget );
        // clone the source and target
        Source empSrcClone = (Source) employeeSrc.clone();
        empSrcClone.setInstanceName( empSrcClone.getName() + "_clone" );
        Target targetClone = (Target) outputTarget.clone();
        targetClone.setInstanceName( outputTarget.getName() + "_clone" );
        mapping.addTarget( targetClone );
        // create DSQ and write to target
        outSet = helper.sourceQualifier( empSrcClone );
        dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        mapping.writeTarget( dsqRS, targetClone );
        folder.addMapping( mapping );
    }

    public static void main( String args[] ) {
        try {
            SourceClone SourceCloneTrans = new SourceClone();
            if (args.length > 0) {
                if (SourceCloneTrans.validateRunMode( args[0] )) {
                    SourceCloneTrans.execute();
                }
            } else {
                SourceCloneTrans.printUsage();
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
        session = new Session( "Session_For_SourceClone", "Session_For_SourceClone",
                "This is session for SourceClone" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_SourceClone", "Workflow_for_SourceClone",
                "This workflow for SourceClone" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }
}
