/*
 * Expression.java Created on Nov 4, 2005.
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
 * 
 * 
 */
public class Filter extends Base {
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
        outputTarget = this.createFlatFileTarget( "Filter_Output" );
    }

    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "FilterMapping", "FilterMapping", "This is filter sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        OutputSet outSet = helper.sourceQualifier( employeeSrc );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        // create a Filter Transformation
        // filter out rows that don't belong to USA
        RowSet filterRS = (RowSet) helper.filter( dsqRS, "Country = 'USA'", "filter_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( filterRS, outputTarget );
        folder.addMapping( mapping );
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Filter", "Session_For_Filter",
                "This is session for filter" );
        session.setMapping( this.mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_filter", "Workflow_for_filter",
                "This workflow for filter" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            Filter filter = new Filter();
            if (args.length > 0) {
                if (filter.validateRunMode( args[0] )) {
                    filter.execute();
                }
            } else {
                filter.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
