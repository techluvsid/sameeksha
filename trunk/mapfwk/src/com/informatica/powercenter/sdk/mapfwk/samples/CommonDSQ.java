/*
 * CommonDSQ.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationContext;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * 
 * 
 */
public class CommonDSQ extends Base {
    protected Target outputTarget;
    protected Source itemSource;
    protected Source productSource;

    /**
     * Create sources
     */
    protected void createSources() {
        itemSource = this.createItemsSource();
        folder.addSource( itemSource );
        ConnectionInfo connInfo = getRelationalConnectionInfo( SourceTargetTypes.RELATIONAL_TYPE_MSSQL );
        connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, "toolsdevelop" );
        itemSource.setConnInfo( connInfo );
        productSource = this.createProductsSource();
        folder.addSource( productSource );
        productSource.setConnInfo( connInfo );
    }

    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "CMN_DSQ_Output" );
    }

    protected void createMappings() throws Exception {
        mapping = new Mapping( "CMN_DSQ", "CMN_DSQ", "This is CMN_DSQ sample" );
        setMapFileName( mapping );
        // Logic to create a DSQ Transformation using 2 sources. They
        // should satisfy PKFK constraint.
        Vector vSources = new Vector();
        InputSet itemIS = new InputSet( itemSource );
        InputSet productIS = new InputSet( productSource );
        vSources.add( itemIS );
        vSources.add( productIS );
        TransformationContext tc = new TransformationContext( vSources );
        Transformation dsqTransform = tc.createTransform( TransformationConstants.DSQ, "CMN_DSQ" );
        // RowSet of combined transformation
        RowSet dsqRS = (RowSet) dsqTransform.apply().getRowSets().get( 0 );
        mapping.addTransformation( dsqTransform );
        // Create an Expression Transformation
        TransformHelper helper = new TransformHelper( mapping );
        TransformField orderCost = new TransformField(
                "number(24,0) OrderCost = (Price*Wholesale_cost)" );
        RowSet expRowSet = (RowSet) helper.expression( dsqRS, orderCost, "comb_exp_transform" )
                .getRowSets().get( 0 ); // target
        // write to target
        mapping.writeTarget( expRowSet, outputTarget );
        // add mapping to folder
        folder.addMapping( mapping );
    }

    /**
     * Create workflow method
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_CMN_DSQ", "Workflow_for_CMN_DSQ",
                "This workflow for joiner" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            CommonDSQ commonDSQ = new CommonDSQ();
            if (args.length > 0) {
                if (commonDSQ.validateRunMode( args[0] )) {
                    commonDSQ.execute();
                }
            } else {
                commonDSQ.printUsage();
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
        session = new Session( "Session_For_CMN_DSQ", "Session_For_CMN_DSQ",
                "This is session for CMN_DSQ" );
        session.setMapping( this.mapping );
    }
}
