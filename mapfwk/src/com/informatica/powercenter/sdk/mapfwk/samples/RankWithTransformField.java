/*
 * Rank1.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.PortType;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * @author asingh
 * 
 * This method creates a Rank Transformation with a new Transformation field and
 * sets this field as the Rank Port for the Transformation.
 * 
 */
public class RankWithTransformField extends Base {
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
        outputTarget = this.createFlatFileTarget( "Rank1_Output" );
    }

    /**
     * This method creates a simple mapping required for Rank Transformation.
     */
    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "Rank1 Mapping", "Rank1 Mapping", "This is sample for rank" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // creating DSQ Transformation
        RowSet dsqRS = (RowSet) helper.sourceQualifier( this.orderDetailSource ).getRowSets().get(
                0 );
        // create a rank Transformation
        // set the rank and rank port
        Field field = new Field( "FirstName", "FirstName", "", TransformationDataTypes.STRING, "10", "0",
                FieldKeyType.NOT_A_KEY, FieldType.TRANSFORM, false );
        TransformField rankPort = new TransformField( field, PortType.OUTPUT );
        rankPort.setExpr( "OrderID" );
        RowSet rankRS = (RowSet) helper.rank( new InputSet( dsqRS ), 5, "FirstName", null,
                "rank_transform", rankPort ).getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( rankRS, this.outputTarget );
        folder.addMapping( mapping );
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Rank1", "Session_For_Rank1",
                "This is session for Rank1" );
        session.setMapping( mapping );
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Rank1", "Workflow_for_Rank1",
                "This workflow for rank" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            RankWithTransformField rank = new RankWithTransformField();
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
