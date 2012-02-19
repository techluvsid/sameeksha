/*
 * SimpleMappletTransformation.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSet;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSetConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

public class SimpleMappletTransformation extends Base {
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
        outputTarget = this.createFlatFileTarget( "Mapplet_Transform_Output" );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "SimpleMappletTransform", "SimpleMappletTransform",
                "This is sample for Simple MappletTransform" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        RowSet dsqRS = (RowSet) helper.sourceQualifier( this.orderDetailSource ).getRowSets().get(
                0 );
        // To create a mapplet, we need a vector of group sets. So, we create a
        // vector of group sets first.
        GroupSet inputGroupSet1 = new GroupSet( dsqRS, "Input1", GroupSetConstants.INPUTGROUP );
        GroupSet outputGroupSet1 = new GroupSet( this.getMappletOutputFields(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        Vector vec = new Vector();
        vec.add( inputGroupSet1 );
        vec.add( outputGroupSet1 );
        // This mapplet should already be present in the Power Center
        // Repository. Please use the same name as that
        // of the mapplet. For e.g., as shown below, there should be a mapplet
        // with name "AggNSorter" in the
        // Power Center Repository.
        RowSet mapRS = (RowSet) helper.mapplet( vec, TransformationConstants.MAPPLET, "AggNSorter" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( mapRS, this.outputTarget );
        folder.addMapping( mapping );
    }

    protected void createSession() throws Exception {
        session = new Session( "Session_For_" + mapping.getName(), "Session_For_"
                + mapping.getName(), "This is session for " + mapping.getName() );
        session.setMapping( this.mapping );
    }

    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_" + mapping.getName(), "Workflow_for_"
                + mapping.getName(), "This workflow for " + mapping.getName() );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String args[] ) {
        try {
            SimpleMappletTransformation simMapTrans = new SimpleMappletTransformation();
            if (args.length > 0) {
                if (simMapTrans.validateRunMode( args[0] )) {
                    simMapTrans.execute();
                }
            } else {
                simMapTrans.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }

    // This method is used to define the vector of output fields coming out of
    // the Output transformation
    // of a mapplet.
    private Vector getMappletOutputFields() {
        Vector fields = new Vector();
        Field field1 = new Field( "NOrderID", "NOrderID", "", DataTypeConstants.DECIMAL, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field1 );
        Field field2 = new Field( "NProductID", "NProductID", "", DataTypeConstants.DECIMAL, "10",
                "0", FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field2 );
        Field field3 = new Field( "NUnitPrice", "NUnitPrice", "", DataTypeConstants.DECIMAL, "28",
                "4", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field3 );
        Field field4 = new Field( "NQuantity", "NQuantity", "", DataTypeConstants.DECIMAL, "5",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field4 );
        Field field5 = new Field( "NDiscount", "NDiscount", "", DataTypeConstants.DECIMAL, "5",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field5 );
        Field field6 = new Field( "NVarcharFld", "NVarcharFld", "", DataTypeConstants.STRING, "5",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field6 );
        Field field7 = new Field( "NVarchar2Fld", "NVarchar2Fld", "", DataTypeConstants.STRING,
                "5", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field7 );
        Field field8 = new Field( "Ntotal_cost", "Ntotal_cost", "", DataTypeConstants.DECIMAL,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field8 );
        return fields;
    }
}
