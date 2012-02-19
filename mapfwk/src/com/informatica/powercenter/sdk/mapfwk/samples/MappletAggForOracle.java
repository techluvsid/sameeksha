/*
 * MappletAggForOracle.java Created on Nov 4, 2005.
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
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;

public class MappletAggForOracle extends SimpleMapplet {
    protected void createTargets() {
        outputTarget = this.createRelationalTarget( "Aggregate_Output_Oracle",
                SourceTargetTypes.RELATIONAL_TYPE_ORACLE );
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "MappletAggForOracle", "MappletAggForOracle",
                "This is sample for MappletAggForOracle" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // To create a mapplet, we need a vector of group sets. So, we create a
        // vector of group sets first.
        GroupSet outputGroupSet1 = new GroupSet( this.getMappletOutputFields(), "Output1",
                GroupSetConstants.OUTPUTGROUP );
        Vector vec = new Vector();
        vec.add( outputGroupSet1 );
        // This mapplet should already be present in the Power Center
        // Repository. Please use the same name as that
        // of the mapplet. For e.g., as shown below, there should be a mapplet
        // with name "aggOracle" in the
        // Power Center Repository.
        RowSet dsqRS = (RowSet) helper.mapplet( vec, TransformationConstants.MAPPLET, "aggOracle" )
                .getRowSets().get( 0 );
        // create an aggregator Transformation
        // calculate cost per order using the formula
        // SUM((UnitPrice * Quantity) * (100 - Discount1) / 100) grouped by
        // OrderId
        TransformField cost = new TransformField(
                "number(15,0) total_cost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))" );
        RowSet aggRS = (RowSet) helper.aggregate( dsqRS, cost, new String[] { "OrderID" },
                "agg_transform" ).getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( aggRS, this.outputTarget );
        folder.addMapping( mapping );
    }

    // This method is used to define the vector of output fields coming out of
    // the Output transformation
    // of a mapplet.
    private Vector getMappletOutputFields() {
        Vector fields = new Vector();
        Field field1 = new Field( "OrderID", "OrderID", "", DataTypeConstants.DECIMAL, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field1 );
        Field field2 = new Field( "ProductID", "ProductID", "", DataTypeConstants.DECIMAL, "10",
                "0", FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field2 );
        Field field3 = new Field( "UnitPrice", "UnitPrice", "", DataTypeConstants.DECIMAL, "28",
                "4", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field3 );
        Field field4 = new Field( "Quantity", "Quantity", "", DataTypeConstants.DECIMAL, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field4 );
        Field field5 = new Field( "Discount", "Discount", "", DataTypeConstants.DECIMAL, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field5 );
        Field field6 = new Field( "VarcharFld", "VarcharFld", "", DataTypeConstants.STRING, "5",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field6 );
        Field field7 = new Field( "Varchar2Fld", "Varchar2Fld", "", DataTypeConstants.STRING, "5",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        fields.add( field7 );
        return fields;
    }

    public static void main( String args[] ) {
        try {
            MappletAggForOracle mapAggforOrcl = new MappletAggForOracle();
            if (args.length > 0) {
                if (mapAggforOrcl.validateRunMode( args[0] )) {
                    mapAggforOrcl.execute();
                }
            } else {
                mapAggforOrcl.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
