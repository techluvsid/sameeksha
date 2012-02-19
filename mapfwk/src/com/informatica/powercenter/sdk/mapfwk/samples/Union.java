/*
 * Union.java Created on Jul 4, 2005.
 * 
 * Copyright 2004 Informatica Corporation. All rights reserved. INFORMATICA
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
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
 * Sample for Union transformation using Mapping framework API
 * 
 */
public class Union extends Base {
    protected Source itemsSrc;
    protected Source productSrc;
    protected Target outputTarget;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        itemsSrc = this.createItemsSource();
        folder.addSource( itemsSrc );
        productSrc = this.createProductsSource();
        folder.addSource( productSrc );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget( "Union_Output" );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping( "UnionMapping", "UnionMapping",
                "This is Union Transformation sample" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // Create a OutputRowSet
        RowSet rsGroupFld = new RowSet();
        Field field1 = new Field( "ItemId", "ItemId", "", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        rsGroupFld.add( field1 );
        Field field2 = new Field( "Item_Name", "Item_Name", "", DataTypeConstants.STRING, "72",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        rsGroupFld.add( field2 );
        Field field3 = new Field( "Item_Price", "Item_Price", "", DataTypeConstants.DECIMAL, "10",
                "2", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        rsGroupFld.add( field3 );
        // creating DSQ Transformation
        OutputSet itemOSet = helper.sourceQualifier( itemsSrc );
        RowSet itemRowSet = (RowSet) itemOSet.getRowSets().get( 0 );
        // itemRowSet.setGroupName("ITEM_GROUP");
        OutputSet productOSet = helper.sourceQualifier( productSrc );
        RowSet productRowSet = (RowSet) productOSet.getRowSets().get( 0 );
        // productRowSet.setGroupName("PRODUCT_GROUP");
        // Port propogation for Items and products
        PortPropagationContext itemRSContext = PortPropagationContextFactory
                .getContextForIncludeCols( new String[] { "ItemId", "Item_Name", "Price" } );
        PortPropagationContext productRSContext = PortPropagationContextFactory
                .getContextForIncludeCols( new String[] { "Item_No", "Item_Name", "Cust_Price" } );
        Vector vInputSet = new Vector();
        vInputSet.add( new InputSet( itemRowSet, itemRSContext ) );
        vInputSet.add( new InputSet( productRowSet, productRSContext ) );
        // create a Union Transformation
        RowSet unionRS = (RowSet) helper.union( vInputSet, rsGroupFld, "Union_transform" )
                .getRowSets().get( 0 );
        // write to target
        mapping.writeTarget( unionRS, outputTarget );
        folder.addMapping( mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception {
        session = new Session( "Session_For_Union", "Session_For_Union",
                "This is session for Union" );
        session.setMapping( this.mapping );
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow( "Workflow_for_Union", "Workflow_for_Union",
                "This workflow for Union" );
        workflow.addSession( session );
        folder.addWorkFlow( workflow );
    }

    public static void main( String[] args ) {
        try {
            Union union = new Union();
            if (args.length > 0) {
                if (union.validateRunMode( args[0] )) {
                    union.execute();
                }
            } else {
                union.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}
