/*
 * SimpleExample.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Iterator;
import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationContext;

/**
 * @author pbalakri
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
public class SimpleExample {
    Repository repos;
    Folder folder;
    Mapping mapping;
    Source src;
    Target tgt;
    String mapFileName;

    /**
     * Default constructor
     */
    public SimpleExample() {
    }

    /**
     * Create all objects
     */
    public void create() {
        createRepos();
        createFolder();
        try {
            createMappingWithoutHelper();
            generateOutput();
        } catch (Exception excp) {
            excp.printStackTrace();
        }
    }

    /**
     * Create Repository
     */
    private void createRepos() {
        repos = new Repository( "Test", "Test", "Simple API Test" );
    }

    /**
     * Create Folder
     */
    private void createFolder() {
        folder = new Folder( "TestFolder", "TestFolder", "Simple API Test" );
        repos.addFolder( folder );
        setupSource();
        setupTarget();
    }

    /**
     * Set up source details
     */
    private void setupSource() {
        src = new Source( "InputData", "InputData", "", "CB_FFSRC", new ConnectionInfo(
                SourceTargetTypes.FLATFILE_TYPE ) );
        src.setFields( createSourceFields() );
        folder.addSource( src );
    }

    /**
     * Set up target details
     */
    private void setupTarget() {
        tgt = new Target( "output1", "simple output target", "", "", new ConnectionInfo(
                SourceTargetTypes.FLATFILE_TYPE ) );
    }

    /**
     * Method to create fields
     */
    private Vector createSourceFields() {
        Vector fields = new Vector();
        Field field1 = new Field( "account_id", "account_id", "", DataTypeConstants.STRING, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field1 );
        Field field2 = new Field( "CTN", "CTN", "", DataTypeConstants.STRING, "14", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field2 );
        Field field3 = new Field( "title", "title", "", DataTypeConstants.STRING, "20", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field3 );
        Field field4 = new Field( "middle_initial", "middle_initial", "", DataTypeConstants.STRING,
                "1", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field4 );
        Field field5 = new Field( "surname", "surname", "", DataTypeConstants.STRING, "50", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field5 );
        Field field6 = new Field( "postcode", "postcode", "", DataTypeConstants.STRING, "8", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field6 );
        Field field7 = new Field( "call_duration", "call_duration", "", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field7 );
        Field field8 = new Field( "call_cost", "call_cost", "", DataTypeConstants.DECIMAL, "10",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field8 );
        Field field9 = new Field( "call_type_code", "call_type_code", "", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field9 );
        return fields;
    }

    /**
     * Create mapping flow
     */
    /*
     * private void createMapping() throws InvalidTransformationException {
     * mapping = new Mapping("Simple Example Mapping", "Simple Example Mapping",
     * "This is simple example"); mapFileName = getMapFileName( mapping );
     * 
     * TransformHelper helper = new TransformHelper( mapping );
     *  // create DSQ Transformation RowSet rs_dsq =
     * (RowSet)helper.sourceQualifier( src ).getRowSets().get( 0 );
     *  // create expression transformation // concatnate middle_initial and
     * surname TransformField field = new TransformField( "string(100,0)
     * fullName=(middle_initial || surname)" ); RowSet rs_exp =
     * (RowSet)helper.expression( rs_dsq, field, "Expression_Transformation"
     * ).getRowSets().get( 0 );
     *  // create aggregate transformation // calculate sum(call_cost) group by
     * account_id TransformField field1 = new TransformField( "integer
     * total_call_cost=(sum(call_cost))"); String[] groupBy = { "account_id" };
     * RowSet rs_agg = (RowSet)helper.aggregate( rs_exp, field1, groupBy,
     * "Aggregate_Transformation" ).getRowSets().get( 0 );
     *  // write to target mapping.writeTarget( rs_agg, tgt );
     * 
     * folder.addMapping( mapping ); }
     */
    /**
     * Create mapping without using the TransformHelper
     */
    private void createMappingWithoutHelper() throws InvalidTransformationException {
        mapping = new Mapping( "Simple Example Mapping", "Simple Example Mapping",
                "This is simple example" );
        mapFileName = getMapFileName( mapping );
        // Pipeline for this sample is:
        // source -> source qualifier -> expression -> aggregate -> target
        // Step 1: Create source qualifier for source
        Transformation dsqTrans = src.createDSQTransform();
        mapping.addTransformation( dsqTrans );
        OutputSet outSet = dsqTrans.apply();
        RowSet dsqRowSet = (RowSet) outSet.getRowSets().get( 0 );
        // Step 2: Use the RowSet from source qualifier and use expression
        // transformation to add an expression
        // Expressions (as specified in Expression Builder) can be directly
        // created using the
        // TransformField object
        TransformField field = new TransformField(
                "string(100,0) fullName=(middle_initial || surname)" );
        TransformationContext tc = new TransformationContext( dsqRowSet );
        Transformation expTrans = tc.createTransform( TransformationConstants.EXPRESSION );
        mapping.addTransformation( expTrans );
        expTrans.add( field );
        OutputSet outSet2 = expTrans.apply();
        RowSet expRowSet = (RowSet) outSet2.getRowSets().get( 0 );
        // Step 3: Use the RowSet from expression and use aggregate
        // transformation
        TransformField field1 = new TransformField( "integer total_call_cost=(sum(call_cost))" );
        String[] groupBy = { "account_id" };
        InputSet aggInputSet = new InputSet( expRowSet );
        aggInputSet.setGroupBy( groupBy );
        TransformationContext tc1 = new TransformationContext( aggInputSet );
        Transformation aggTrans = tc1.createTransform( TransformationConstants.AGGREGATION );
        mapping.addTransformation( aggTrans );
        aggTrans.add( field1 );
        OutputSet outSet3 = aggTrans.apply();
        RowSet aggRowSet = (RowSet) outSet3.getRowSets().get( 0 );
        // Step 4: write rowset to target
        mapping.writeTarget( aggRowSet, tgt );
        folder.addMapping( mapping );
    }

    private String getMapFileName( Mapping mapping ) {
        StringBuffer buff = new StringBuffer();
        buff.append( System.getProperty( "user.dir" ) );
        buff.append( java.io.File.separatorChar );
        buff.append( mapping.getName() );
        buff.append( ".xml" );
        return buff.toString();
    }

    /**
     * Write output
     */
    private void generateOutput() throws Exception {
        MapFwkOutputContext outputContext = new MapFwkOutputContext(
                MapFwkOutputContext.OUTPUT_FORMAT_XML, MapFwkOutputContext.OUTPUT_TARGET_FILE,
                mapFileName );
        repos.save( outputContext, false );
        System.err.println( "Mapping file generated is: [" + mapFileName + "]" );
    }

    public static void main( String[] args ) {
        SimpleExample simpleExample = new SimpleExample();
        simpleExample.create();
    }
}
