/*
 * PrototypeForHyperion.java Created on April 6, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.powerconnect.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Vector;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.LookupField;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MetaExtension;
import com.informatica.powercenter.sdk.mapfwk.core.MetaExtensionDataType;
import com.informatica.powercenter.sdk.mapfwk.core.OutputField;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectConInfo;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectTarget;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.SessionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationProperties;
import com.informatica.powercenter.sdk.mapfwk.core.UnconnectedLookup;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.powerconnect.samples.*;
import com.informatica.powercenter.sdk.mapfwk.util.INIFile;

/**
 * This class demonstrates the mapping code for FlatFile Source->Hyperion target
 * mapping
 */
public class PrototypeForHyperion extends HyperionBase {
    protected Source PlanningRefAppDat;
    protected Target Disra_console_load;

    protected void createSources() {
        PlanningRefAppDat = createPlanningRefAppData();
    }

    protected void createTargets() {
        Disra_console_load = createHyperionTarget( "Disra_console_load" );
    }

    public Target createHyperionTarget( String name ) {
        Vector fields = new Vector();
        /*
         * Add Field objects to the Vector if you want the target fields to have
         * precision specified by you.
         */
        PowerConnectConInfo info = new PowerConnectConInfo( "EssbaseConnector", "EssbaseConnection" );
        info.getConnProps().setProperty( ConnectionPropsConstants.CONNECTIONNAME,
                "Essbase_PlanningRefData" );
        HyperionTarget employeeTarget = new HyperionTarget( "Disra_console_load",
                "Disra_console_load", "This is Disra_console_load Table", "Disra_console_load",
                info );
        employeeTarget.setFields( fields );
        return employeeTarget;
    }

    protected void createMappings() throws Exception {
        // Create mappings
        // Generates mapping name based on invoking class
        String mapName = this.getClass().getName();
        mapName = mapName.substring( mapName.lastIndexOf( "." ) + 1, mapName.length() );
        mapping = new Mapping( mapName, mapName, "This is a Hyperion Prototype" );
        setMapFileName( mapping );
        TransformHelper helper = new TransformHelper( mapping );
        // Mapping stage 1: creating DSQ Transformation for PlanningRefAppData
        OutputSet outSet = helper.sourceQualifier( PlanningRefAppDat );
        RowSet dsqRS = (RowSet) outSet.getRowSets().get( 0 );
        PortPropagationContext dsqRSContext1 = PortPropagationContextFactory
                .getContextForExcludeColsFromAll( new String[] { "Account" } );
        PortPropagationContext dsqRSContext2 = PortPropagationContextFactory
                .getContextForIncludeCols( new String[] { "Account" } );
        InputSet dsqIS1 = new InputSet( dsqRS, dsqRSContext1 );
        InputSet dsqIS2 = new InputSet( dsqRS, dsqRSContext2 );
        // The below HashMap is used to link fields explicitly - used for
        // linking ports individually
        Vector linkFields1 = getLinkFieldsForTgt();
        LinkedHashMap fMap2 = new LinkedHashMap();
        fMap2.put( dsqRS.getField( "Period" ), (Field) getLinkFieldsForTgt().get( 0 ) );
        fMap2.put( dsqRS.getField( "Entity" ), (Field) getLinkFieldsForTgt().get( 1 ) );
        fMap2.put( dsqRS.getField( "Segments" ), (Field) getLinkFieldsForTgt().get( 2 ) );
        fMap2.put( dsqRS.getField( "Channels" ), (Field) getLinkFieldsForTgt().get( 3 ) );
        fMap2.put( dsqRS.getField( "HSP_Rates" ), (Field) getLinkFieldsForTgt().get( 4 ) );
        fMap2.put( dsqRS.getField( "Scenario" ), (Field) getLinkFieldsForTgt().get( 5 ) );
        fMap2.put( dsqRS.getField( "Version" ), (Field) getLinkFieldsForTgt().get( 6 ) );
        fMap2.put( dsqRS.getField( "Currency" ), (Field) getLinkFieldsForTgt().get( 7 ) );
        fMap2.put( dsqRS.getField( "Year" ), (Field) getLinkFieldsForTgt().get( 8 ) );
        fMap2.put( dsqRS.getField( "Value" ), (Field) getLinkFieldsForTgt().get( 9 ) );
        PortLinkContext portLinkContext1 = PortLinkContextFactory.getPortLinkContextByMap( fMap2 );
        InputSet linkInputSet1 = new InputSet( dsqRS, portLinkContext1 );
        // Create Expression Transform
        Vector vTrnsField1 = new Vector();
        vTrnsField1
                .add( new TransformField(
                        "Account_Result",
                        DataTypeConstants.STRING,
                        "10",
                        "0",
                        "IIF( NOT ISNULL(:LKP.LKP_PLANNING_ACCOUNT(Account)),:LKP.LKP_PLANNING_ACCOUNT(Account),IIF(INSTR(Account,'00') = 1,SUBSTR(Account,LENGTH('00')),IIF((TO_INTEGER(Account) >= 500 AND TO_INTEGER(Account) <= 600),'502113','Not_Transformed')))" ) );
        RowSet expRS = (RowSet) helper.expression( dsqIS2.getOutRowSet(), vTrnsField1,
                "EX_AWB_MAPPING" ).getRowSets().get( 0 );
        PortPropagationContext expRSContext = PortPropagationContextFactory
                .getContextForExcludeColsFromAll( new String[] { "Account" } );
        InputSet expIS = new InputSet( expRS, expRSContext );
        // Create Unconnected lookup on Flat file
        Vector input = new Vector();
        Vector output = new Vector();
        createUncLkpFields( input, output );
        String condition = "Account_In = Account_From";
        UnconnectedLookup uncOP = helper.unconnectedLookup( "LKP_Planning_Account", null, input,
                condition, this.createUnConnectedLkupSource() );
        // Set properties for session for Flat file lookup and port types
        uncOP.setPortType( "Account_In", OutputField.TYPE_LOOKUP );
        uncOP.setPortType( "Account_From", OutputField.TYPE_INPUT );
        Vector linkFields = getLinkFieldsForExp();
        // To link fields explicitly
        // Use linked hash map to preserve order of the ports
        LinkedHashMap fMap1 = new LinkedHashMap();
        fMap1.put( expIS.getOutRowSet().getField( "Account_Result" ), (Field) getLinkFieldsForExp()
                .get( 0 ) );
        PortLinkContext portLinkContext = PortLinkContextFactory.getPortLinkContextByMap( fMap1 );
        InputSet linkInputSet = new InputSet( expRS, portLinkContext );
        // Add meta data extensions
        // The precision in constructor has no effect
        MetaExtension metaExt = new MetaExtension( "RuleFile", MetaExtensionDataType.STRING, "200",
                "", true );
        metaExt.setDescription( "This is RuleFile" );
        metaExt.setMaxLength( "200" );// Set the precision of the String
        Disra_console_load.addMetaExtension( metaExt );
        metaExt = new MetaExtension( "RuleSeperator", MetaExtensionDataType.STRING, "5", "", true );
        metaExt.setMaxLength( "5" );
        Disra_console_load.addMetaExtension( metaExt );
        metaExt = new MetaExtension( "TargetApp", MetaExtensionDataType.STRING, "50", "DISRA", true );
        metaExt.setMaxLength( "50" );
        Disra_console_load.addMetaExtension( metaExt );
        metaExt = new MetaExtension( "TargetDatabase", MetaExtensionDataType.STRING, "50",
                "Consol", true );
        metaExt.setMaxLength( "50" );
        Disra_console_load.addMetaExtension( metaExt );
        metaExt = new MetaExtension( "TargetDatabaseOtl", MetaExtensionDataType.STRING, "50",
                "Consol", true );
        metaExt.setMaxLength( "50" );
        Disra_console_load.addMetaExtension( metaExt );
        metaExt = new MetaExtension( "TargetType", MetaExtensionDataType.NUMBER, "", "2", true );
        Disra_console_load.addMetaExtension( metaExt );
        // Write to targets
        Vector vTgtInset = new Vector();
        vTgtInset.add( linkInputSet );
        vTgtInset.add( linkInputSet1 );
        mapping.writeTarget( vTgtInset, Disra_console_load );
        folder.addMapping( mapping );
    }

    void createUncLkpFields( Vector input, Vector output ) {
        Field field1 = new Field( "Account_From", "Account_From", "", DataTypeConstants.STRING,
                "80", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        input.add( field1 );
        Field field2 = new Field( "Account_Out", "Account_Out", "", DataTypeConstants.STRING, "80",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        output.add( field2 );
        Field retField = new Field( "Account_In", "Account_In", "", DataTypeConstants.STRING, "80",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        output.add( retField );
    }

    public Vector getLinkFieldsForExp() {
        Vector vFields = new Vector();
        Field field1 = new Field( "Account", "Account", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field1 );
        return vFields;
    }

    public Vector getLinkFieldsForTgt() {
        Vector vFields = new Vector();
        Field field2 = new Field( "Period", "Period", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        // Set the attributes for fields as required.
        field2.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "1" );
        field2.setAttributeValues( HyperionAttributes.ASSOCIATED_DIMENSION, "Product" );
        field2.setAttributeValues( HyperionAttributes.UDA, "1" );
        field2.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "" );
        field2.setAttributeValues( HyperionAttributes.MEMBER_FILTER,
                "<SORTNONE <DIMBOTTOM \"Product\"" );
        field2.setAttributeValues( HyperionAttributes.DTS_MEMBERS, "1" );
        vFields.add( field2 );
        Field field3 = new Field( "Entity", "Entity", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        field3.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "1" );
        field3.setAttributeValues( HyperionAttributes.ASSOCIATED_DIMENSION, "Product" );
        field3.setAttributeValues( HyperionAttributes.UDA, "1" );
        field3.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "" );
        field3.setAttributeValues( HyperionAttributes.MEMBER_FILTER,
                "<SORTNONE <DIMBOTTOM \"Product\"" );
        field3.setAttributeValues( HyperionAttributes.DTS_MEMBERS, "1" );
        vFields.add( field3 );
        Field field4 = new Field( "Segments", "Segments", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        field4.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "1" );
        field4.setAttributeValues( HyperionAttributes.ASSOCIATED_DIMENSION, "Product" );
        field4.setAttributeValues( HyperionAttributes.UDA, "1" );
        field4.setAttributeValues( HyperionAttributes.COLUMN_TYPE, "" );
        field4.setAttributeValues( HyperionAttributes.MEMBER_FILTER,
                "<SORTNONE <DIMBOTTOM \"Product\"" );
        field4.setAttributeValues( HyperionAttributes.DTS_MEMBERS, "1" );
        vFields.add( field4 );
        Field field5 = new Field( "Channels", "Channels", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field5 );
        Field field6 = new Field( "HSP_Rates", "HSP_Rates", "", DataTypeConstants.STRING, "80",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field6 );
        Field field7 = new Field( "Scenario", "Scenario", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field7 );
        Field field8 = new Field( "Version", "Version", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field8 );
        Field field9 = new Field( "Currency", "Currency", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field9 );
        Field field10 = new Field( "Year", "Year", "", DataTypeConstants.STRING, "80", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field10 );
        Field field11 = new Field( "Data", "Data", "", DataTypeConstants.NUMBER, "15", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false );
        vFields.add( field11 );
        return vFields;
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

    protected Source createUnConnectedLkupSource() {
        Source temp = null;
        Vector vFields = new Vector();
        Field field1 = new Field( "Account_Out", "Account_Out", "", DataTypeConstants.STRING, "80",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        vFields.add( field1 );
        Field field2 = new Field( "Account_In", "Account_In", "", DataTypeConstants.STRING, "80",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TRANSFORM, false );
        vFields.add( field2 );
        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty( ConnectionPropsConstants.SOURCE_FILENAME,
                "LookupFlatFile.txt" );
        temp = new Source( "LKP_Planning_Source", "LKP_Planning_Source",
                "This is LKP_Planning_Source table", "LKP_Planning_Source", info );
        temp.setFields( vFields );
        return temp;
    }

    public static void main( String[] args ) {
        try {
            PrototypeForHyperion mapGenObj = new PrototypeForHyperion();
            mapGenObj.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println( "Exception is: " + e.getMessage() );
        }
    }
}