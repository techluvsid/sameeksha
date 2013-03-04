/**
 * Copyright 2010 Informatica Corporation. All rights reserved.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.ArrayList;
import java.util.List;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.SAPASQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.SAPFunction;
import com.informatica.powercenter.sdk.mapfwk.core.SAPScalarInputValueType;
import com.informatica.powercenter.sdk.mapfwk.core.SAPStructure;
import com.informatica.powercenter.sdk.mapfwk.core.SAPStructureField;
import com.informatica.powercenter.sdk.mapfwk.core.SAPStructureInstance;
import com.informatica.powercenter.sdk.mapfwk.core.SAPVariable;
import com.informatica.powercenter.sdk.mapfwk.core.SAPVariableCategory;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.StringConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 * This is a sample program that demonstrates how to create a sample mapping
 * with sap source and sap application source qualifier.
 *<P>
 * 1. Create a mapping with following structure:
 * <P>
 * SAP source --> ASQ --> Expr Transform --> Flatfile Target
 *<P>
 * 2. Create a session for the mapping
 *<P>
 * 3. Create workflow using the session created in step 2.
 * 
 * @author ufarooqu
 * @since 9.0.1
 * 
 */
public class SAPMappingExample extends Base {

	private Mapping mapping = null;
	private Source ekkoSAPSrc = null;
	private Target fileTgt = null;
	private SAPASQTransformation dsq;
	
	private SAPStructure sapStruc1;
	private SAPStructure sapStruc2;
	private SAPStructure sapStruc3;
	private SAPStructure sapStruc4;
	private SAPStructure sapStruc5;

	/*
	 * Create a SAP source
	 */
	protected void createSources() {
		ekkoSAPSrc = this.createSAPekkoSource("sophie");
		folder.addSource(ekkoSAPSrc);
	}

	/*
	 * Create a Flatfile target
	 */
	protected void createTargets() {
		fileTgt = this.createFlatFileTarget("Output1");
	}

	/*
	 * Create a mapping with the structure below SAP Source --> ASQ --> Flatfile Target
	 */
	protected void createMappings() throws Exception {

		// create a mapping object
		mapping = new Mapping("M_SAP_EKKO_SAMPLE_MAPPING",
				"M_SAP_EKKO_SAMPLE_MAPPING",
				"Mapping to test mapping with an SAP Source");
		setMapFileName(mapping);

		// create transform helper
		TransformHelper helper = new TransformHelper(mapping);

		// create DSQ
		dsq = (SAPASQTransformation) ekkoSAPSrc.createASQTransform();
		mapping.addTransformation(dsq);

		//Create Structures
		createSAPStructures();
		
		SAPVariable var1 = new SAPVariable("var1", SAPVariableCategory.ABAPTYPE, 
				NativeDataTypes.SAP.CHAR, "0", "10", "10", true);
		dsq.addSAPVariable(var1);
		SAPVariable var2 = new SAPVariable("var2", SAPVariableCategory.STRUCTURETYPE, "Definition");
		dsq.addSAPVariable(var2);
		SAPVariable var3 = new SAPVariable("var3", SAPVariableCategory.STRUCTUREFIELDTYPE, "initialValue", "def-def");
		dsq.addSAPVariable(var3);
		
		/**
		 * SAPFunction with ScalarInput and Table
		 */
		SAPFunction func2 = new SAPFunction("Z_PM_RFC_RUN_PROGRAM", "A RFC callable function to run programs for staging data in files.");
		
		SAPStructureField fld1 = new SAPStructureField("/INFATRAN/ZPMDATATYPE-CHAR32", NativeDataTypes.SAP.CHAR, "PROGRAM_NAME", "32", "0", false, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld1);
		SAPStructureField fld2 = new SAPStructureField("/INFATRAN/ZPMDATATYPE-FILENAME", NativeDataTypes.SAP.CHAR, "OUTPUT_FILENAME", "128", "0", false, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld2);
		SAPStructureField fld3 = new SAPStructureField("/INFATRAN/ZPMDATATYPE-CHAR32", NativeDataTypes.SAP.CHAR, "FORM_NAME", "32", "0", false, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld3);
		SAPStructureField fld4 = new SAPStructureField("/INFATRAN/ZPRAMS-PARAMKEY", NativeDataTypes.SAP.CHAR, "PARAMID", "10", "0", false, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld4);
		SAPStructureField fld5 = new SAPStructureField("/INFATRAN/ZPMDATATYPE-CHAR1", NativeDataTypes.SAP.CHAR, "MODE", "1", "0", true, "' '", SAPScalarInputValueType.DEFAULT);
		func2.addSAPFunctionScalarInput(fld5);
		SAPStructureField fld6 = new SAPStructureField("/INFATRAN/ZPRAMS-STRF2", NativeDataTypes.SAP.INT4, "PACKAGESIZE", "10", "0", true, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld6);
		SAPStructureField fld7 = new SAPStructureField("/INFATRAN/ZPRAMS-STRF2", NativeDataTypes.SAP.INT4, "NUMOFROWS", "10", "0", true, "<None>", SAPScalarInputValueType.NONE);
		func2.addSAPFunctionScalarInput(fld7);

		SAPStructureInstance sapStrucinst1 = new SAPStructureInstance(sapStruc2, "SELECTFIELDLIST");
		func2.addSAPFunctionScalarTable(sapStrucinst1);
		SAPStructureInstance sapStrucinst2 = new SAPStructureInstance(sapStruc3, "SELECTFIELDDETAIL");
		func2.addSAPFunctionScalarTable(sapStrucinst2);
		SAPStructureInstance sapStrucinst3 = new SAPStructureInstance(sapStruc2, "ORDERFIELDLIST");
		func2.addSAPFunctionScalarTable(sapStrucinst3);
		
		dsq.addSAPFunction(func2);
		
		/**
		 * SAPFunction with Table and Changing
		 */
		SAPFunction func4 = new SAPFunction("ZTEMP_TABPARAM", "TABPARAM");
		
		SAPStructureInstance sapStrucinst4 = new SAPStructureInstance(sapStruc4, "ZTABONE");
		func4.addSAPFunctionScalarTable(sapStrucinst4);
		SAPStructureInstance sapStrucinst5 = new SAPStructureInstance(sapStruc4, "TEMPCHANGE");
		func4.addSAPFunctionScalarChanging(sapStrucinst5);	
		
		dsq.addSAPFunction(func4);
		
		/**
		 * SAP Function containing scalar input and scalar output.
		 */
		SAPFunction func5 = new SAPFunction("ZCHAR_UNISCALAR", "RFC for unicode testing");
		SAPStructureField func5fld1 = new SAPStructureField("ZCHAR_UNI-FKEY", NativeDataTypes.SAP.CHAR, "FKEY_OUT", "10", "0", false);
		func5.addSAPFunctionScalarOutput(func5fld1);
		SAPStructureField func5fld2 = new SAPStructureField("ZZCHAR_UNI-FCHAR", NativeDataTypes.SAP.CHAR, "FCHAR_OUT", "255", "0", false);
		func5.addSAPFunctionScalarOutput(func5fld2);
		SAPStructureField func5fld3 = new SAPStructureField("ZCHAR_UNI-FKEY", NativeDataTypes.SAP.CHAR, "FKEYTYPE_OUT", "10", "0", false);
		func5.addSAPFunctionScalarOutput(func5fld3);
		SAPStructureField func5fld4 = new SAPStructureField("ZCHAR_UNI-FCHAR", NativeDataTypes.SAP.CHAR, "FCHARTYPE_OUT", "255", "0", false);
		func5.addSAPFunctionScalarOutput(func5fld4);
		
		SAPStructureField func5fld5 = new SAPStructureField("ZCHAR_UNI-FKEY", NativeDataTypes.SAP.CHAR, "FKEY_IN", "10", "0", false, StringConstants.NONETAG, SAPScalarInputValueType.NONE);
		func5.addSAPFunctionScalarInput(func5fld5);
		SAPStructureField func5fld6 = new SAPStructureField("ZCHAR_UNI-FCHAR", NativeDataTypes.SAP.CHAR, "FCHAR_IN", "255", "0", false, StringConstants.NONETAG, SAPScalarInputValueType.NONE);
		func5.addSAPFunctionScalarInput(func5fld6);
		SAPStructureField func5fld7 = new SAPStructureField("ZCHAR_UNI-FKEY", NativeDataTypes.SAP.CHAR, "FKEYTYPE_IN", "10", "0", false, StringConstants.NONETAG, SAPScalarInputValueType.NONE);
		func5.addSAPFunctionScalarInput(func5fld7);
		SAPStructureField func5fld8 = new SAPStructureField("ZCHAR_UNI-FCHAR", NativeDataTypes.SAP.CHAR, "FCHARTYPE_IN", "255", "0", false, StringConstants.NONETAG, SAPScalarInputValueType.NONE);
		func5.addSAPFunctionScalarInput(func5fld8);
		
		dsq.addSAPFunction(func5);
		
		RowSet dsqRowSet = dsq.apply().getRowSets().get(0);
		
		// create an expression transformation with a output port "AVG_SALARY".,
		// of type decimal, which provides average of MIN_SALARY and MAX_SALARY
		String expr = "String (10,0)AVG_SALARY = PINCR";
		TransformField outField = new TransformField(expr);
		//Create expression transformation
		List<TransformField> fields = new ArrayList<TransformField>();
		fields.add(outField);
		RowSet expRS = (RowSet) helper.expression(dsqRowSet, fields,
				"exp_transform").getRowSets().get(0);

		// write to target
		mapping.writeTarget(expRS, this.fileTgt);
		folder.addMapping(mapping);
	}
	
	protected void createSAPStructures()
	{
		sapStruc2 = new SAPStructure("/INFATRAN/ZPMSELFLDLIST");
		SAPStructureField sapStruc2fld1 = new SAPStructureField("CONTINUATION", NativeDataTypes.SAP.CHAR, "1", "0");
		sapStruc2.addSAPStructureFields(sapStruc2fld1);
		SAPStructureField sapStruc2fld2 = new SAPStructureField("COLUMNLIST", NativeDataTypes.SAP.CHAR, "1024", "0");
		sapStruc2.addSAPStructureFields(sapStruc2fld2);
		
		sapStruc3 = new SAPStructure("/INFATRAN/ZPMSELFLDDETAIL");
		SAPStructureField sapStruc3fld1 = new SAPStructureField("TABNAME", NativeDataTypes.SAP.CHAR, "32", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld1);
		SAPStructureField sapStruc3fld2 = new SAPStructureField("FIELDNAME", NativeDataTypes.SAP.CHAR, "32", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld2);
		SAPStructureField sapStruc3fld3 = new SAPStructureField("LENG", NativeDataTypes.SAP.NUMC, "6", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld3);
		SAPStructureField sapStruc3fld4 = new SAPStructureField("INTLEN", NativeDataTypes.SAP.NUMC, "6", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld4);
		SAPStructureField sapStruc3fld5 = new SAPStructureField("DECIMALS", NativeDataTypes.SAP.NUMC, "6", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld5);
		SAPStructureField sapStruc3fld6 = new SAPStructureField("DATATYPE", NativeDataTypes.SAP.CHAR, "4", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld6);
		SAPStructureField sapStruc3fld7 = new SAPStructureField("FLDOFFSET", NativeDataTypes.SAP.NUMC, "6", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld7);
		SAPStructureField sapStruc3fld8 = new SAPStructureField("INTTYPE", NativeDataTypes.SAP.CHAR, "1", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld8);
		SAPStructureField sapStruc3fld9 = new SAPStructureField("FLDALIAS", NativeDataTypes.SAP.CHAR, "64", "0");
		sapStruc3.addSAPStructureFields(sapStruc3fld9);
		
		sapStruc4 = new SAPStructure("ZTABONE");
		SAPStructureField sapStruc4fld1 = new SAPStructureField("FIELD1", NativeDataTypes.SAP.INT4, "10", "0");
		sapStruc4.addSAPStructureFields(sapStruc4fld1);
		SAPStructureField sapStruc4fld2 = new SAPStructureField("FIELD2", NativeDataTypes.SAP.CHAR, "100", "0");
		sapStruc4.addSAPStructureFields(sapStruc4fld2);
		SAPStructureField sapStruc4fld3 = new SAPStructureField("FIELD3", NativeDataTypes.SAP.CHAR, "100", "0");
		sapStruc4.addSAPStructureFields(sapStruc4fld3);
		SAPStructureField sapStruc4fld4 = new SAPStructureField("FIELD4", NativeDataTypes.SAP.CHAR, "100", "0");
		sapStruc4.addSAPStructureFields(sapStruc4fld4);
	}

	/*
	 * Create a session
	 */
	protected void createSession() throws Exception {
		session = new Session("SampleSAPSession", "SampleSAPSession",
				"SAP session with sap source");
		session.setMapping(mapping);
		
        ConnectionInfo connInfo = getRelationalConnectionInfo(SourceTargetType.SAP_R3);
        
        connInfo.getConnProps().setProperty( ConnectionPropsConstants.SESSION_EXTENSION_NAME, StringConstants.SAP_STAGING_READER );
    	
        //SAP ASQ Session level properties
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.STAGE_FILE_DIRECTORY, "STAGE_FILE_DIRECTORY" );
    	connInfo.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILE_DIRECTORY,"SOURCE_FILE_DIRECTORY");
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.STAGE_FILE_NAME, "asq_ekko" );    	
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.REINITIALIZE_THE_STAGE_FILE, "YES" );    	
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.PERSIST_THE_STAGE_FILE, "YES" );    	
    	connInfo.getConnProps().setProperty( ConnectionPropsConstants.RUN_SESSION_IN_BACKGROUND, "YES" );
    	session.addConnectionInfoObject(dsq, connInfo);
    	
	}

	/*
	 * Create workflow using SAP session
	 */
	protected void createWorkflow() throws Exception {
		workflow = new Workflow("WF_Sample_SAP_Workflow",
				"WF_Sample_SAP_Workflow", "Workflow for sap session");
		workflow.addSession(session);
		folder.addWorkFlow(workflow);
	}

	/*
	 * Sample program
	 */
	public static void main(String[] args) {
		try {
			SAPMappingExample sapMapping = new SAPMappingExample();
			if (args.length > 0) {
				if (sapMapping.validateRunMode(args[0])) {
					sapMapping.execute();
				}
			} else {
				sapMapping.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}
