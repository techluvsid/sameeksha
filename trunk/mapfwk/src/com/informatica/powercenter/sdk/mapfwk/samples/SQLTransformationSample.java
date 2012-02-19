package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.SQLField;
import com.informatica.powercenter.sdk.mapfwk.core.SQLTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

public class SQLTransformationSample extends Base
{

        protected Source InputData;

        protected Target outputTarget;

        /**
         * Create sources
         */
        protected void createSources() {
            ConnectionInfo connInfo = getFlatFileConnectionInfo();
            connInfo.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME, "InputData.dat");
            InputData = new Source("InputData","InputData","","InputData",connInfo);
            InputData.setFields(createInputDataSource());
            folder.addSource(InputData);
        }
        
        private Vector createInputDataSource()
        {
            Vector fields = new Vector();
            Field field1 = new Field("ScriptName", "ScriptName", "",
                    DataTypeConstants.STRING, "10", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field1);

            Field field2 = new Field("ConnectString", "ConnectString", "", DataTypeConstants.STRING,
                    "14", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                    false);
            fields.add(field2);

            Field field3 = new Field("DBUser", "DBUser", "",
                    DataTypeConstants.STRING, "20", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field3);

            Field field4 = new Field("DBPassword", "DBPassword", "",
                    DataTypeConstants.STRING, "20", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field4);

            Field field5 = new Field("CodePage", "CodePage", "",
                    DataTypeConstants.STRING, "10", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field5);

            Field field6 = new Field("AdvancedOptions", "AdvancedOptions", "",
                    DataTypeConstants.STRING, "50", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field6);

            Field field7 = new Field("postcode", "postcode", "",
                    DataTypeConstants.STRING, "8", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field7);

            Field field8 = new Field("call_duration", "call_duration", "",
                    DataTypeConstants.INTEGER, "10", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field8);

            Field field9 = new Field("call_cost", "call_cost", "",
                    DataTypeConstants.INTEGER, "10", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field9);

            Field field10 = new Field("call_type_code", "call_type_code", "",
                    DataTypeConstants.STRING, "10", "0", FieldConstants.NOT_A_KEY,
                    Field.FIELDTYPE_SOURCE, false);
            fields.add(field10);

            return fields;
        }
        /**
         * Create targets
         */
        protected void createTargets() {
                outputTarget = this.createFlatFileTarget("SQL_Output");
        }

    protected void createMappings() throws Exception
    {
        mapping = new Mapping("SQLMapping", "SQLMapping",
        "This is sample for SQLMapping");
        setMapFileName(mapping);
        TransformHelper helper = new TransformHelper(mapping);

        //      creating DSQ Transformation
        RowSet dsqRS = (RowSet) helper.sourceQualifier(this.InputData)
        .getRowSets().get(0);
        
        Vector vTransformFields = new Vector();
        Field field1 = new Field("NewField1","NewField1","",DataTypeConstants.STRING,"20","0", FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        field1.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_ATTRIBUTE, new Integer(2));       
        field1.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_NATIVE_TYPE,"char");        
        TransformField tField1 = new TransformField(field1,TransformField.TYPE_OUTPUT);
        
        Field field2 = new Field("NewField2","NewField2","NewField2",DataTypeConstants.STRING,"20","0", FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        field2.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_ATTRIBUTE, new Integer(2));       
        field2.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_NATIVE_TYPE,"char");  
        TransformField tField2 = new TransformField(field2,TransformField.TYPE_INPUT);

        Field field3 = new Field("NewField3","NewField3","NewField3",DataTypeConstants.STRING,"20","0", FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        field3.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_ATTRIBUTE, new Integer(2));       
        field3.setAttributeValues(SQLTransformation.ATTR_SQLT_PORT_NATIVE_TYPE,"char");  
        TransformField tField3 = new TransformField(field3,TransformField.TYPE_OUTPUT);
        
        vTransformFields.add(tField1);
        vTransformFields.add(tField2);
        vTransformFields.add(tField3);
        
        InputSet ip = new InputSet (dsqRS,PortPropagationContextFactory.getContextForAllIncludeCols(),PortLinkContextFactory.getPortLinkContextByName());
        RowSet sqlRS = (RowSet) helper.sqlTransformation(ip, vTransformFields, false, SQLTransformation.DBTYPE_ORACLE, false, false, "SqlTransformation", null).getRowSets().get(0);
        
        //RowSet sqlRS = (RowSet) helper.sqlTransformation(new RowSet(),vTransformFields, false, SourceTargetTypes.RELATIONAL_TYPE_ORACLE, false, false, "SqlTransformation", null).getRowSets().get(0);
        mapping.writeTarget(sqlRS, this.outputTarget);

        folder.addMapping(mapping);
    }

        protected void createSession() throws Exception {
                session = new Session("Session_For_SQLT", "Session_For_SQLT",
                                "This is session for SQLT");
                session.setMapping(mapping);
        }

        /**
         * Create workflow
         */
        protected void createWorkflow() throws Exception {
                workflow = new Workflow("Workflow_for_SQLT",
                                "Workflow_for_SQLT", "This workflow for SQLT");
                workflow.addSession(session);
                folder.addWorkFlow(workflow);
        }


    /**
     * @param args
     */
    public static void main(String[] args)
    {
        try {
                SQLTransformationSample sql_trans = new SQLTransformationSample();
                if (args.length > 0) {
                        if (sql_trans.validateRunMode(args[0])) {
                            sql_trans.execute();
                        }
                } else {
                    sql_trans.printUsage();
                }
        } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Exception is: " + e.getMessage());
        }

    }

}
