/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Iterator;
import java.util.Vector;

import javax.xml.bind.JAXBException;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkException;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MetaExtension;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PowerCenterCompatibilityFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectSource;
import com.informatica.powercenter.sdk.mapfwk.core.PowerConnectTarget;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SourceGroup;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TargetGroup;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.Factory.PowerConnectSourceFactory;
import com.informatica.powercenter.sdk.mapfwk.core.Factory.PowerConnectTargetFactory;
import com.informatica.powercenter.sdk.mapfwk.exceptions.SourceTargetDefinitionNotFoundException;

/**
 * 
 * @author rjain
 *
 */
public class PowerExchangeTargetSample extends Base
{
    PowerConnectSource source;
    PowerConnectTarget outputTarget;
    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception
    {
        mapping = new Mapping("myMapping", "myMapping",
                "Testing sample with myMapping");
        TransformHelper helper = new TransformHelper(mapping);
        OutputSet asqOutputSet=(OutputSet)helper.sourceQualifier(source);
        
/*        RowSet asqRS = (RowSet) helper.sourceQualifier(source)
                .getRowSets().get(0);
*/        // creating ASQ Transformation
        /*
         * Transformation asqTrans = employeeSource.createASQTransform(); RowSet
         * asqRS = (RowSet) asqTrans.apply().getRowSets().get(0);
         * mapping.addTransformation( asqTrans );
         */
        // write to target
        Vector asqRS=new Vector();
        asqRS.addAll(asqOutputSet.getRowSets());
        Vector vin = new Vector();
        Iterator iter=asqRS.iterator();
        
        
        //use PortLinkContextByPosition if fields name in target are not same to those which comes from previous transformation.
        //if fileds name are same then we can directly create input set.
        int i=0;
        while(iter.hasNext())
        {
            Vector v1=new Vector();
            v1.add(outputTarget.getFields().get(i++));
            v1.add(outputTarget.getFields().get(i++));
            PortLinkContext portLinkContext = PortLinkContextFactory
            .getPortLinkContextByPosition( v1);
            
            RowSet rs=(RowSet)iter.next();
            InputSet asqIS = new InputSet(rs,portLinkContext);
            vin.add(asqIS);
        }
        //InputSet asqIS = new InputSet(asqRS);
        
        //vin.add(asqIS);
        mapping.writeTarget(vin, outputTarget);
        /*PortPropagationContext m_objPropContext=
        PortLinkContext m_objPortLinkContext;*/
       //mapping.linkPowerConnectTarget(vin,outputTarget);
        folder.addMapping(mapping);

    }

    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception
    {
        session = new Session("Session_For_PowerExchange",
                "Session_For_PowerExcange", "This is session for PowerExcange");
        session.setMapping(this.mapping);       
    }

    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources()
    {
        PowerConnectSourceFactory sourceFactory = PowerConnectSourceFactory.getInstance();
        try
        {
            source = sourceFactory.getPowerConnectSourceInstance("PWX_SEQ_NRDB2", "mySource", "mySourceDBD", "mySource", "mySource");
            SourceGroup srcGrp = new SourceGroup("stqa",null);
            source.createField("sdsa",srcGrp,"","","NUM32","10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
            source.createField("gfd",srcGrp,"","","DOUBLE","15","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
            
            SourceGroup srcGrp1 = new SourceGroup("stqa1",null);
            source.createField("ghfdyt",srcGrp1,"","","NUM32","10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
            source.createField("jk",srcGrp1,"","","DOUBLE","15","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
            source.setMetaExtensionValue("Access Method", "S");
            //source.setMetaExtensionValue("Map Name", "ct_ALLDTYPES_SRC");
            //source.setMetaExtensionValue("Original Name" , "ALLDTYPES_SRC");
            source.setMetaExtensionValue("Original Schema", "PWXUDB");
            Vector connInfos = source.getConnInfos();
            for (int i=0;i<connInfos.size();i++)
            {
                ConnectionInfo connInfo = (ConnectionInfo) connInfos.elementAt(i);
                connInfo.getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, "myTestConnection");
                connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME,"myDBName");
            }
        } catch (SourceTargetDefinitionNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MapFwkException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        folder.addSource(source);
        this.mapFileName = "PowerExchangeSource.xml";
    }

    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets()
    {
        //outputTarget = this.createFlatFileTarget("SAP_Output");
        PowerConnectTargetFactory targetFactory = PowerConnectTargetFactory.getInstance();
        try
        {
            outputTarget = targetFactory.getPowerConnectTargetInstance("PWX_SEQ_NRDB2", "myTargetDBD",  "myTargetDBD",  "myTargetDBD",  "myTargetDBD");
            TargetGroup trgGrp = new TargetGroup("stqa",null);
            outputTarget.createField("CCK_ROOT_LAST",trgGrp,"","","NUM32","10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);
            outputTarget.createField("LAST",trgGrp,"","","DOUBLE","15","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);

            TargetGroup trgGrp1 = new TargetGroup("stqa1",null);
            outputTarget.createField("CCK_ROOT_LAST1",trgGrp1,"","","NUM32","10","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);
            outputTarget.createField("LAST1",trgGrp1,"","","DOUBLE","15","0",FieldConstants.NOT_A_KEY, Field.FIELDTYPE_TARGET, false);
            
            outputTarget.setMetaExtensionValue("Access Method", "S");
            //source.setMetaExtensionValue("Map Name", "ct_ALLDTYPES_SRC");
            //source.setMetaExtensionValue("Original Name" , "ALLDTYPES_SRC");
            outputTarget.setMetaExtensionValue("Original Schema", "PWXUDB");
            Vector connInfos = outputTarget.getConnInfos();
            for (int i=0;i<connInfos.size();i++)
            {
                ConnectionInfo connInfo = (ConnectionInfo) connInfos.elementAt(i);
                connInfo.getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, "myTestConnection");
                connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME,"myDBName");
            }
            
        } catch (SourceTargetDefinitionNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MapFwkException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        folder.addTarget(outputTarget);
        
    }

    /* (non-Javadoc)
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception
    {
        workflow = new Workflow("Workflow_for_PowerExcange",
                "Workflow_for_PowerExcange", "This workflow for PowerExcange");
        workflow.addSession(session);
        folder.addWorkFlow(workflow);
    }
    
    public static void main(String args[]) {
        try {
            PowerExchangeTargetSample sample = new PowerExchangeTargetSample();
            if (args.length > 0) {
                if (sample.validateRunMode(args[0])) {
                    PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
                    compFactory.setCompatibilityVersion(8, 5, 0);
                    sample.execute();
                }
            } else {
                sample.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception is: " + e.getMessage());
        }
    }
    
}
