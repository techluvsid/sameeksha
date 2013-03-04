package latest;
import DAGF.CycleDetectedException;
import DAGF.DAGF;
import DAGF.Vertex;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import util.Connector;

/**
 *
 * @author Administrator
 */
public class MapHelper {

    private final Mapping mapping;
    private List<Connector> conect;
    private Map<String, Field> inputfield = new HashMap<String, Field>();
    private Map<String, Field> allfield = new HashMap<String, Field>();
    private List<Transformation> expression = new ArrayList<Transformation>();
    private DAGF dag = new DAGF();

    public MapHelper(Mapping mapping) throws CycleDetectedException {
        this.mapping = mapping;
        conect = new ArrayList<Connector>();
        init();
        makeLinks();
        initGraph();
    }

    private void init() {
        for (Transformation tr : mapping.getTransformations()) {
            for (InputSet is : tr.getTransContext().getInputSets()) {
                for (PortDef pd : is.getPortDefs()) {
                    //System.out.println(pd.getFromInstanceName() + " [" + pd.getInputField().getName() + "] "+pd.getInputField().getDataType()+" - " 
                    //+ pd.getToInstanceName() + " [" + pd.getTransformField().getName() + "] "+pd.getTransformField().getDataType());
                    conect.add(new Connector(pd.getInputField(), pd.getOutputField(), pd.getFromInstanceName(),
                            pd.getToInstanceName(), pd.getFromInstanceType(), pd.getToInstanceType()));
                }
                
                for (Object ofs : is.getOutputFields()) {
                    TransformField of = (TransformField) ofs;
                    allfield.put(tr.getInstanceName() + "." + of.getField().getName(), of);
                    if (of.getPortType() == 3 || of.getPortType() == 1 || of.getPortType() == 10) {
                        inputfield.put(tr.getInstanceName() + "." + of.getField().getName(), of);
                        //System.out.println(tr.getInstanceName()+" ["+of.getField().getName()+"] --"+of.getPortType()+"--"+of.getField().getFieldType());
                    }
                    if (tr.getTransformationType() == TransformationConstants.ROUTER && of.getPortType()==TransformField.TYPE_OUTPUT) {
                        TransformField offrom = inputfield.get(tr.getInstanceName()+"."+of.getRefFieldName());
                        conect.add(new Connector(offrom.getField(), of.getField(), tr.getInstanceName(), tr.getTransformationType()));
                        //System.out.println(of.getGroupName()+"#"+offrom.getField().getName());
                    }
                    if (tr.getTransformationType() == TransformationConstants.UNION && of.getPortType()==TransformField.TYPE_INPUT) {
                        
                        System.out.println(of.getGroupName()+"#"+of.getRefFieldName());
                    }
                }

            }

            if (tr.getTransformationType() == 3 || tr.getTransformationType() == 1) {
                expression.add(tr);
            }

        }
        

        for (Object ot : mapping.getTargets()) {
            Target tgt = (Target) ot;
            for (Object opd : tgt.getPortDefs()) {
                PortDef pd = (PortDef) opd;
                conect.add(new Connector(pd.getInputField(), pd.getTransformField(), pd.getFromInstanceName(),
                        pd.getToInstanceName(), pd.getFromInstanceType(), pd.getToInstanceType()));
                //System.out.println(pd.getFromInstanceName() + " [" + pd.getInputField().getName() + "] "+pd.getInputField().getDataType()+" - " 
                // + pd.getToInstanceName() + " [" + pd.getTransformField().getName() + "] "+pd.getTransformField().getDataType());

            }
        }

    }

    private void makeLinks() {
        for (Transformation trf : expression) {
            for (Object ofs : trf.getOutFields()) {
                TransformField of = (TransformField) ofs;
                List<String> colmns = ExpParse.getColumns(" " + of.getExpr() + " ");
                for (String col : colmns) {
                    if (!col.contains(".")) {
                        col = trf.getInstanceName() + "." + col;
                    }
                    if (inputfield.containsKey(col)) {
                        Field offrom = inputfield.get(col);
                        conect.add(new Connector(offrom.getField()s, of.getField(), trf.getInstanceName(), trf.getTransformationType()));
                        //System.out.println(trf.getInstanceName()+"(["+offrom.getField().getName()+"]--->["+of.getField().getName()+"]");
                    }
                }
            }
        }
    }

    private void initGraph() throws CycleDetectedException {
        for (Connector conct : conect) {
            Vertex from = new Vertex(conct.getFrom(), conct.getFROMINSTANCE(), conct.getFROMINSTANCETYPE());
            Vertex to = new Vertex(conct.getTo(), conct.getTOINSTANCE(), conct.getTOINSTANCETYPE());
            //System.out.println(from + "--->" + to);
            dag.addEdge(from, to);
        }
    }

    public void getChild(String name) {
        Vertex vt = dag.getVertex(name);
        LinkedList<Vertex> result = new LinkedList<Vertex>();
        List<LinkedList<Vertex>> paths = new ArrayList<LinkedList<Vertex>>();
        getparents(paths, result, vt, vt, false);
        for (LinkedList<Vertex> path : paths) {
            String pth = "";
            for (Vertex v : path) {
                pth += v.getLabel() + "-->";
            }
            System.out.println(pth);
        }
    }

    private void getChildren(List<LinkedList<Vertex>> paths, LinkedList<Vertex> result, Vertex v, Vertex vt, boolean branch) {
        //System.out.println(v.getFName());
        if (v.isLeaf()) {
            result.add(v);
            paths.add((LinkedList<Vertex>) result.clone());
            if (branch) {
                result.removeLast();
            }
            return;
        }
        List chd = v.getChildren();
        result.add(v);

        if (!chd.isEmpty()) {
            branch = true;
        }
        for (Object o : chd) {

            getChildren(paths, result, (Vertex) o, vt, branch);
        }
        if (branch) {
            result.removeLast();
        }
    }
    
    private void getparents(List<LinkedList<Vertex>> paths, LinkedList<Vertex> result, Vertex v, Vertex vt, boolean branch) {
        //System.out.println(v.getFName());
        if (v.isRoot()) {
            result.add(v);
            paths.add((LinkedList<Vertex>) result.clone());
            if (branch) {
                result.removeLast();
            }
            return;
        }
        List chd = v.getParents();
        result.add(v);

        if (!chd.isEmpty()) {
            branch = true;
        }
        for (Object o : chd) {

            getparents(paths, result, (Vertex) o, vt, branch);
        }
        if (branch) {
            result.removeLast();
        }
    }
    
    private boolean checkPath(Vertex vs, Vertex vt, LinkedList<Vertex> path) {
        Iterator<Vertex> itr = path.iterator();
        boolean add = false;
        if (vs == null) {
            while (itr.hasNext()) {
                Field tr = itr.next().getBox();

                if (Integer.valueOf(tr.getPrecision()) > Integer.valueOf(vt.getBox().getPrecision()) || 
                        Integer.valueOf(tr.getScale()) > Integer.valueOf(vt.getBox().getScale())) {
                    return true;
                }

            }
        } else {
            while (itr.hasNext()) {
                Field tr = itr.next().getBox();

                if ((Integer.valueOf(tr.getPrecision()) > Integer.valueOf(vt.getBox().getPrecision()) || Integer.valueOf(tr.getScale()) > Integer.valueOf(vt.getBox().getScale())) || 
                        (Integer.valueOf(tr.getPrecision()) < Integer.valueOf(vs.getBox().getPrecision()) || Integer.valueOf(tr.getScale()) < Integer.valueOf(vs.getBox().getScale()))) {
                    add = true;
                }
            }
        }
        return add;
    }
    public String getName() {
        return mapping.getName();
    }
}
