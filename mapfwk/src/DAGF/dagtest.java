/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package DAGF;

/**
 *
 * @author Administrator
 */
public class dagtest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here

        final DAGF dag = new DAGF();
        dag.addEdge("a", "b");
        dag.addEdge("b", "c");
        dag.addEdge("b", "d");
        dag.addEdge("b", "e");
        dag.addEdge("c", "f");
        dag.addEdge("d", "g");
        dag.addEdge("e", "h");
        //dag.addEdge("c", "g");

        for(Object o : dag.getSuccessorLabels("a")){
            System.out.print(o+"<-");
        }

    }

}
