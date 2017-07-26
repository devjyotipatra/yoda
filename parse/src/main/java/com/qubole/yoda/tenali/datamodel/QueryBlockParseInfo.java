package com.qubole.yoda.tenali.datamodel;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * QueryProperties.
 *
 * A structure to contain features of a query that are determined
 * during parsing and may be useful for categorizing a query type
 *
 * These include whether the query contains:
 * a join clause, a group by clause, an order by clause, a sort by
 * clause, a group by clause following a join clause, and whether
 * the query uses a script for mapping/reducing
 */
public class QueryBlockParseInfo {

    boolean hasFunctionDistinct;

    private final Map<String, ASTNode> destToSelExpr;

    private ASTNode whereExprNode;

    private final ArrayList<WindowSpec> windowExpressions;

    private final Map<ASTNode, String> exprToColumnAlias;

    private ArrayList<String> insertTables;

    private ASTNode joinRelnNode;

    //private ASTNode queryFromExpr;

    private ASTNode tableTree;


    public QueryBlockParseInfo(String alias, boolean isSubQ) {
        destToSelExpr = new HashMap<String, ASTNode>();
        windowExpressions = new ArrayList<WindowSpec>();
        exprToColumnAlias = new HashMap<ASTNode, String>();
    }

    public void setHasFunctionDistinct(boolean value) {
        hasFunctionDistinct = value;
    }

    public boolean hasFunctionDistinct() {
        return hasFunctionDistinct;
    }

    public ASTNode getWhereExprNode() {
        return whereExprNode;
    }

    public void setWhereExprNode(ASTNode node) {
        whereExprNode = node;
    }

    public void setSelExprForClause(String clause, ASTNode ast) {
        destToSelExpr.put(clause, ast);
    }

    public boolean existsWindowSpec(WindowSpec w) { return windowExpressions.indexOf(w) >= 0;}

    public void addWindowSpec(WindowSpec w) {
        windowExpressions.add(w);
    }


    public boolean hasExprToColumnAlias(ASTNode expr) {
        return exprToColumnAlias.containsKey(expr);
    }

    public void setExprToColumnAlias(ASTNode expr, String alias) {
        exprToColumnAlias.put(expr,  alias);
    }

    public void setJoinRelnNode(ASTNode joinNode) { joinRelnNode = joinNode; }

    public ASTNode getJoinRelnNode() { return joinRelnNode; }

    public void setInsertTables(String dbName, String tabName) {
        if(insertTables == null) {
            insertTables = new ArrayList<String>();
        }
        insertTables.add(dbName + "." + tabName);
    }

    public List<String> getInsertTables() {
        return insertTables;
    }

    /*public ASTNode getQueryFromExpr() {
        return queryFromExpr;
    }

    public void setQueryFromExpr(ASTNode queryFromExpr) {
        this.queryFromExpr = queryFromExpr;
    }*/

    public ASTNode getTableTree() {
        return tableTree;
    }

    public void setTableTree(ASTNode tableTree) {
        this.tableTree = tableTree;
    }

    /*
     * Captures how an Input should be Partitioned and Ordered. This is captured as the
     * ASTNode that is the parent of all the expressions in the Partition/Distribute/Cluster
     * by clause specifying the partitioning applied for a PTF invocation.
    */
    public static class WindowSpec {

        String sourceId;

        ASTNode distributeByExpr;

        ASTNode orderByExpr;

        boolean hasFunctionStar;

        boolean hasFunctionDistinct;


        public void processWindowSpec(ASTNode windowSpecNode) {
            int srcIdIdx = -1, partIdx = -1, wfIdx = -1;

            for(int i=0; i < windowSpecNode.getChildCount(); i++) {
                int type = windowSpecNode.getChild(i).getType();
                switch(type) {
                    case HiveParser.Identifier:
                        srcIdIdx = i;
                        break;
                    case HiveParser.TOK_PARTITIONINGSPEC:
                        partIdx = i;
                        break;
                    case HiveParser.TOK_WINDOWRANGE:
                    case HiveParser.TOK_WINDOWVALUES:
                        wfIdx = i;
                        break;
                }
            }

            if(srcIdIdx > 0) {
                sourceId = ((ASTNode) windowSpecNode.getChild(srcIdIdx)).getText();
            }

            if(partIdx > 0) {
                ASTNode partNode = (ASTNode) windowSpecNode.getChild(partIdx);
                assert (partNode.getChildCount() != 0);

                ASTNode firstChild = (ASTNode) partNode.getChild(0);
                if (firstChild.getType() == HiveParser.TOK_DISTRIBUTEBY || firstChild.getType() == HiveParser.TOK_CLUSTERBY) {
                    //We can have sort condition inside the window. This will be extracted by the node visitor.
                    distributeByExpr = firstChild;
                } else if (firstChild.getType() == HiveParser.TOK_SORTBY || firstChild.getType() == HiveParser.TOK_ORDERBY) {
                    orderByExpr = firstChild;
                }
            }
        }


        public ASTNode getDistributeByExpression() {
            return distributeByExpr;
        }

        public void setDistributeByExpression(ASTNode expr) {
            distributeByExpr = expr;
        }

        public ASTNode getOrderByExpression() {
            return orderByExpr;
        }

        public void setOrderByExpression(ASTNode expr) {
            orderByExpr = expr;
        }

        public boolean hasNumFunctionStar() { return hasFunctionStar; }

        public boolean hasNumFunctionDistinct() { return hasFunctionDistinct; }

        public void setHasFunctionStar(boolean value) { hasFunctionStar = value; }

        public void setHasFunctionDistinct(boolean value) { hasFunctionDistinct = value; }

        public boolean equals(WindowSpec w) {
            return this.sourceId.equals(w.sourceId);
        }

    }
}
