package com.qubole.yoda.tenali;


import com.qubole.yoda.tenali.datamodel.QueryBlock;
import com.qubole.yoda.tenali.datamodel.QueryBlockParseInfo;
import com.qubole.yoda.tenali.utility.exception.TenaliSemanticException;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by devjyotip on 6/1/17.
 */

public class TenaliHiveASTAnalyzer {

    protected static final Logger LOG = LoggerFactory.getLogger(TenaliParser.class.getName());

    public static final String SUBQUERY_TAG_1 = "-subquery1";
    public static final String SUBQUERY_TAG_2 = "-subquery2";

    private static int QBID = 0;

    private static QueryBlock QB;

    HashMap<String, Opcode> relMapQueryBlockSiblings = new HashMap<String, Opcode>();


    public static enum Opcode {
        NULLOP, JOIN, UNION, UNIONDISTINCT, SCALARSUBQUERY
    };



    /**
     * Returns the ID of the next QueryBlock
     */
    public static int getNextQBId() {
        QBID++;
        return QBID;
    }


    public QueryBlock getHeadQueryBlock() {
        return QB;
    }


    /**
     * DFS-scan the expressionTree to find all aggregation subtrees and put them
     * in aggregations.
     */
    private void doPhase1GetAllAggregations(ASTNode expressionTree,
                                            ArrayList<ASTNode> aggregations,
                                            List<ASTNode> wdwFns, ASTNode wndParent)  {
        int exprTokenType = expressionTree.getToken().getType();
        if(exprTokenType == HiveParser.TOK_SUBQUERY_EXPR) {
            //since now we have scalar subqueries we can get subquery expression in having
            // we don't want to include aggregate from within subquery
            return;
        }

        if (exprTokenType == HiveParser.TOK_FUNCTION
                || exprTokenType == HiveParser.TOK_FUNCTIONDI
                || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
            assert (expressionTree.getChildCount() != 0);

            if (expressionTree.getChild(expressionTree.getChildCount() - 1).getType()
                    == HiveParser.TOK_WINDOWSPEC) {
                // If it is a windowing spec, we include it in the list
                // Further, we will examine its children AST nodes to check whether
                // there are aggregation functions within
                wdwFns.add(expressionTree);
                for (Node child : expressionTree.getChildren()) {
                    doPhase1GetAllAggregations((ASTNode) child, aggregations, wdwFns, expressionTree);
                }
                return;
            }

            if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
                aggregations.add(expressionTree);
                return;
            }
        }

        for (int i=0; i<expressionTree.getChildCount(); i++) {
            ASTNode child = (ASTNode) expressionTree.getChild(i);
            doPhase1GetAllAggregations(child, aggregations, wdwFns, expressionTree);
        }
    }



    private List<ASTNode> doPhase1GetAggregationsFromSelect(ASTNode selExpr, QueryBlock qb)
            throws SemanticException {
        // Iterate over the selects search for aggregation Trees.
        // Use String as keys to eliminate duplicate trees.
        ArrayList<ASTNode> aggregationTrees = new ArrayList<ASTNode>();
        List<ASTNode> wdwFns = new ArrayList<ASTNode>();

        for (int i = 0; i < selExpr.getChildCount(); ++i) {
            ASTNode functionNode = (ASTNode) selExpr.getChild(i);
            if (functionNode.getType() == HiveParser.TOK_SELEXPR ||
                    functionNode.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
                functionNode = (ASTNode)functionNode.getChild(0);
            }

            doPhase1GetAllAggregations(functionNode, aggregationTrees, wdwFns, null);
        }

        // window based aggregations are handled here
        QueryBlockParseInfo qbp = qb.getQBParseInfo();
        if(wdwFns.size() > 0) {
            qb.setHasWindowing(true);
        }

        for (ASTNode wdwFn : wdwFns) {
            QueryBlockParseInfo.WindowSpec wfSpec = new QueryBlockParseInfo.WindowSpec();

            switch(wdwFn.getType()) {
                case HiveParser.TOK_FUNCTIONSTAR:
                    wfSpec.setHasFunctionStar(true);
                    break;
                case HiveParser.TOK_FUNCTIONDI:
                    wfSpec.setHasFunctionDistinct(true);
                    break;
            }

            wfSpec.processWindowSpec(wdwFn);
            // If this is a duplicate invocation of a function; don't add to QueryBlockParseInfo.
            if (!qbp.existsWindowSpec(wfSpec)) {
                qbp.addWindowSpec(wfSpec);
            }
        }

        return aggregationTrees;
    }


    private void doPhase1GetColumnAliasesFromSelect(ASTNode selectExpr, QueryBlockParseInfo qbp) throws SemanticException {
        for (int i = 0; i < selectExpr.getChildCount(); ++i) {
            ASTNode selExpr = (ASTNode) selectExpr.getChild(i);
            if ((selExpr.getToken().getType() == HiveParser.TOK_SELEXPR)
                    && (selExpr.getChildCount() == 2)) {
                String columnAlias = TenaliParserUtilities.unescapeIdentifier(selExpr.getChild(1).getText());
                qbp.setExprToColumnAlias((ASTNode) selExpr.getChild(0), columnAlias);
            }
        }
    }


    private void doPhase1GetDistinctFuncExprs(List<ASTNode> aggregationTrees, QueryBlockParseInfo qbp) throws SemanticException {
        int numDistinctFuncExprs = 0;
        for (ASTNode node : aggregationTrees) {
            assert (node != null);
            if (node.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
                numDistinctFuncExprs++;
            }
        }
        qbp.setHasFunctionDistinct(numDistinctFuncExprs > 0);
    }


    public void preProcessForInsert(ASTNode insertTree, QueryBlockParseInfo qbp) throws TenaliSemanticException {
        try {
            if (!(insertTree.getToken() != null && insertTree.getToken().getType() == HiveParser.TOK_QUERY)) {
                return;
            }

            //TOK_QUERY , TOK_INSERT , TOK_INSERT_INTO , TOK_TAB , TOK_TABNAME , galaxy
            //TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_TAB , TOK_TABNAME , galaxy
            for (Node child : insertTree.getChildren()) {
                if (((ASTNode) child).getToken().getType() != HiveParser.TOK_INSERT) {
                    continue;
                }

                ASTNode in = (ASTNode) ((ASTNode) child).getFirstChildWithType(HiveParser.TOK_INSERT_INTO);
                ASTNode io = (ASTNode) ((ASTNode) child).getFirstChildWithType(HiveParser.TOK_DESTINATION);

                if (in == null && io == null) continue;
                ASTNode n = (in != null) ? in : io;
                n = (ASTNode) n.getFirstChildWithType(HiveParser.TOK_TAB);
                if (n == null) continue;
                n = (ASTNode) n.getFirstChildWithType(HiveParser.TOK_TABNAME);
                if (n == null) continue;

                String[] dbTab = TenaliParserUtilities.getQualifiedTableName(QB.getDefaultDBName(), n);
                qbp.setInsertTables(dbTab[0], dbTab[1]);
            }
        } catch(Exception ex) {
            throw new TenaliSemanticException(ex);
        }
    }


    public void doPhase1QBReln(ASTNode ast, QueryBlock parentQB, String alias) throws TenaliSemanticException {
        System.out.println("Inside doPhase1QBReln");
        assert (ast.getToken() != null);

        if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
            QueryBlock qbexpr = new QueryBlock(getNextQBId(), parentQB, alias, true);
            parentQB.addChild(qbexpr);
            doPhase1(ast, qbexpr);
        } else {
            Opcode opcode = null;
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_UNIONALL:
                    opcode = Opcode.UNION;
                    break;
                case HiveParser.TOK_UNIONDISTINCT:
                    opcode = Opcode.UNIONDISTINCT;
                    break;
                case HiveParser.TOK_SUBQUERY_OP:
                case HiveParser.TOK_SUBQUERY_OP_NOTIN:
                case HiveParser.TOK_SUBQUERY_OP_NOTEXISTS:
                    opcode = Opcode.SCALARSUBQUERY;
                    break;
                default:
                    //throw new TenaliSemanticException(ErrorMsg.UNSUPPORTED_SET_OPERATOR.getMsg("Type "
                    //        + ast.getToken().getType()));
            }
            // query 1
            assert (ast.getChild(0) != null);
            int sqid1 = getNextQBId();
            QueryBlock parent = (opcode != Opcode.SCALARSUBQUERY) ? parentQB : parentQB.getParentQueryBlock();
            QueryBlock qbexpr1 = new QueryBlock(sqid1, parent, alias + SUBQUERY_TAG_1, true);
            parentQB.addSibling(qbexpr1);
            doPhase1(ast, qbexpr1);

            // query 2
            if(opcode != Opcode.SCALARSUBQUERY) {
                assert (ast.getChild(1) != null);
                int sqid2 = getNextQBId();
                QueryBlock qbexpr2 = new QueryBlock(sqid2, parentQB, alias + SUBQUERY_TAG_2, true);
                parentQB.addSibling(qbexpr2);
                doPhase1(ast, qbexpr2);

                relMapQueryBlockSiblings.put(String.valueOf(sqid1) + "." + String.valueOf(sqid2), opcode);
            }
        }

        System.out.println("RETIURNING FROM doPhase1QBReln");
    }


    private void processSubQuery(QueryBlock qb, ASTNode subq) throws TenaliSemanticException {

        String alias = TenaliParserUtilities.unescapeIdentifier(subq.getChild(1).getText());

        ASTNode subqref = (ASTNode) subq.getChild(0);

        System.out.println("processing subquery   =>   " + alias);

        // Recursively do the first phase of semantic analysis for the subquery
        doPhase1QBReln(subqref, qb, alias);
    }


    private List<ASTNode> findSubQueries(ASTNode node) {
        List<ASTNode> subQueries = new ArrayList<ASTNode>();
        Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
        stack.push(node);

        while (!stack.isEmpty()) {
            ASTNode next = stack.pop();

            switch(next.getType()) {
                case HiveParser.TOK_SUBQUERY_EXPR:
                    subQueries.add(next);
                    break;
                default:
                    int childCount = next.getChildCount();
                    for(int i = childCount - 1; i >= 0; i--) {
                        stack.push((ASTNode) next.getChild(i));
                    }
            }
        }

        return subQueries;
    }


    static boolean isJoinToken(ASTNode node) {
        if ((node.getToken().getType() == HiveParser.TOK_JOIN)
                || (node.getToken().getType() == HiveParser.TOK_CROSSJOIN)
                || isOuterJoinToken(node)
                || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
                || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN)) {
            return true;
        }

        return false;
    }

    static private boolean isOuterJoinToken(ASTNode node) {
        return (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
                || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
                || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN);
    }


    /**
     * Given the AST with TOK_JOIN as the root, get all the aliases for the tables
     * or subqueries in the join.
     */
    @SuppressWarnings("nls")
    private void processJoin(QueryBlock qb, ASTNode join) throws TenaliSemanticException {
        QueryBlockParseInfo qbp = qb.getQBParseInfo();
        int numChildren = join.getChildCount();

        //queryProperties.incrementJoinCount(isOuterJoinToken(join));
        for (int num = 0; num < numChildren; num++) {
            ASTNode child = (ASTNode) join.getChild(num);
            if (child.getToken().getType() == HiveParser.TOK_TABREF) {
                qbp.setTableTree(child);
            } else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY) {
                processSubQuery(qb, child);
            } else if (isJoinToken(child)) {
                processJoin(qb, child);
            }
        }
    }



    private boolean doPhase1(ASTNode root, QueryBlock qb) throws TenaliSemanticException {
        boolean phase1Result = true;
        QueryBlockParseInfo qbp = qb.getQBParseInfo();
        System.out.println( "  _____   " + root.getText());

        try {
            switch (root.getToken().getType()) {
                case HiveParser.TOK_SELECTDI:
                    qb.countSelDi();
                case HiveParser.TOK_SELECT:
                    qb.countSel();

                    if (root.getChild(0).getChild(0).getType() == HiveParser.TOK_TRANSFORM ||
                            (root.getChildCount() > 1 && root.getChild(1).getChild(0).getType() == HiveParser.TOK_TRANSFORM))
                        qb.setUsesScript(true);

                    List<ASTNode> aggregations = doPhase1GetAggregationsFromSelect(root, qb);
                    doPhase1GetDistinctFuncExprs(aggregations, qbp);
                    doPhase1GetColumnAliasesFromSelect(root, qbp);
                    break;
                case HiveParser.TOK_WHERE:
                    qb.setHasWhereClause(true);
                    qbp.setWhereExprNode(root);
                    List<ASTNode> subqueries = findSubQueries((ASTNode) root.getChild(0));

                    for(ASTNode sq : subqueries) {
                        processSubQuery(qb, sq);
                        phase1Result = false;
                    }
                    break;
                case HiveParser.TOK_INSERT_INTO:
                    preProcessForInsert(root, qbp);
                    if(qbp.getInsertTables().size() > 1) {
                        qb.setMultiInsertQuery(true);
                    }
                    break;
                case HiveParser.TOK_FROM:
                    ASTNode frmNode = (ASTNode) root.getChild(0);
                    if (frmNode.getToken().getType() == HiveParser.TOK_TABREF) {
                        qbp.setTableTree((ASTNode) frmNode.getChild(0));
                    } else if (frmNode.getToken().getType() == HiveParser.TOK_SUBQUERY) {
                        processSubQuery(qb, frmNode);
                        phase1Result = false;
                    } else if (isJoinToken(frmNode)) {
                        processJoin(qb, frmNode);
                        qbp.setJoinRelnNode(frmNode);
                    }

                    /*
                        ToDo: Handle lateral views, user defined functions and window functions
                    */
                    break;
            }

            int child_count = root.getChildCount();
            for (int child_pos = 0; child_pos < child_count && phase1Result; ++child_pos) {
                phase1Result = phase1Result && doPhase1((ASTNode) root.getChild(child_pos), qb);
            }
        } catch(SemanticException ex) {
            throw new TenaliSemanticException(ex);
        }

        return phase1Result;
    }


    public void parseAST(ASTNode root) throws TenaliSemanticException {
        QB = new QueryBlock(getNextQBId(), null, null, false);
        doPhase1(root, QB);
    }

}
