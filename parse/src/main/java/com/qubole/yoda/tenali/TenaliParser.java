package com.qubole.yoda.tenali;

import com.qubole.yoda.tenali.datamodel.QueryBlock;
import com.qubole.yoda.tenali.utility.exception.TenaliSemanticException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.*;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public class TenaliParser {
    protected static final Logger LOG = LoggerFactory.getLogger(TenaliParser.class.getName());

    private ParseDriver parseDriver = new ParseDriver();


    public TenaliParser() {  }



    public ASTNode parse(String query, String source) {
        ASTNode tree = null;
        try {
            HiveConf conf = new HiveConf();
            conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS, false);
            String scratchDir = HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR);
            conf.set("_hive.hdfs.session.path", scratchDir);
            conf.set("_hive.local.session.path", HiveConf.getVar(conf, HiveConf.ConfVars.LOCALSCRATCHDIR)
                    + "/" + System.getProperty("user.name") + "/" + "000");
            tree = parseDriver.parse(query, new Context(conf));
        } catch(Exception ex) {
            //
        }
        return findRootNonNullToken(tree);
    }


    /**
     * Performs a descent of the leftmost branch of the tree, stopping when either a
     * node with a non-null token is found or the leaf level is encountered.
     *
     * @param tree
     *          candidate node from which to start searching
     *
     * @return node at which descent stopped
     */

    private static ASTNode findRootNonNullToken(ASTNode tree) {
        while ((tree != null) && (tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        return tree;
    }


    private static void printPath(List<ASTNode> path) {
        StringBuilder sb = new StringBuilder();
        for(ASTNode node : path) {
            sb.append(node.getText()).append(" , ");
        }

        System.out.println("=> "+ sb.toString());
    }



    private static void traverseTree(ASTNode root) {
        Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
        Deque<Integer> numChildrenNotVisited = new ArrayDeque<Integer>();
        List<ASTNode> list = new ArrayList<ASTNode>();

        stack.push(root);

        while(!stack.isEmpty()) {
            ASTNode next = stack.pop();
            list.add(next);

            int numChildren = next.getChildCount();
            if (numChildren > 0) {
                numChildrenNotVisited.push(numChildren);

                for (int i = numChildren - 1; i >= 0 ; i--) {
                    stack.push((ASTNode)next.getChild(i));
                }
            } else {
                printPath(list);

                numChildren = 0;
                while (numChildren <= 1) {
                    list.remove(list.size()-1);
                    numChildren = numChildrenNotVisited.pop();
                }

                numChildrenNotVisited.push(numChildren-1);
            }
        }
    }



    public static void main(String[] args) {
        TenaliParser parser = new TenaliParser();
        String query0 = "SELECT  mc.guid AS na_id, mc.household_id AS hhid,ic.group_id AS group_id, " +
                "Nvl(ic.individual_id, mc.individual_id) AS individual_id FROM core_digital.matched_cookies mc " +
                "LEFT JOIN core_shared.individual_consolidated ic ON ic.individual_id = mc.individual_id WHERE  " +
                "mc.last_seen_date > Date_sub(To_date(From_unixtime(Unix_timestamp())), 91)";

        //parser.parse("select a.id, b.name, count(*) from ( select * from tab1 a join tab2 b on a.xx=b.yy " +
        //        "where a.id>350) ss group by a.id, b.name");

        String query1 = "insert overwrite table galaxy  partition (source = 'hive', submit_time = '2017-07-15', account_id)" +
                "    select sub.query, astTable.q_ast, sub.query_hists_id," +
                "    sub.account_id from (select h.query as query, q.id as" +
                "    query_hists_id, q.account_id as account_id, q.submit_time as submit_time" +
                "    from rstore.query_hists as q join rstore.hive_commands as h on q.command_id = h.id" +
                "    where q.status = 'done' and q.command_type = 'HiveCommand' and" +
                "    to_date(FROM_UNIXTIME(q.submit_time)) >= '2017-07-15' and" +
                "    to_date(FROM_UNIXTIME(q.submit_time)) < '2017-07-16') as" +
                "    sub lateral view explode(astUDF(translate(sub.query, '\\006', '\\n'))) astTable as q_ast";

        String query2 = "INSERT INTO TABLE `tenaliv2`.`galaxy` PARTITION (source = 'hive', submit_time = '2017-07-15', account_id) " +
                "select query, account_id, some_id FROM query_hists";

        String query3 = "select a, b, c, sum_custom(d) OVER (PARTITION BY e ORDER BY f ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) as g FROM query_hists";
        /*
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , sum_custom ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_TABLE_OR_COL , d ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_WINDOWSPEC , TOK_PARTITIONINGSPEC , TOK_DISTRIBUTEBY , TOK_TABLE_OR_COL , e ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_WINDOWSPEC , TOK_PARTITIONINGSPEC , TOK_ORDERBY , TOK_TABSORTCOLNAMEASC , TOK_TABLE_OR_COL , f ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_WINDOWSPEC , TOK_WINDOWRANGE , PRECEDING , 3 ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_WINDOWSPEC , TOK_WINDOWRANGE , FOLLOWING , 3 ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , g ,
         */

        String query4 = "SELECT count(1), sum(salary) OVER w, avg(distinct salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY some_exp(salary) DESC)";
        /*
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , avg ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_TABLE_OR_COL , salary ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_WINDOWSPEC , w ,
        => TOK_QUERY , TOK_INSERT , WINDOW , TOK_WINDOWDEF , w ,
        => TOK_QUERY , TOK_INSERT , WINDOW , TOK_WINDOWDEF , TOK_WINDOWSPEC , TOK_PARTITIONINGSPEC , TOK_DISTRIBUTEBY , TOK_TABLE_OR_COL , depname ,
        => TOK_QUERY , TOK_INSERT , WINDOW , TOK_WINDOWDEF , TOK_WINDOWSPEC , TOK_PARTITIONINGSPEC , TOK_ORDERBY , TOK_TABSORTCOLNAMEDESC , TOK_TABLE_OR_COL , salary ,
         */

        String query5 = "select * from (select id from test where id>10) a join (select id from test where id>20) b on a.id=b.id";
        /*
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_TABREF , TOK_TABNAME , test ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_TABLE_OR_COL , id ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , > , TOK_TABLE_OR_COL , id ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , > , 10 ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , a ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_TABREF , TOK_TABNAME , test ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_TABLE_OR_COL , id ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , > , TOK_TABLE_OR_COL , id ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , > , 20 ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , TOK_SUBQUERY , b ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , = , . , TOK_TABLE_OR_COL , a ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , = , . , id ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , = , . , TOK_TABLE_OR_COL , b ,
        => TOK_QUERY , TOK_FROM , TOK_JOIN , = , . , id ,
        => TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_ALLCOLREF ,
         */

        //"SELECT x FROM t1 WHERE x > (SELECT MAX(y) FROM t2)"  -- not supported
        String query6 = "SELECT x FROM t1 WHERE x in (SELECT MAX(y) FROM t2)";
        /*
        => TOK_QUERY , TOK_FROM , TOK_TABREF , TOK_TABNAME , t1 ,
        => TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_TABLE_OR_COL , x ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_SUBQUERY_OP , in ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_QUERY , TOK_FROM , TOK_TABREF , TOK_TABNAME , t2 ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , MAX ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_FUNCTION , TOK_TABLE_OR_COL , y ,
        => TOK_QUERY , TOK_INSERT , TOK_WHERE , TOK_SUBQUERY_EXPR , TOK_TABLE_OR_COL , x ,
         */

        String query7 = "SELECT A FROM T1 WHERE EXISTS (SELECT B FROM T2 WHERE T1.X = T2.Y)";


        String query8 = "SELECT S.B FROM (SELECT T.C AS B FROM ( SELECT C FROM T2 WHERE X = 10) T) S";
        /*
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_TABREF , TOK_TABNAME , T2 ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , TOK_TABLE_OR_COL , C ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , = , TOK_TABLE_OR_COL , X ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_WHERE , = , 10 ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_FROM , TOK_SUBQUERY , T ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , . , TOK_TABLE_OR_COL , T ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , . , C ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , B ,
        => TOK_QUERY , TOK_FROM , TOK_SUBQUERY , S ,
        => TOK_QUERY , TOK_INSERT , TOK_DESTINATION , TOK_DIR , TOK_TMP_FILE ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , . , TOK_TABLE_OR_COL , S ,
        => TOK_QUERY , TOK_INSERT , TOK_SELECT , TOK_SELEXPR , . , B ,
         */

        String query9 = "";
        /*
        // SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
            // is not supported. Instead, the lateral view must be in a subquery
            // SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
            // JOIN src2 ...
         */

        String query10 = "select * from (select *, udf_A(some_column) over (partition by aa) as trans_A from A) as AA JOIN (select *, udf_B(some_column) as trans_B from B) as BB on AA.trans_A = BB.trans_B";

        ASTNode root = parser.parse(query10, "hive");

        traverseTree(root);

        TenaliHiveASTAnalyzer analyzer = new TenaliHiveASTAnalyzer();
        try {
            analyzer.parseAST(root);
        } catch(TenaliSemanticException ex) {

        }

        Deque<QueryBlock> queue = new ArrayDeque<QueryBlock>();

        queue.push(analyzer.getHeadQueryBlock());
       while(!queue.isEmpty()) {
            QueryBlock q = queue.removeFirst();

            System.out.println(((q.getParentQueryBlock()==null) ? "-1" : q.getParentQueryBlock().getId()) + "   =##=      " + q.getId());
            List<QueryBlock>  children = q.getChildren();
            List<QueryBlock> siblings = q.getSiblings();

            for(QueryBlock qq : children) {
                queue.addFirst(qq);
            }

            for(QueryBlock qq : siblings) {
                queue.addFirst(qq);
            }
        }
    }
}