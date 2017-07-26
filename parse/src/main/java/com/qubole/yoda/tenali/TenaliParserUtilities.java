package com.qubole.yoda.tenali;

import com.qubole.yoda.tenali.utility.exception.TenaliSemanticException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Created by devjyotip on 7/21/17.
 */
public class TenaliParserUtilities {

    /**
     * Get the unqualified name from a table node.
     *
     * This method works for table names qualified with their schema (e.g., "db.table")
     * and table names without schema qualification. In both cases, it returns
     * the table name without the schema.
     */

    public static String[] getQualifiedTableName(String defaultDBName, ASTNode tabNameNode) throws SemanticException {
        if (tabNameNode.getType() != HiveParser.TOK_TABNAME ||
                (tabNameNode.getChildCount() != 1 && tabNameNode.getChildCount() != 2)) {
            throw new TenaliSemanticException(ErrorMsg.INVALID_TABLE_NAME.getMsg(tabNameNode));
        }

        if (tabNameNode.getChildCount() == 2) {
            String dbName = unescapeIdentifier(tabNameNode.getChild(0).getText());
            String tableName = unescapeIdentifier(tabNameNode.getChild(1).getText());
            return new String[] {dbName, tableName};
        }

        String tableName = unescapeIdentifier(tabNameNode.getChild(0).getText());
        return new String[] {defaultDBName, tableName};
    }

    /**
     * Remove the encapsulating "`" pair from the identifier. We allow users to
     * use "`" to escape identifier for table names, column names and aliases, in
     * case that coincide with Hive language keywords.
     */
    public static String unescapeIdentifier(String val) {
        if (val == null) {
            return null;
        }
        if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

}
