package com.qubole.yoda.tenali;


import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Created by devjyotip on 6/5/17.
 */
public final class TenaliASTAnalyzerFactory {

    static final private Logger LOG = LoggerFactory.getLogger(TenaliASTAnalyzerFactory.class);


    public static TenaliHiveASTAnalyzer get(ASTNode tree) throws SemanticException {
        TenaliHiveASTAnalyzer sem = getInternal(tree);
        return sem;
    }

    private static TenaliHiveASTAnalyzer getInternal(ASTNode tree) {
        if (tree.getToken() == null) {
            throw new RuntimeException("Empty Syntax Tree");
        } else {
            switch (tree.getType()) {
                case HiveParser.TOK_ALTERTABLE: {
                    ASTNode child = (ASTNode) tree.getChild(1);

                    switch (child.getType()) {
                        case HiveParser.TOK_ALTERTABLE_RENAME:
                        case HiveParser.TOK_ALTERTABLE_TOUCH:
                        case HiveParser.TOK_ALTERTABLE_ARCHIVE:
                        case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
                        case HiveParser.TOK_ALTERTABLE_ADDCOLS:
                        case HiveParser.TOK_ALTERTABLE_RENAMECOL:
                        case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
                        case HiveParser.TOK_ALTERTABLE_DROPPARTS:
                        case HiveParser.TOK_ALTERTABLE_ADDPARTS:
                        case HiveParser.TOK_ALTERTABLE_PARTCOLTYPE:
                        case HiveParser.TOK_ALTERTABLE_PROPERTIES:
                        case HiveParser.TOK_ALTERTABLE_DROPPROPERTIES:
                        case HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION:
                        case HiveParser.TOK_ALTERTABLE_SKEWED:
                            return null;
                        //return new DDLSemanticAnalyzer(queryState);
                    }
                }
            }
        }
        return null;
    }
}
