package com.qubole.yoda.tenali;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Map;

/**
 * Created by devjyotip on 6/28/17.
 */
public class QuickTests {


    /**
     * Goes though the tabref tree and finds the alias for the table. Once found,
     * it records the table name-> alias association in aliasToTabs. It also makes
     * an association from the alias to the table AST in parse info.
     *
     * @return the alias of the table
     */

 /*   private String processTable(QB qb, ASTNode tabref) throws SemanticException {
        // For each table reference get the table name
        // and the alias (if alias is not present, the table name
        // is used as an alias)
        int[] indexes = findTabRefIdxs(tabref);
        int aliasIndex = indexes[0];
        int propsIndex = indexes[1];
        int tsampleIndex = indexes[2];
        int ssampleIndex = indexes[3];

        ASTNode tableTree = (ASTNode) (tabref.getChild(0));

        String tabIdName = getUnescapedName(tableTree).toLowerCase();

        String alias = findSimpleTableName(tabref, aliasIndex);

        if (propsIndex >= 0) {
            Tree propsAST = tabref.getChild(propsIndex);
            Map<String, String> props = DDLSemanticAnalyzer.getProps((ASTNode) propsAST.getChild(0));
            // We get the information from Calcite.
            if ("TRUE".equals(props.get("insideView"))) {
                qb.getAliasInsideView().add(alias.toLowerCase());
            }
            qb.setTabProps(alias, props);
        }

        // If the alias is already there then we have a conflict
        if (qb.exists(alias)) {
            throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref
                    .getChild(aliasIndex)));
        }
        if (tsampleIndex >= 0) {
            ASTNode sampleClause = (ASTNode) tabref.getChild(tsampleIndex);
            ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
            if (sampleClause.getChildCount() > 2) {
                for (int i = 2; i < sampleClause.getChildCount(); i++) {
                    sampleCols.add((ASTNode) sampleClause.getChild(i));
                }
            }
            // TODO: For now only support sampling on up to two columns
            // Need to change it to list of columns
            if (sampleCols.size() > 2) {
                throw new SemanticException(generateErrorMessage(
                        (ASTNode) tabref.getChild(0),
                        ErrorMsg.SAMPLE_RESTRICTION.getMsg()));
            }
            TableSample tabSample = new TableSample(
                    unescapeIdentifier(sampleClause.getChild(0).getText()),
                    unescapeIdentifier(sampleClause.getChild(1).getText()),
                    sampleCols);
            qb.getParseInfo().setTabSample(alias, tabSample);
            if (unparseTranslator.isEnabled()) {
                for (ASTNode sampleCol : sampleCols) {
                    unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
                            .getChild(0));
                }
            }
        } else if (ssampleIndex >= 0) {
            ASTNode sampleClause = (ASTNode) tabref.getChild(ssampleIndex);

            Tree type = sampleClause.getChild(0);
            Tree numerator = sampleClause.getChild(1);
            String value = unescapeIdentifier(numerator.getText());


            SplitSample sample;
            if (type.getType() == HiveParser.TOK_PERCENT) {
                assertCombineInputFormat(numerator, "Percentage");
                Double percent = Double.valueOf(value).doubleValue();
                if (percent < 0  || percent > 100) {
                    throw new SemanticException(generateErrorMessage((ASTNode) numerator,
                            "Sampling percentage should be between 0 and 100"));
                }
                int seedNum = conf.getIntVar(HiveConf.ConfVars.HIVESAMPLERANDOMNUM);
                sample = new SplitSample(percent, seedNum);
            } else if (type.getType() == HiveParser.TOK_ROWCOUNT) {
                sample = new SplitSample(Integer.parseInt(value));
            } else {
                assert type.getType() == HiveParser.TOK_LENGTH;
                assertCombineInputFormat(numerator, "Total Length");
                long length = Integer.parseInt(value.substring(0, value.length() - 1));
                char last = value.charAt(value.length() - 1);
                if (last == 'k' || last == 'K') {
                    length <<= 10;
                } else if (last == 'm' || last == 'M') {
                    length <<= 20;
                } else if (last == 'g' || last == 'G') {
                    length <<= 30;
                }
                int seedNum = conf.getIntVar(HiveConf.ConfVars.HIVESAMPLERANDOMNUM);
                sample = new SplitSample(length, seedNum);
            }
            String alias_id = getAliasId(alias, qb);
            nameToSplitSample.put(alias_id, sample);
        }
        // Insert this map into the stats
        qb.setTabAlias(alias, tabIdName);
        if (qb.isInsideView()) {
            qb.getAliasInsideView().add(alias.toLowerCase());
        }
        qb.addAlias(alias);

        qb.getParseInfo().setSrcForAlias(alias, tableTree);

        // if alias to CTE contains the table name, we do not do the translation because
        // cte is actually a subquery.
        if (!this.aliasToCTEs.containsKey(tabIdName)) {
            unparseTranslator.addTableNameTranslation(tableTree, SessionState.get().getCurrentDatabase());
            if (aliasIndex != 0) {
                unparseTranslator.addIdentifierTranslation((ASTNode) tabref.getChild(aliasIndex));
            }
        }

        return alias;
    }*/




}
