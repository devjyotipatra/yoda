package com.qubole.yoda.tenali.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Implementation of the query block.
 *
 **/

public class QueryBlock {

    private final int id;

    private final QueryBlock parent;

    private List<QueryBlock> children;

    private List<QueryBlock> siblings;

    private HashMap<String, String> aliasToTabs;

    // does the query have a using clause
    private boolean usesScript = false;

    private boolean hasWindowing;

    private boolean hasWhereClause;

    private String defaultDBName;

    private boolean isMultiInsertQuery;

    private int numSels = 0;

    private int numSelDis = 0;

    private boolean isSubQuery = false;

    private String subQueryAlias;


    private QueryBlockParseInfo qbp;


    public QueryBlock(int id) {this(id, null, null, false); }

    public QueryBlock(int id, QueryBlock parent, String alias, boolean isSubQ) {
        this.id = id;
        this.parent = parent;
        this.subQueryAlias = alias;
        this.isSubQuery = isSubQ;

        children = new ArrayList<QueryBlock>();
        siblings = new ArrayList<QueryBlock>();

        aliasToTabs = new LinkedHashMap<String, String>();

        qbp = new QueryBlockParseInfo(alias, isSubQ);
    }

    /*public boolean exists(String alias) {
        alias = alias.toLowerCase();
        if (aliasToTabs.get(alias) != null || aliasToSubq.get(alias) != null) {
            return true;
        }

        return false;
    }*/

    public int getId() {
        return id;
    }

    public QueryBlock getParentQueryBlock() {
        return parent;
    }

    public void addChild(QueryBlock qb) {
        children.add(qb);
    }

    public void addSibling(QueryBlock qb) {
        siblings.add(qb);
    }


    public List<QueryBlock> getChildren() {
        return children;
    }

    public List<QueryBlock> getSiblings() {
        return siblings;
    }


    public void setHasWindowing(boolean hasWindowing) {
        this.hasWindowing = hasWindowing;
    }

    public boolean getHasWindowing() {
        return hasWindowing;
    }

    public boolean usesScript() {
        return usesScript;
    }

    public void setUsesScript(boolean usesScript) {
        this.usesScript = usesScript;
    }

    public QueryBlockParseInfo getQBParseInfo() {
        return qbp;
    }

    public void setDefaultDBName(String dbName) {
        defaultDBName = dbName;
    }

    public String getDefaultDBName() {
        return defaultDBName;
    }

    public void countSel() {
        numSels++;
    }

    public int getCountSelect() {
        return numSels;
    }

    public void countSelDi() {
        numSelDis++;
    }

    public int getCountSelectDi() {
        return numSelDis;
    }

    public boolean hasWhereClause() {
        return hasWhereClause;
    }

    public void setHasWhereClause(boolean hasWhereClause) {
        this.hasWhereClause = hasWhereClause;
    }

    public boolean isMultiInsertQuery() {
        return isMultiInsertQuery;
    }

    public void setMultiInsertQuery(boolean multiInsertQuery) {
        isMultiInsertQuery = multiInsertQuery;
    }

    public boolean isSubQuery() {
        return isSubQuery;
    }

    public void setIsSubQuery(boolean subQuery) {
        isSubQuery = subQuery;
    }

    public String getSubQueryAlias() {
        return subQueryAlias;
    }

    public void setSubQueryAlias(String subQueryAlias) {
        this.subQueryAlias = subQueryAlias;
    }
}
