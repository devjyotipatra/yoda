package com.qubole.yoda.tenali.datamodel;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by devjyotip on 7/14/17.
 */
public class TenaliASTNode extends ASTNode {

    /**
     * For every node in this subtree, make sure it's start/stop token's
     * are set.  Walk depth first, visit bottom up.  Only updates nodes
     * with at least one token index < 0.
     *
     * In contrast to the method in the parent class, this method is
     * iterative.
     */


    public void setUnknownTokenBoundaries() {
        Deque<ASTNode> stack1 = new ArrayDeque<ASTNode>();
        Deque<ASTNode> stack2 = new ArrayDeque<ASTNode>();
        stack1.push(this);

        while (!stack1.isEmpty()) {
            ASTNode next = stack1.pop();
            stack2.push(next);

            if (next.getChildren() != null) {
                for (int i = next.getChildCount() - 1; i >= 0 ; i--) {
                    stack1.push((ASTNode)next.getChild(i));
                }
            }
        }

        while (!stack2.isEmpty()) {
            ASTNode next = stack2.pop();

            if (next.getChildren() == null) {
                if (next.getTokenStartIndex() < 0 || next.getTokenStopIndex() < 0) {
                    next.setTokenStartIndex(next.token.getTokenIndex());
                    next.setTokenStopIndex(next.token.getTokenIndex());
                }
            } else if (next.getTokenStartIndex() >= 0 && next.getTokenStopIndex() >= 0) {
                continue;
            } else if (next.getChildCount() > 0) {
                ASTNode firstChild = (ASTNode)next.getChild(0);
                ASTNode lastChild = (ASTNode)next.getChild(((ASTNode) next).getChildCount()-1);
                next.setTokenStartIndex(firstChild.getTokenStartIndex());
                next.setTokenStopIndex(lastChild.getTokenStopIndex());
            }
        }
    }
}
