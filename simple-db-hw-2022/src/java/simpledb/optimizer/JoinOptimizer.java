package simpledb.optimizer;

import simpledb.ParsingException;
import simpledb.common.Database;
import simpledb.execution.*;
import simpledb.storage.TupleDesc;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.util.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    final LogicalPlan p;
    final List<LogicalJoinNode> joins;

    /**
     * Constructor
     *
     * @param p     the logical plan being optimized
     * @param joins the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     *
     * @param lj    The join being considered
     * @param plan1 The left join node's child
     * @param plan2 The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj,
                                             OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().indexForFieldName(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().indexForFieldName(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        j = new Join(p,plan1,plan2);

        return j;

    }

    /**
     * Estimate the cost of a join.
     * <p>
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     * @param j     A LogicalJoinNode representing the join operation being
     *              performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand
     *              side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand
     *              side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
                                   double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            double ioCost   = cost1 + card1 * cost2;
            double cpuCost  = card1 * card2;
            return ioCost + cpuCost;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     *
     * @param j      A LogicalJoinNode representing the join operation being
     *               performed.
     * @param card1  Cardinality of the left-hand table in the join
     * @param card2  Cardinality of the right-hand table in the join
     * @param t1pkey Is the left-hand table a primary-key table?
     * @param t2pkey Is the right-hand table a primary-key table?
     * @param stats  The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
                                       boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   String table1Alias, String table2Alias, String field1PureName,
                                                   String field2PureName, int card1, int card2, boolean t1pkey,
                                                   boolean t2pkey, Map<String, TableStats> stats,
                                                   Map<String, Integer> tableAliasToId) {
        // DONE
        // For equality joins, when one of the attributes is a primary key,
        // the number of tuples produced by the join cannot be larger than
        // the cardinality of the non-primary key attribute.
        if (joinOp.equals(Predicate.Op.EQUALS)) {
            if (t1pkey && t2pkey) {
                return Math.min(card1, card2);
            } else if (!t1pkey && !t2pkey) {
                return Math.max(card1, card2);
            } else {
                return t1pkey ? card2 : card1;
            }
        } else if (joinOp.equals(Predicate.Op.NOT_EQUALS)) {
            if (t1pkey && t2pkey) {
                return card1 * card2 - Math.min(card1, card2);
            } else if (!t1pkey && !t2pkey) {
                return card1 * card2 - Math.max(card1, card2);
            } else {
                return card1 * card2 - (t1pkey ? card2 : card1);
            }
        } else {
            // For range scans, it is similarly hard to say anything accurate about sizes.
            // The size of the output should be proportional to the sizes of the inputs.
            // It is fine to assume that a fixed fraction of the cross-product is emitted by range scans (say, 30%).
            return (int) (0.3 * card1 * card2);
        }
    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     *
     * @param v    The vector whose subsets are desired
     * @param size The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> els = new HashSet<>();
        els.add(new HashSet<>());
        // Iterator<Set> it;
        // long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Set<Set<T>> newels = new HashSet<>();
            for (Set<T> s : els) {
                for (T t : v) {
                    Set<T> news = new HashSet<>(s);
                    if (news.add(t))
                        newels.add(news);
                }
            }
            els = newels;
        }

        return els;

    }

    // 此函数的函数语义其实是求解组合问题：返回范围 List<T> v 中所有可能的 size 个元素的组合。
    // 而提供的解法更类似于枚举法，有性能浪费，每次都从 C(n,1) -> C(n,2) -> C(n,k)，
    // 无论k为多少，都是从头开始迭代，时间复杂度O(2^n)。
    // 枚举法更适合解决子集问题，每个元素都有选与不选两种选择。
    // 因此，由此可以延伸出一种可选的方法，其实可以一次性把 2^n 种子集都枚举出来，
    // 放进一个Map中，k —> List of C(n,k)。
    // 而组合问题更适合使用 DFS 回溯，时间复杂度O(C(n,k))，性能更优。
    // 2^n = C(n,1) + C(n,2) + ... + C(n,k) +... + C(n,n)
    public <T> Set<Set<T>> myEnumerateSubsets(List<T> v, int size) {
        final Set<Set<T>> result = new HashSet<>();
        final Set<T> path = new HashSet<>();
        subsetDfs(result, path, v, 0, size);
        return result;
    }

    private <T> void subsetDfs(final Set<Set<T>> result, final Set<T> path, final List<T> v, int curIndex, final int size) {
        if (path.size() == size) {
            result.add(new HashSet<>(path));
            return;
        }
        for (int i = curIndex; i <= (v.size() - (size - path.size())); i++) {
            path.add(v.get(i));
            subsetDfs(result, path, v, i + 1, size);
            path.remove(v.get(i));
        }
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * the Lab 3 description for hints on how this should be implemented.
     *
     * @param stats               Statistics for each table involved in the join, referenced by
     *                            base table names, not alias
     * @param filterSelectivities Selectivities of the filter predicates on each table in the
     *                            join, referenced by table alias (if no alias, the base table
     *                            name)
     * @param explain             Indicates whether your code should explain its query plan or
     *                            simply execute it
     * @return A List<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     * @throws ParsingException when stats or filter selectivities is missing a table in the
     *                          join, or when another internal error occurs
     */
    public List<LogicalJoinNode> orderJoins(
            Map<String, TableStats> stats,
            Map<String, Double> filterSelectivities, boolean explain)
            throws ParsingException {
        // Not necessary for labs 1 and 2.

        // DONE
        // Seilinger algorithm, like dp.
        PlanCache   pc              = new PlanCache();
        CostCard    bestCostCard    = null;
        // 时间复杂度O(2^N)
        // 因为实际上此算法在迭代执行 C(n,1), C(n,2),..., C(n,k),..., C(n,n).
        for (int i = 1; i <= joins.size(); i++) {
            // all length i subsets of joins.size()
            Set<Set<LogicalJoinNode>> subsets = myEnumerateSubsets(joins, i);
            for (Set<LogicalJoinNode> subplan : subsets) { //得到固定长度i的子集
                //遍历集合中的集合，得到集合中的每个集合的最小
                bestCostCard        = new CostCard();
                bestCostCard.cost   = Double.MAX_VALUE;   //长度i的最低cost
                for (LogicalJoinNode removedNode : subplan) {
                    //计算 removedNode 与其他 node 的 join 结果
                    CostCard costCard1 = computeCostAndCardOfSubplan(stats, filterSelectivities, removedNode, subplan, bestCostCard.cost, pc);
                    if (costCard1 == null) {
                        continue;
                    }
                    bestCostCard = costCard1.cost < bestCostCard.cost ? costCard1 : bestCostCard;
                }
                pc.addPlan(subplan, bestCostCard.cost, bestCostCard.card, bestCostCard.plan);
            }
        }
        if (explain) {
            assert bestCostCard != null;
            printJoins(bestCostCard.plan, pc, stats, filterSelectivities);
        }
        assert bestCostCard != null;
        return bestCostCard.plan;
    }

    // ===================== Private Methods =================================

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats               table stats for all of the tables, referenced by table names
     *                            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities the selectivities of the filters over each of the tables
     *                            (where tables are identified by their alias or name if no
     *                            alias is given)
     * @param joinToRemove        the join to remove from joinSet
     * @param joinSet             the set of joins being considered
     * @param bestCostSoFar       the best way to join joinSet so far (minimum of previous
     *                            invocations of computeCostAndCardOfSubplan for this joinSet,
     *                            from returned CostCard)
     * @param pc                  the PlanCache for this join; should have subplans for all
     *                            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects describing the cost, cardinality,
     *         optimal subplan
     * @throws ParsingException when stats, filterSelectivities, or pc object is missing
     *                          tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(
            Map<String, TableStats> stats,
            Map<String, Double> filterSelectivities,
            LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
            double bestCostSoFar, PlanCache pc) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParsingException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParsingException("Unknown table " + j.t2Alias);

        String table1Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        if (news.isEmpty()) { // base case -- both are base relations
            prevBest = new ArrayList<>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(
                    filterSelectivities.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(
                            filterSelectivities.get(j.t2Alias));
            rightPkey = table2Alias != null && isPkey(table2Alias,
                    j.f2PureName);
        } else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(
                                filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias,
                        j.f2PureName);
            } else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                // (both
                // shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(
                        filterSelectivities.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
                rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j); // prevbest is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias The alias of the table in the query
     * @param field      The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js            the join plan to visualize
     * @param pc            the PlanCache accumulated whild building the optimal plan
     * @param stats         table statistics for base tables
     * @param selectivities the selectivities of the filters over each of the tables
     *                      (where tables are indentified by their alias or name if no
     *                      alias is given)
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc,
                            Map<String, TableStats> stats,
                            Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.getCost(pathSoFar) + ", card = "
                    + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                        selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                + " (Cost = "
                                + stats.get(table2Name)
                                .estimateScanCost()
                                + ", card = "
                                + stats.get(table2Name)
                                .estimateTableCardinality(
                                        selectivities
                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
