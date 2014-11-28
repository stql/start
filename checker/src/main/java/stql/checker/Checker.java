/**
 * Introduction: This is an independent tool to check the basic syntax of stql language.
 * Usage: java -jar Checker.jar "query1;query2;...;"
 * query1, query2 are stql queries.
 */
package stql.checker;
public class Checker {
    public static void main(String[] args) {
        ParseDriver pd = new ParseDriver();
        String[] stmts = args[0].split(";");
        for (String stmt : stmts)
            try {
                ASTNode tree = pd.parse(stmt.trim());
/*
                DOTTreeGenerator gen = new DOTTreeGenerator();
                StringTemplate st = gen.toDOT(tree.getChild(0));
                System.out.println(st);
*/
            } catch (ParseException e) {
                System.out.println(e.getMessage());
            }
    }
}