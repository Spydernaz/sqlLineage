package com.spyder.sqlLineage;

// File Readers
import java.io.File; 
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

// Data Types
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.*;

// Apache Calcite
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParser.ConfigBuilder;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.dialect.ParaccelSqlDialect;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.prepare.Prepare.CatalogReader;

import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.jdbc.JdbcSchema;


import java.sql.DriverManager;
import java.sql.Connection;
import java.util.Properties;
import javax.sql.DataSource;


// Babel for Parser
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;



public class UnwindSql {
    public static String parseQuery(String query) throws Exception {

        final String connectionstring = "PSSQL://somesqlserver.example.com:5432/database1";
        String testSimpleQuery = "SELECT a_id, firstname as FirstName from tbl where FirstName = 'Nate' ORDER BY 1";
        String testComplexQuery = "SELECT s.fname as FirstName, s.lname as LastName, c.CourseName FROM students s JOIN student_courses sc ON s.id = sc.student_id JOIN course c ON c.id = sc.course_id";
        
        final Config config = SqlParser.configBuilder()
            .setLex(Lex.SQL_SERVER)
            .setCaseSensitive(false)
            .setParserFactory(SqlDdlParserImpl.FACTORY)
            .build();

        String q;
        q = query.toString();
        System.out.println("======================");

        final SqlParser parser = SqlParser.create(q, config);
        System.out.println("===> Loaded Config and Created Parser");
        
        final SqlNode sqlNode = parser.parseQuery();
        System.out.println("===> Parsed the Query");

        // Try to create a a RelNode
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final FrameworkConfig plannerConfig = Frameworks.newConfigBuilder()
            .parserConfig(config)
            .defaultSchema(null)
            .traitDefs()
            .programs()
            .build();
        Planner planner = Frameworks.getPlanner(plannerConfig);
        SqlNode parse = planner.parse(query);

        SqlNode validate = planner.validate(parse);
        RelNode rel = planner.rel(validate).project();
        System.out.println(rel.toString());

        // final SqlSelect sqlSelect = (SqlSelect) sqlNode;
        // final SqlJoin from = (SqlJoin) sqlSelect.getFrom();
        // JSONObject jsonObject = JsonBuilderFactory.buildObject();
        JSONObject jsonObject = new JSONObject();
        jsonObject.append("query", query);
        jsonObject.append("results", unwrap(sqlNode));

        return (jsonObject.toString());
        // System.out.println("pause");
    }

    public static List unwrap(SqlNode node) throws Exception {
        // If order by comes in the query.
        // @TODO: check for what type of query
        String temp = node.getKind().name();
        List sources = new ArrayList();

        switch (node.getKind()) {
            case ORDER_BY:
                // Should only return what is already in the query as this doesnt change the sources
                // Calling recursively to try and capture the select query
                // List sources = new ArrayList();
                System.out.println("===> ORDERBY, NO CHANGE TO SOURCES");
                sources.addAll(unwrap((SqlNode) ((SqlOrderBy) node).query));
                return sources;

            case JOIN:
                // Should join a list of sources as a list of objects 
                JSONObject join = new JSONObject();
                // List sources = new ArrayList();
                System.out.println("===>JOIN, APPENDING LIST OF JOINS");

                // join.put("type", "JOIN");
                // join.put("joinType", ((SqlJoin) node).getJoinType().toString());
                // join.put("joinCondition", ((SqlJoin) node).getConditionType().toString());
                // join.put("joinLeft", unwrap(((SqlJoin) node).getLeft()));
                // join.put("joinRight", unwrap(((SqlJoin) node).getRight()));
                
                sources.addAll(unwrap(((SqlJoin) node).getLeft()));
                sources.addAll(unwrap(((SqlJoin) node).getRight()));

                return sources;

            case SELECT:
                // Shouldn't change sources unless there are literals but this is not coming from another table
                System.out.println("===> SELECT STATEMENT - Select List");
                JSONObject select = new JSONObject();
                // List sources = new ArrayList();
                // select.put("type", "SELECT");

                // select list
                // System.out.println("CREATE SELECT LIST");
                // JSONArray rselectList = new JSONArray();
                // SqlNodeList selectList = ((SqlSelect) node).getSelectList();
                // for (SqlNode el : selectList) {
                //     rselectList.put(unwrap(el));
                // }
                // select.put("selectList", rselectList);

                // from statement
                System.out.println("===> FROM - Should update sources");
                SqlNode from = (SqlNode) ((SqlSelect) node).getFrom();
                if (from != null){
                    sources.addAll(unwrap(from));
                }

                return sources;
            case AS:
                System.out.println("===>SOME 'AS' FUNCTION");
                sources.addAll(unwrap((SqlNode) ((SqlBasicCall) node).operand(0)));
                return sources;
                // // Shouldnt update anything other than resolving aliases
                // JSONObject as = new JSONObject();
                // // String actualId = (String) unwrap((SqlNode) ((SqlBasicCall)
                // // node).operand(0));
                // // String alias = (String) unwrap(((SqlBasicCall) node).operand(1));
                // as.put("type", "AS");
                // as.put("actual", unwrap((SqlNode) ((SqlBasicCall) node).operand(0)));
                // as.put("alias", unwrap((SqlNode) ((SqlBasicCall) node).operand(1)));
                // return (as);

            case IDENTIFIER:
                System.out.println("===>SOME 'IDENTIFIER' FUNCTION");
                System.out.println("" + (String) ((SqlIdentifier) node).toString());
                sources.add("" + (String) ((SqlIdentifier) node).toString());
                return sources;
                // JSONObject identifier = new JSONObject();
                // identifier.put("type", "IDENTIFIER");
                // identifier.put("name", (String) ((SqlIdentifier) node).toString());
                // return (identifier);
            case OTHER_FUNCTION:
                System.out.println("===> OTHER FUNCTION");
                return sources;
                // System.out.println("===>SOME 'OTHER FUNCTION?' WHATEVER THAT MEANS");
                // JSONObject other_function = new JSONObject();
                // other_function.put("type", "OTHER_FUNCTION");
                // // other_function.put("name", (String) ((SqlFunction) ((SqlBasicCall)
                // // node)).getName();
                // other_function.put("string", (String) node.toString());
                // return (other_function);

            case AND:
                System.out.println("===> Logic and Math");
                return sources;
            case OR:
                System.out.println("===> Logic and Math");
                // JSONObject logic = new JSONObject();
                // logic.put("type", "LOGICAL_CONDITION");
                // logic.put("condition_type", node.getKind().toString().toLowerCase());
                // logic.put("left", unwrap((SqlNode) ((SqlBasicCall) node).operand(0)));
                // logic.put("right", unwrap((SqlNode) ((SqlBasicCall) node).operand(1)));

                return sources;
            case EQUALS:
                System.out.println("===> Logic and Math");
                return sources;
            case NOT_EQUALS:
                System.out.println("===> Logic and Math");
                return sources;
            case GREATER_THAN:
                System.out.println("===> Logic and Math");
                return sources;
            case GREATER_THAN_OR_EQUAL:
                System.out.println("===> Logic and Math");
                return sources;
            case LESS_THAN:
                System.out.println("===> Logic and Math");
                return sources;
            case LESS_THAN_OR_EQUAL:
                System.out.println("===> Logic and Math");
                return sources;
            case LIKE:
                System.out.println("===> a LIKE for conditional search");
                return sources;
                // JSONObject comparison = new JSONObject();
                // comparison.put("type", "comparison");
                // comparison.put("comparison_type", node.getKind().toString().toLowerCase());
                // comparison.put("left", unwrap((SqlNode) ((SqlBasicCall) node).operand(0)));
                // comparison.put("right", unwrap((SqlNode) ((SqlBasicCall) node).operand(1)));

                // return (comparison);
            case LITERAL:
                System.out.println("===> LITERAL :: " + node.getKind().name());
                List stringish = new ArrayList();
                stringish.add("" + ((SqlLiteral) node).getValue());
                return stringish;
                // System.out.println("===> Literal :: " + node.getKind().name());
                // JSONObject literal = new JSONObject();
                // literal.put("type", "LITERAL");
                // literal.put("value", ((SqlLiteral) node).getValue());
                
                // return (literal);
            case WITH:
                System.out.println("===> WITH STATEMENT (NOT IMPLEMENTED)");
                return sources;
                
            default:
                System.out.println("some other type :: " + node.getKind().name());
                String defaultString = ((SqlNode) node).toString();
                return sources;
        }

        // return objects back to a hashmap
        // return results;

    }	
	public static void main(String[] args) throws Exception {
		// Check for input argument
		if (args.length < 1) {
			System.out.println("Please specify a directory path.");
			System.exit(1); 
		}
		String path = args[0];
		File dir = new File(path);
		
        // Validate that argument is valid and a directory
		if (!dir.exists() || !dir.isDirectory()) {
			System.out.println("The specified directory does not exist.");
			System.exit(1); 
		}
		// Get list of files in directory
		String[] paths = dir.list(); 
		
        // Iterate over each file
		for (int i = 0; i < paths.length; i++) {
            System.out.println(paths[i]);
			File file = new File(path + "/" + paths[i]);
			
            // Print the lastElement... should be a SQL file
            String extension = paths[i].substring(paths[i].lastIndexOf(".") + 1);

            // If it is a file, read the contents
            if (file.isFile() && extension.toUpperCase().equals("SQL")) {
				StringBuilder sb = new StringBuilder();
				try {
					FileReader fr = new FileReader(file); 
					BufferedReader br = new BufferedReader(fr); 
					String line;
					while ((line = br.readLine()) != null) {
						sb.append(line);
						sb.append(" ");
					}
					br.close();
				} catch (IOException e) {
					System.out.println("Error while reading from file.");
					e.printStackTrace();
				}

                // Print the contents as a single string
				System.out.println("SQL sources: ");
				String sqlScript = sb.toString();
                sqlScript = sqlScript.toUpperCase();
                System.out.println(parseQuery(sqlScript));
			}
		}
	}
}
