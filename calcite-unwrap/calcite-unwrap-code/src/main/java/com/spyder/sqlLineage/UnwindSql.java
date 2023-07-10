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
import java.util.Arrays;
import org.json.*;

// Apache Calcite
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParser.ConfigBuilder;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.fun.*;
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
import org.apache.calcite.sql.parser.impl.SqlParserImpl;



public class UnwindSql {
    // Create a DDL Parser
    final static Config ddlconfig = SqlParser.configBuilder()
        .setLex(Lex.SQL_SERVER)
        .setCaseSensitive(false)
        .setParserFactory(SqlDdlParserImpl.FACTORY)
        .build();

    // Create a Babel Parser
    final static Config defaultconfig = SqlParser.configBuilder()
        .setLex(Lex.SQL_SERVER)
        .setCaseSensitive(false)
        .setParserFactory(SqlParserImpl.FACTORY)
        .build();

    public static String parseQuery(String query) throws Exception {
        String q;
        q = query.toString();
        System.out.println("======================");

        // check if DDL statement. If so, use ddlconfig
        final SqlParser parser = SqlParser.create(q, ddlconfig);
        System.out.println("===> Loaded Config and Created DDL Parser");

        final SqlNode sqlNode = parser.parseQuery();
        System.out.println("===> Parsed the Query");

        JSONObject jsonObject = new JSONObject();
        jsonObject.append("query", query);
        jsonObject.append("results", unwrap(sqlNode));

        return (jsonObject.toString());
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
                
                sources.addAll(unwrap(((SqlJoin) node).getLeft()));
                sources.addAll(unwrap(((SqlJoin) node).getRight()));

                return sources;

            case SELECT:
                SqlNode from = (SqlNode) ((SqlSelect) node).getFrom();
                if (from != null){
                    sources.addAll(unwrap(from));
                }
                return sources;

            case AS:
                sources.addAll(unwrap((SqlNode) ((SqlBasicCall) node).operand(0)));
                return sources;

            case IDENTIFIER:
                sources.add("" + (String) ((SqlIdentifier) node).toString());
                return sources;

            case OTHER_FUNCTION:
                System.out.println("===> OTHER FUNCTION");
                return sources;

            case AND:
                System.out.println("===> AND STATEMENT");
                return sources;

            case OR:
                System.out.println("===> OR STATEMENT");
                return sources;

            case EQUALS:
                System.out.println("===> EQUALS");
                return sources;

            case NOT_EQUALS:
                System.out.println("===> NOT EQUALS");
                return sources;

            case GREATER_THAN:
                System.out.println("===> GREATER THAN");
                return sources;

            case GREATER_THAN_OR_EQUAL:
                System.out.println("===> GREATER THAN OR EQUAL");
                return sources;

            case LESS_THAN:
                System.out.println("===> LESS THAN");
                return sources;

            case LESS_THAN_OR_EQUAL:
                System.out.println("===> LESS THAN OR EQUAL");
                return sources;

            case LIKE:
                System.out.println("===> LIKE");
                return sources;

            case LITERAL:
                System.out.println("===> LITERAL :: " + node.getKind().name());
                List stringish = new ArrayList();
                stringish.add("" + ((SqlLiteral) node).getValue());
                return stringish;

            case WITH:
                // Parse each WITH table
                for (int i = 0; i < ( (SqlWith) node ).withList.size(); i++)
                {
                    SqlWithItem withTable = ( (SqlWithItem) ( (SqlWith) node ).withList.get(i));
                    SqlSelect withTbl = (SqlSelect) withTable.query;
                    sources.addAll(unwrap(withTbl));
                }
                // Parse remainder of the query
                sources.addAll(unwrap(((SqlWith)(node)).body));
                return sources;

            case CREATE_VIEW:
                String q = ( (org.apache.calcite.sql.ddl.SqlCreateView) node ).query.toString();
                sources.addAll(unwrap(( (org.apache.calcite.sql.ddl.SqlCreateView) node ).query));
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
