# SQL Lineage #

Using Apache Calcite to parse SQL queries and work out the lineage of an environment

# How to run #
Get the file path of the `scripts` folder
Ensure Maven is installed

Run

```sh
mvn clean package exec:java -Dexec.args="<PATH_TO_SCRIPTS>"
```

I have marked the failing scripts as `*.old`, to see the effects, change to `.sql` but it will crash with errors.



