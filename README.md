# SQL Performance Tests

This project was built to test SQL performance of queries:
* that are formatted different ways and use different parameter types
* comparing Java and .NET performance

This is not a nicely packaged solution, it is just something I was using and didn't want to lose the source code to.

## Java

Compile:
1. javac JavaSql.java
2. mkdir custom
3. mv *.class custom/
4. jar vcf custom.jar custom

Execute:
1. java -cp "./sqljdbc41.jar:./custom.jar" custom.JavaSql [settings-file] [server] [database] [username] [password] [scenario] [action]
