#! /usr/bin/env sh

# First download and copy the new jdbc drivers to userlib
mvn dependency:copy-dependencies@userlib-copy

# Then create remove the old RequiredLib files:
rm -f userlib/*DatabaseConnector.RequiredLib

# Recreate those RequiredLib files:
mvn dependency:list \
    | sed -nE 's#[^\s]*    (.*):(.*):jar:(.*):(compile|runtime)#userlib/\1.\2-\3.jar.DatabaseConnector.RequiredLib#p' \
    | xargs touch

# Update the license report file
mvn project-info-reports:dependencies

# Create a neat zip file with that information
cd target/site
zip -r ../../userlib/database-connector-dependencies.zip .
cd ../../

# Clean up
mvn clean
