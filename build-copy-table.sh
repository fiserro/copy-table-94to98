cd dependency-lib
./pack.sh
cd ..
mvn install
cd dependency-fix
mvn install
cd ../copy-table
mvn package -Ppack
mv target/*with-dependencies.jar ../copy-table.jar
cd ..
