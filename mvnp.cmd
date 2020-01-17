call mvn clean
call mvn compile -Dmaven.test.skip=true -P prod
call mvn package -Dmaven.test.skip=true -P prod
copy .\target\*.tar.gz .\dist /y