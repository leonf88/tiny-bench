
LIB_PATH=target/alternateLocation

JAR_PATH=""

for jar in `ls $LIB_PATH/*.jar target/*.jar`; do
    JAR_PATH=$jar:$JAR_PATH
done

java -cp $JAR_PATH org.base.mergest.Main $@
