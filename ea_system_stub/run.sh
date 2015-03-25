#PERFORMANCE="-Xmx4G"
#OPTIONS="-Djava.awt.headless=true -Xverify:none"
#DEBUG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=4000,suspend=n"
PERFORMANCE=""
OPTIONS=""
DEBUG=""
java $PERFORMANCE $DEBUG $OPTIONS -cp target/stub_ea_system-*-SNAPSHOT-jar-with-dependencies.jar ea_stub.EAStub