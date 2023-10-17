JC = javac
JR = java
JFLAGS  = -g -Wall 
 
default: all

all:
	$(JC) *.java

clean: 
	$(RM) *.class

run:
	$(JR) FileTS

run_test:
	$(JR) TestClient
