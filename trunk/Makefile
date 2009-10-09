SRCS := *.py
OBJS := $(patsubst %.py,%.pyc,$(SRCS))
TEST_SRCS := $(wildcard *test.py)
TESTS := $(patsubst %.py,%.pyc,$(TEST_SRCS))
COVERS := $(patsubst %.py,.cover.%.py,$(TEST_SRCS))

all : test

test : $(TESTS)

%_test.pyc : %_test.py %.py
	python $(patsubst %.pyc,%.py,$@)
	@touch $@

%.pyc : %.py
	python $<

.cover.%.py : %.py
	coverage -p -e -x $<
	@touch .cover.$<

cover : .coverage
	coverage -c
	coverage -r -m $(SRCS)
	coverage -a $(SRCS) 

.coverage : $(COVERS)
	touch .coverage

clean :
	@rm -f $(OBJS)
	@rm -f *.py,cover
	@rm -f $(COVERS)
	@rm -f .coverage*
