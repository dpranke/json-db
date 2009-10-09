PYTHON = python2.5
COVERAGE= coverage
SRCS := *.py
OBJS := $(patsubst %.py,%.pyc,$(SRCS))
TEST_SRCS := $(wildcard *test.py)
TESTS := $(patsubst %.py,%.pyc,$(TEST_SRCS))
COVERS := $(patsubst %.py,.cover.%.py,$(TEST_SRCS))

all : test

test : $(TESTS)

%_test.pyc : %_test.py %.py
	$(PYTHON) $(patsubst %.pyc,%.py,$@)
	@touch $@

%.pyc : %.py
	$(PYTHON) $<

.cover.%.py : %.py
	$(COVERAGE) -p -e -x $<
	@touch .cover.$<

cover : .coverage
	$(COVERAGE) -c
	$(COVERAGE) -r -m $(SRCS)
	$(COVERAGE) -a $(SRCS) 

.coverage : $(COVERS)
	touch .coverage

clean :
	@rm -f $(OBJS)
	@rm -f *.py,cover
	@rm -f $(COVERS)
	@rm -f .coverage*
