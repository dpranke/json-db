# Copyright 2009 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#     
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
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
