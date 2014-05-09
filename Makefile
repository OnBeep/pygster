# Makefile for pygster.
#
# Author:: Greg Albrecht <gba@onbeep.com>
# Copyright:: Copyright 2014 OnBeep, Inc.
# License:: GNU General Public License, Version 3
# Source:: https://github.com/OnBeep/pygster
#


.DEFAULT_GOAL := develop


develop:
	python setup.py develop

install:
	python setup.py install

uninstall:
	pip uninstall pygster

install_requirements:
	pip install -r requirements.txt --use-mirrors

lint:
	pylint --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" \
		-r n bin/*.py tests/*.py || exit 0

flake8:
	flake8 --max-complexity 12 --exit-zero bin/* logster/*.py logster/parsers/*.py

pep8: flake8

nosetests:
	nosetests

test: nosetests

clean:
	@rm -rf *.egg* build/* dist/* *.pyc *.pyo cover doctest_pypi.cfg \
	nosetests.xml *.egg output.xml *.log */*.pyc .coverage
