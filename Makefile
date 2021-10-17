.PHONY: run env install clean 
# .SILENT:

## 
## Makefile for heizung
## -------------------------
## ⁣
## This file contains various targets for the heizung.

ENV?=env
VENV?=venv
PYTHON?=python3

SRC:=gui
ENV_BIN:=$(ENV)/bin
ENV_PYTHON:=$(ENV_BIN)/$(PYTHON)

## ⁣
## Deployment:

run:		## run to the hills 
run: env
	$(ENV_PYTHON) read_sdm72.py

#env: $(ENV_BIN)/activate install
env:
	test -d $(ENV) || $(PYTHON) -m $(VENV) $(ENV)
	$(ENV_PYTHON) -m ensurepip --upgrade
	$(ENV_PYTHON) -m pip install -qq --upgrade pip
	$(ENV_PYTHON) -m pip install -qq -r requirements.txt
	touch ./$(ENV_BIN)/activate

install:	## install project in editable mode
	$(ENV_PYTHON) -m pip install -qq -e .

## ⁣
## Helpers:

clean:		## Remove generated files (env, docs, ...)
	rm -rf $(ENV)
	rm -rf docs/_build/*.*
	rm -rf docs/coverage/*.*


help:		## Show this help
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)
