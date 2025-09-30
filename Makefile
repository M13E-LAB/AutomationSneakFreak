# Makefile for Snowflake Inventory Pipeline repo (using uv + ruff)
PYTHON ?= python3
UV ?= uv

.PHONY: help install jupyter lint fmt test clean

help:
	@echo "Common commands:"
	@echo " make install - Install dependencies with uv"
	@echo " make jupyter - Launch Jupyter Notebook"
	@echo " make lint - Run ruff linter"
	@echo " make fmt - Auto-format with ruff"
	@echo " make test - Run pytest"
	@echo " make clean - Remove caches and artifacts"

install: 
	$(UV) sync

jupyter: 
	$(UV) run jupyter notebook

lint: 
	$(UV) run ruff check .

fmt: 
	$(UV) run ruff format .

test: 
	$(UV) run pytest -q

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache .ipynb_checkpoints
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	find . -type d -name 'dist' -exec rm -rf {} +
