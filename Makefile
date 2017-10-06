upload:
	python setup.py register sdist upload

test:
	python tests/agent_spec.py 
