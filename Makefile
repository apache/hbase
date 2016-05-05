build:
	$(shell buck build core/... )

check:
	$(shell buck test --all --no-results-cache )

doc:
	$(shell doxygen hbase.doxygen > /dev/null )

clean:
	$(shell rm -rf docs )

help:
	@echo "Available targets:"
	@echo ""
	@echo "	build : will build everything."
	@echo "	clean : will remove the docs folder"
	@echo "	check : will test everything."

all: build doc check
