.DEFAULT_GOAL := cqc

cqc: clean
	go build -o cqc *.go

.PHONY: clean
clean:
	rm -f cqc

