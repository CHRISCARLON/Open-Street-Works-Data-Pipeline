# Make the deployment of updated docker containers quick and easy!
# If you're not using AWS then replace aws commands with GCP commands for exmaple - or other cloud providers. 

include .env

.PHONY: login build tag push verify all

all: login build tag push verify

login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

build:
	docker build -t $(REPO_NAME) .

tag:
	docker tag $(REPO_NAME):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

push:
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPO_NAME):latest

verify:
	aws ecr describe-images --repository-name $(REPO_NAME) --region $(REGION)
