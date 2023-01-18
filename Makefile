fmt:
	cd terraform && terraform fmt

plan:
	cd terraform && terraform plan

deploy:
	cd terraform && terraform apply

destroy:
	cd terraform && terraform destroy
