# Let's Play: DynamoDB

Example libraries and documentation for using DynamoDB

This is free information. You get what you pay for.

## Packages

`pkg/dynamodb` provides conveniences for performing CRUD operations against a DynamoDB table.

`pkg/session` wraps an AWS session

`pkg/uuid` is a copy/paste of `github.com/google/uuid` for generating UUIDs

`pkg/csv` is a simple CSV Parsing package

`pkg/loader` accepts a CSV object and load its contents into a DynamoDB table via BatchWriteItem

`pkg/irrigation` loads an example CSV from `data` and inserts it into a DynamoDB table called `irrigations`, which must first be created using the terraform configs. See See `irrigation_test.go` for LOTS example usage of all of these packages.
