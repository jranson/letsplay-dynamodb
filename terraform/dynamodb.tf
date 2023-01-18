resource "aws_dynamodb_table" "irrigations" {
  name         = "irrigations"
  billing_mode = "PAY_PER_REQUEST" # On Demand Provisioning
  hash_key     = "id"              # AKA Partition Key


  # Example Configuration for Provisioned Access
  # billing_mode     = "PROVISIONED"
  # read_capacity    = 2
  # write_capacity   = 1

  # all hash_keys and range_keys must be defined as attributes
  # for the table and any secondary indexes
  # do not define attributes for non-indexed fields or the apply will fail
  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "location"
    type = "S"
  }

  attribute {
    name = "report_time"
    type = "S"
  }

  global_secondary_index {
    // it's a good practice to name the index like "${HashKey}${RangeKey}Index"
    // for predictable / programmatic access
    name            = "LocationReportTimeIndex"
    hash_key        = "location"
    range_key       = "report_time"
    projection_type = "ALL"

    # if provisioned access on the table, you MUST separately sepcify
    # capacity for each secondary index
    # read_capacity    = 2
    # write_capacity   = 1
  }

  tags = {
    Component = "irrigation"
  }
}
