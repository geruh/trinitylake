# View

A view is a [tabular object](./overview.md#traits) that represents a transformation applied to a set of tabular objects.

## Object Definition Schema

***Schema ID: 3***

| Field Name            | Protobuf Type       | Description                                                                                          | Required? | Default |
|-----------------------|---------------------|------------------------------------------------------------------------------------------------------|-----------|---------|
| name                  | string              | A user-friendly name of this view                                                                    | Yes       |         |
| schema_binding        | boolean             | If `true`, the view uses the schema defined at creation time; otherwise, it is evaluated dynamically | Yes       | false   |
| schema                | Schema              | Schema of the view, similar to [Table Schema](table-schema.md). Required if `schema_binding = true`  | No        |         |
| substrait_read_rel    | bytes               | Serialized Substrait read relation                                                                   | Yes       |         |
| referenced_object_ids | repeated Identifier | List of tabular object identifiers referenced by this view                                           | No        |         |
| properties            | map<string, string> | Free form user-defined key-value string properties                                                   | No        |         |

## Name Size

All view names must obey the maximum size configuration defined in the [Lakehouse definition file](../lakehouse.md).