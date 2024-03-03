from marshmallow import Schema, fields
from schema.accelerator_schema import AccelerometerSchema
from schema.location_schema import GpsSchema

class AggregatedDataSchema(Schema):
    accelerometer = fields.Nested(AccelerometerSchema)
    gps = fields.Nested(GpsSchema)
    time = fields.DateTime('iso')
