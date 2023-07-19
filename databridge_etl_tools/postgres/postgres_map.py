DATA_TYPE_MAP = {
    'string':                      'text',
    'number':                      'numeric',
    'float':                       'numeric',
    'double precision':            'numeric',
    'integer':                     'integer',
    'boolean':                     'boolean',
    'object':                      'jsonb',
    'array':                       'jsonb',
    'date':                        'date',
    'time':                        'time',
    'datetime':                    'timestamp without time zone',
    'timestamp without time zone': 'timestamp without time zone',
    'timestamp with time zone':    'timestamp with time zone',
    'geom':                        'geometry',
    'geometry':                    'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'linestring':      'Linestring',
    'polygon':         'MultiPolygon',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
    'geometry':        'Geometry',
}
